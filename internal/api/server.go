package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/0xrameshh/velum/internal/history"
)

type Server struct {
	history history.Client
}

func NewServer(hist history.Client) *Server {
	return &Server{history: hist}
}

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))

	r.Get("/health", s.health)
	r.Get("/ready", s.ready)

	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/namespaces/{namespace}/workflows/{workflow}/start", s.startWorkflow)
		r.Get("/namespaces/{namespace}/runs/{runID}", s.getRun)
	})

	return r
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) ready(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type startWorkflowRequest struct {
	Input map[string]any `json:"input"`
}

func (s *Server) startWorkflow(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	workflowName := chi.URLParam(r, "workflow")

	var req startWorkflowRequest
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if req.Input == nil {
		req.Input = map[string]any{}
	}

	runID, err := s.history.StartWorkflow(r.Context(), namespace, workflowName, req.Input)
	if err != nil {
		slog.Error("start workflow", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"run_id":        runID.String(),
		"namespace":     namespace,
		"workflow_name": workflowName,
		"status":        "running",
	})
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	runIDStr := chi.URLParam(r, "runID")
	runID, err := uuid.Parse(runIDStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid run_id")
		return
	}

	run, events, err := s.history.GetRun(r.Context(), namespace, runID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || grpcNotFound(err) {
			writeError(w, http.StatusNotFound, "run not found")
			return
		}
		slog.Error("get run", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	type eventDTO struct {
		ID        int64           `json:"id"`
		EventType string          `json:"event_type"`
		Payload   json.RawMessage `json:"payload"`
		CreatedAt time.Time       `json:"created_at"`
	}
	evDTOs := make([]eventDTO, 0, len(events))
	for _, e := range events {
		evDTOs = append(evDTOs, eventDTO{
			ID:        e.ID,
			EventType: e.EventType,
			Payload:   e.Payload,
			CreatedAt: e.CreatedAt,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"run": map[string]any{
			"id":            run.ID.String(),
			"namespace":     run.Namespace,
			"workflow_name": run.WorkflowName,
			"status":        run.Status,
			"input":         jsonRawOrEmpty(run.Input),
			"created_at":    run.CreatedAt,
			"updated_at":    run.UpdatedAt,
			"completed_at":  run.CompletedAt,
		},
		"events": evDTOs,
	})
}

func jsonRawOrEmpty(b []byte) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage(`{}`)
	}
	return json.RawMessage(b)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func grpcNotFound(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.NotFound
}
