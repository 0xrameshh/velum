package persistence

const (
	QueueDefault  = "default"
	QueueEmail    = "email"
	QueuePayments = "payments"
)

func NormalizeQueue(q string) string {
	switch q {
	case QueueEmail, QueueDefault, QueuePayments:
		return q
	case "":
		return QueueDefault
	default:
		return q
	}
}
