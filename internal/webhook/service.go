package webhook

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/patricioarmesto/0007-stripe/internal/events"
)

type Service struct {
	repo Repository
	pub  events.Publisher
	log  *log.Logger
}

func NewService(repo Repository, pub events.Publisher) *Service {
	return &Service{repo: repo, pub: pub, log: log.Default()}
}

type publishedEvent struct {
	EventID   string    `json:"event_id"`
	Type      string    `json:"type"`
	PaymentID string    `json:"payment_id"`
	Amount    int       `json:"amount"`
	Email     string    `json:"email"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

func (s *Service) Handle(ctx context.Context, ev Event) (bool, error) {
	processed, err := s.repo.Process(ctx, ev)
	if err != nil || !processed {
		return processed, err
	}
	p, err := s.repo.GetPayment(ctx, ev.PaymentID)
	if err != nil {
		return true, err
	}
	payload, _ := json.Marshal(publishedEvent{
		EventID: ev.EventID, Type: ev.Type, PaymentID: ev.PaymentID,
		Amount: p.Amount, Email: p.Email, Status: string(p.Status), Timestamp: time.Now().UTC(),
	})
	// Postgres ya confirmó el cambio. Si Kafka falla, se loguea y listo:
	// el dashboard se atrasará, pero el cobro sigue siendo verdad.
	if err := s.pub.Publish(ctx, ev.PaymentID, payload); err != nil {
		s.log.Printf("evento %s no publicado: %v", ev.EventID, err)
	}
	return true, nil
}

func (s *Service) Exists(ctx context.Context, eventID string) (bool, error) {
	return s.repo.Exists(ctx, eventID)
}

func (s *Service) MarkAttempts(ctx context.Context, eventID string, attempts int) error {
	return s.repo.MarkAttempts(ctx, eventID, attempts)
}
