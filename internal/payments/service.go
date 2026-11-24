package payments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/patricioarmesto/0007-stripe/internal/cache"
	"github.com/patricioarmesto/0007-stripe/internal/events"
	"github.com/patricioarmesto/0007-stripe/internal/state"
)

var (
	ErrInvalidInput = errors.New("datos inválidos")
	ErrBusy         = errors.New("idempotency key en uso")
)

type CreateInput struct {
	IdempotencyKey string `json:"-"`
	Amount         int    `json:"amount"`
	Currency       string `json:"currency"`
	Email          string `json:"email"`
	PaymentMethod  string `json:"payment_method"`
}

type Service struct {
	repo   Repository
	locker cache.Locker
	pub    events.Publisher
	log    *log.Logger
}

func NewService(repo Repository, locker cache.Locker, pub events.Publisher) *Service {
	return &Service{repo: repo, locker: locker, pub: pub, log: log.Default()}
}

type createdEvent struct {
	EventID   string       `json:"event_id"`
	Type      string       `json:"type"`
	PaymentID string       `json:"payment_id"`
	Amount    int          `json:"amount"`
	Email     string       `json:"email"`
	Status    state.Status `json:"status"`
	Timestamp time.Time    `json:"timestamp"`
}

func (s *Service) Create(ctx context.Context, in CreateInput) (*Payment, bool, error) {
	if _, err := uuid.Parse(in.IdempotencyKey); err != nil {
		return nil, false, fmt.Errorf("%w: Idempotency-Key debe ser uuid", ErrInvalidInput)
	}
	if in.Amount <= 0 || in.Currency != "ARS" || !strings.Contains(in.Email, "@") || in.PaymentMethod != "card_test" {
		return nil, false, fmt.Errorf("%w: revisá amount, currency, email y payment_method", ErrInvalidInput)
	}

	lockKey := "idem:" + in.IdempotencyKey
	token, ok, err := s.locker.Acquire(ctx, lockKey)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		// El otro request está escribiendo. Esperamos un instante y releemos:
		// si ya guardó su pago, le devolvemos ese, no un error 500.
		for i := 0; i < 10; i++ {
			time.Sleep(20 * time.Millisecond)
			if p, err := s.repo.GetByIdempotencyKey(ctx, in.IdempotencyKey); err == nil && p != nil {
				return p, false, nil
			}
		}
		return nil, false, ErrBusy
	}
	defer func() {
		if err := s.locker.Release(ctx, lockKey, token); err != nil {
			s.log.Printf("no se pudo liberar lock %s: %v", lockKey, err)
		}
	}()

	if p, err := s.repo.GetByIdempotencyKey(ctx, in.IdempotencyKey); err == nil && p != nil {
		return p, false, nil
	}

	p := &Payment{
		ID: uuid.NewString(), IdempotencyKey: in.IdempotencyKey, Amount: in.Amount,
		Currency: in.Currency, Email: in.Email, Status: state.StatusCreated,
	}
	if err := s.repo.Insert(ctx, p); err != nil {
		return nil, false, err
	}
	// created -> processing pasa antes de responder: el webhook del banco
	// siempre encuentra un estado desde el cual avanzar a succeeded.
	if err := s.repo.SetStatus(ctx, p.ID, state.StatusCreated, state.StatusProcessing); err != nil {
		return nil, false, err
	}

	payload, _ := json.Marshal(createdEvent{
		EventID: "evt_" + uuid.NewString(), Type: "payment.created", PaymentID: p.ID,
		Amount: p.Amount, Email: p.Email, Status: p.Status, Timestamp: time.Now().UTC(),
	})
	if err := s.pub.Publish(ctx, p.ID, payload); err != nil {
		// Acá NO hacemos rollback: Postgres ya dijo que el pago existe.
		s.log.Printf("pago %s guardado pero no se pudo publicar: %v", p.ID, err)
	}
	return p, true, nil
}

func (s *Service) Get(ctx context.Context, id string) (*Payment, []Transition, error) {
	p, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	ts, err := s.repo.Transitions(ctx, id)
	return p, ts, err
}

func (s *Service) List(ctx context.Context) ([]Payment, error) { return s.repo.List(ctx) }
