package payments

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/patricioarmesto/0007-stripe/internal/state"
)

type fakeRepo struct {
	byKey   map[string]*Payment
	byID    map[string]*Payment
	inserts int
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{byKey: map[string]*Payment{}, byID: map[string]*Payment{}}
}

func (f *fakeRepo) GetByIdempotencyKey(_ context.Context, key string) (*Payment, error) {
	return f.byKey[key], nil
}
func (f *fakeRepo) GetByID(_ context.Context, id string) (*Payment, error) {
	return f.byID[id], nil
}
func (f *fakeRepo) List(_ context.Context) ([]Payment, error) { return nil, nil }
func (f *fakeRepo) Insert(_ context.Context, p *Payment) error {
	f.inserts++
	c := *p
	f.byKey[p.IdempotencyKey] = &c
	f.byID[p.ID] = &c
	return nil
}
func (f *fakeRepo) SetStatus(_ context.Context, id string, _, to state.Status) error {
	f.byID[id].Status = to
	return nil
}
func (f *fakeRepo) Transitions(_ context.Context, _ string) ([]Transition, error) {
	return []Transition{}, nil
}

type fakeLocker struct{ busy bool }

func (f *fakeLocker) Acquire(context.Context, string) (string, bool, error) {
	return "token", !f.busy, nil
}
func (f *fakeLocker) Release(context.Context, string, string) error { return nil }

type fakePublisher struct {
	calls int
	err   error
}

func (f *fakePublisher) Publish(context.Context, string, []byte) error {
	f.calls++
	return f.err
}

func input() CreateInput {
	return CreateInput{
		IdempotencyKey: uuid.NewString(), Amount: 12500, Currency: "ARS",
		Email: "ana@example.com", PaymentMethod: "card_test",
	}
}

func TestServicioCreateEsIdempotente(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{}
	s := NewService(repo, &fakeLocker{}, pub)
	in := input()

	primero, created, err := s.Create(context.Background(), in)
	if err != nil || !created {
		t.Fatalf("primer create falló: %+v %v %v", primero, created, err)
	}
	segundo, created, err := s.Create(context.Background(), in)
	if err != nil || created {
		t.Fatalf("segundo create debía devolver existente: %+v %v %v", segundo, created, err)
	}
	if primero.ID != segundo.ID || repo.inserts != 1 || pub.calls != 1 {
		t.Fatalf("se duplicó el pago: ids %s/%s inserts=%d pubs=%d", primero.ID, segundo.ID, repo.inserts, pub.calls)
	}
	if segundo.Status != state.StatusProcessing {
		t.Fatalf("debió quedar processing: %s", segundo.Status)
	}
}

func TestServicioLockOcupadoDevuelvePagoExistente(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{}
	s := NewService(repo, &fakeLocker{busy: true}, pub)
	in := input()
	existente := &Payment{ID: "pay_1", IdempotencyKey: in.IdempotencyKey, Status: state.StatusProcessing}
	_ = repo.Insert(context.Background(), existente)

	got, created, err := s.Create(context.Background(), in)
	if err != nil || created || got.ID != "pay_1" {
		t.Fatalf("lock ocupado debía devolver el existente: %+v %v %v", got, created, err)
	}
}

func TestServicioCreatePublicacionFallidaNoRompePago(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{err: errors.New("kafka caido")}
	s := NewService(repo, &fakeLocker{}, pub)
	p, created, err := s.Create(context.Background(), input())
	if err != nil || !created || p == nil {
		t.Fatalf("el pago no debe depender de Kafka: %+v %v %v", p, created, err)
	}
}

func TestServicioValidaEntrada(t *testing.T) {
	s := NewService(newFakeRepo(), &fakeLocker{}, &fakePublisher{})
	in := input()
	in.Currency = "USD"
	if _, _, err := s.Create(context.Background(), in); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("esperaba ErrInvalidInput, got %v", err)
	}
}
