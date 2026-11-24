package payments

import (
	"time"

	"github.com/patricioarmesto/0007-stripe/internal/state"
)

type Payment struct {
	ID             string       `json:"id"`
	IdempotencyKey string       `json:"-"`
	Amount         int          `json:"amount"`
	Currency       string       `json:"currency"`
	Email          string       `json:"email"`
	Status         state.Status `json:"status"`
	CreatedAt      time.Time    `json:"created_at"`
	Attempts       int          `json:"attempts"`
}

type Transition struct {
	FromStatus string    `json:"from_status"`
	ToStatus   string    `json:"to_status"`
	CreatedAt  time.Time `json:"created_at"`
}
