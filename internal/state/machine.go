// Package state es el árbitro de los estados del pago. No toca la DB a propósito:
// así se puede testear sin levantar nada y tanto payments como webhook usan
// EXACTAMENTE la misma regla.
package state

import "fmt"

type Status string

const (
	StatusCreated    Status = "created"
	StatusProcessing Status = "processing"
	StatusSucceeded  Status = "succeeded"
	StatusFailed     Status = "failed"
)

func CanTransition(from, to Status) bool {
	switch from {
	case StatusCreated:
		return to == StatusProcessing
	case StatusProcessing:
		return to == StatusSucceeded || to == StatusFailed
	default:
		// succeeded y failed son terminales. Un webhook viejo que llega tarde
		// no puede deshacer un cobro ya confirmado.
		return false
	}
}

func Transition(from, to Status) error {
	if !CanTransition(from, to) {
		return fmt.Errorf("transición inválida: %s -> %s", from, to)
	}
	return nil
}
