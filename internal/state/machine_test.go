package state

import "testing"

func TestCanTransition(t *testing.T) {
	casos := []struct {
		from, to Status
		want     bool
	}{
		{StatusCreated, StatusProcessing, true},
		{StatusProcessing, StatusSucceeded, true},
		{StatusProcessing, StatusFailed, true},
		{StatusCreated, StatusSucceeded, false},
		{StatusSucceeded, StatusProcessing, false},
		{StatusSucceeded, StatusFailed, false},
		{StatusFailed, StatusSucceeded, false},
		{StatusSucceeded, StatusSucceeded, false},
	}
	for _, c := range casos {
		if got := CanTransition(c.from, c.to); got != c.want {
			t.Errorf("CanTransition(%s,%s) = %v, quería %v", c.from, c.to, got, c.want)
		}
	}
}

func TestTransitionRechazaEstadoInvalido(t *testing.T) {
	if err := Transition(StatusSucceeded, StatusProcessing); err == nil {
		t.Fatal("un pago cobrado no puede volver a processing")
	}
}
