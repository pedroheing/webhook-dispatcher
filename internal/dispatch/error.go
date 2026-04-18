package dispatch

import "errors"

type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

func NewPermanentError(err error) error {
	return &PermanentError{Err: err}
}

func isPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}
