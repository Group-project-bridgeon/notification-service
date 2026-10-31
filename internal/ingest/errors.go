package ingest

import (
	"errors"
	"fmt"
)

// PermanentError wraps a failure that must NOT be retried.
// Triggers immediate dead-letter routing.
// Examples: non-JSON bytes, envelope schema violation, oversized payload.
type PermanentError struct {
	Cause error
}

func (e *PermanentError) Error() string {
	return fmt.Sprintf("permanent: %v", e.Cause)
}

func (e *PermanentError) Unwrap() error { return e.Cause }

// TransientError wraps a failure that may succeed on a later retry.
// Examples: database temporarily unreachable, lock acquisition timeout.
type TransientError struct {
	Cause error
}

func (e *TransientError) Error() string {
	return fmt.Sprintf("transient: %v", e.Cause)
}

func (e *TransientError) Unwrap() error { return e.Cause }

// IsPermanent reports whether err is or wraps a PermanentError.
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}

// IsTransient reports whether err is or wraps a TransientError.
func IsTransient(err error) bool {
	var t *TransientError
	return errors.As(err, &t)
}
