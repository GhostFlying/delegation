package userservice

import (
	"errors"
	"fmt"
)

type State string

const (
	StateAbsent          State = "absent"
	StatePrepared        State = "prepared"
	StateActive          State = "active"
	StateForeignConflict State = "foreignConflict"
	StateIndeterminate   State = "indeterminate"
)

type CommittedError struct {
	Err error
}

func (e *CommittedError) Error() string {
	return fmt.Sprintf("service state was committed but final durability work failed: %v", e.Err)
}

func (e *CommittedError) Unwrap() error {
	return e.Err
}

func IsCommitted(err error) bool {
	var committed *CommittedError
	return errors.As(err, &committed)
}
