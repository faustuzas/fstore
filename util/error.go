package util

import (
	"context"
	"errors"
)

func IsCancelled(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, context.Canceled)
}

func DrainErrChannel(errCh <-chan error) error {
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}
