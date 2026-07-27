package broker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/GhostFlying/delegation/internal/store"
)

const resultPackageGCOperationTimeout = 30 * time.Second

type resultPackageDetailCompactor interface {
	CompactReleasedResultPackageDetails(
		context.Context,
		string,
		int,
	) (store.ResultPackageDetailCompaction, error)
}

func (s *Server) wakeResultPackageGC() {
	if s.resultPackageGCWake == nil {
		return
	}
	select {
	case s.resultPackageGCWake <- struct{}{}:
	default:
	}
}

func (s *Server) resultPackageGCLoop() {
	defer s.background.Done()
	ticker := time.NewTicker(s.resultPackageGCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.context.Done():
			return
		case <-s.resultPackageGCWake:
		case <-ticker.C:
		}
		s.compactReleasedResultPackageDetails()
	}
}

func (s *Server) compactReleasedResultPackageDetails() {
	for {
		ctx, cancel := context.WithTimeout(s.context, resultPackageGCOperationTimeout)
		result, err := s.resultPackageGCRegistry.CompactReleasedResultPackageDetails(
			ctx,
			s.controllerID,
			store.MaximumResultPackageDetailCompactionBatch,
		)
		cancel()
		if err != nil {
			if !errors.Is(err, context.Canceled) || s.context.Err() == nil {
				s.reportError(fmt.Errorf("compact released result package details: %w", err))
			}
			return
		}
		if !result.More {
			return
		}
		if result.Compacted == 0 {
			s.reportError(errors.New("result package detail compaction reported more work without progress"))
			return
		}
	}
}
