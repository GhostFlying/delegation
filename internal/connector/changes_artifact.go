package connector

import (
	"context"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/protocol"
)

const maximumPendingChangesPublications = 64

var errChangesArtifactNotificationsClosed = errors.New("changes artifact notification channel closed")

func (s *session) publishPendingChangesArtifacts(ctx context.Context) error {
	listContext, cancelList := context.WithTimeout(ctx, s.client.artifactCallLimit)
	publications, err := s.client.changesArtifacts.ListPendingChangesPublications(listContext)
	cancelList()
	if err != nil {
		return fmt.Errorf("list pending changes artifact publications: %w", err)
	}
	var publicationErrors []error
	if len(publications) > maximumPendingChangesPublications {
		publicationErrors = append(publicationErrors, fmt.Errorf(
			"changes artifact source returned more than %d pending publications",
			maximumPendingChangesPublications,
		))
		publications = publications[:maximumPendingChangesPublications]
	}
	artifactIDs := make(map[string]struct{}, len(publications))
	turns := make(map[string]struct{}, len(publications))
	for index, publication := range publications {
		if err := s.validateChangesArtifactPublication(publication); err != nil {
			publicationErrors = append(publicationErrors, fmt.Errorf(
				"pending changes artifact %d: %w", index, err,
			))
			continue
		}
		if _, duplicate := artifactIDs[publication.Params.ArtifactID]; duplicate {
			publicationErrors = append(publicationErrors, fmt.Errorf(
				"pending changes artifact %d: duplicate artifactId", index,
			))
			continue
		}
		artifactIDs[publication.Params.ArtifactID] = struct{}{}
		turnKey := publication.Source.TreeID + "\x00" + publication.Source.AgentID +
			"\x00" + publication.Params.TurnID
		if _, duplicate := turns[turnKey]; duplicate {
			publicationErrors = append(publicationErrors, fmt.Errorf(
				"pending changes artifact %d: duplicate worker turn", index,
			))
			continue
		}
		turns[turnKey] = struct{}{}

		callContext, cancelCall := context.WithTimeout(ctx, s.client.artifactCallLimit)
		payload, err := s.call(
			callContext,
			protocol.MethodPublishChangesArtifact,
			publication.Source.TreeID,
			&publication.Source,
			publication.Params,
		)
		cancelCall()
		if err != nil {
			publicationErrors = append(publicationErrors, fmt.Errorf(
				"publish pending changes artifact %d: %w", index, err,
			))
			continue
		}
		var result protocol.PublishChangesArtifactResult
		if err := decodeResult(payload, &result); err != nil {
			publicationErrors = append(publicationErrors, fmt.Errorf(
				"decode pending changes artifact %d publication result: %w", index, err,
			))
			continue
		}
		if err := result.Validate(); err != nil || result.ArtifactID != publication.Params.ArtifactID {
			publicationErrors = append(publicationErrors, fmt.Errorf(
				"pending changes artifact %d: broker returned a mismatched publication result", index,
			))
			continue
		}
		acknowledgeContext, cancelAcknowledge := context.WithTimeout(
			ctx, s.client.artifactCallLimit,
		)
		err = s.client.changesArtifacts.AcknowledgeChangesArtifact(
			acknowledgeContext, publication, result.Sequence,
		)
		cancelAcknowledge()
		if err != nil {
			publicationErrors = append(publicationErrors, fmt.Errorf(
				"acknowledge pending changes artifact %d publication: %w", index, err,
			))
		}
	}
	return errors.Join(publicationErrors...)
}

func (s *session) validateChangesArtifactPublication(publication ChangesArtifactPublication) error {
	if err := publication.Source.Validate(); err != nil {
		return fmt.Errorf("source: %w", err)
	}
	if publication.Source.ControllerID != s.client.hello.ControllerID ||
		publication.Source.DeviceID != s.client.hello.DeviceID ||
		publication.Source.ParentAgentID == "" {
		return errors.New("changes artifact source returned an unauthorized worker principal")
	}
	if err := publication.Params.Validate(); err != nil {
		return fmt.Errorf("metadata: %w", err)
	}
	return nil
}

func (s *session) changesArtifactLoop() {
	retryDelay := s.client.reconnectMin
	for {
		err := s.publishPendingChangesArtifacts(s.context)
		if err != nil {
			s.client.reportError(fmt.Errorf("publish pending changes artifacts: %w", err))
			if err := waitContext(s.context, retryDelay); err != nil {
				return
			}
			retryDelay = min(retryDelay*2, s.client.reconnectMax)
			continue
		}
		retryDelay = s.client.reconnectMin
		select {
		case <-s.done:
			return
		case _, ok := <-s.client.artifactChanges:
			if !ok {
				s.close(errChangesArtifactNotificationsClosed)
				return
			}
		}
	}
}
