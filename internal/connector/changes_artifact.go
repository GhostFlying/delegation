package connector

import (
	"context"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/protocol"
)

const maximumPendingChangesPublications = 64

func (s *session) publishPendingChangesArtifacts(ctx context.Context) error {
	publications, err := s.client.changesArtifacts.ListPendingChangesPublications(ctx)
	if err != nil {
		return fmt.Errorf("list pending changes artifact publications: %w", err)
	}
	if len(publications) > maximumPendingChangesPublications {
		return fmt.Errorf(
			"changes artifact source returned more than %d pending publications",
			maximumPendingChangesPublications,
		)
	}
	artifactIDs := make(map[string]struct{}, len(publications))
	turns := make(map[string]struct{}, len(publications))
	for index, publication := range publications {
		if err := s.validateChangesArtifactPublication(publication); err != nil {
			return fmt.Errorf("pending changes artifact %d: %w", index, err)
		}
		if _, duplicate := artifactIDs[publication.Params.ArtifactID]; duplicate {
			return errors.New("changes artifact source returned a duplicate artifactId")
		}
		artifactIDs[publication.Params.ArtifactID] = struct{}{}
		turnKey := publication.Source.TreeID + "\x00" + publication.Source.AgentID +
			"\x00" + publication.Params.TurnID
		if _, duplicate := turns[turnKey]; duplicate {
			return errors.New("changes artifact source returned a duplicate worker turn")
		}
		turns[turnKey] = struct{}{}
	}
	for _, publication := range publications {
		payload, err := s.call(
			ctx,
			protocol.MethodPublishChangesArtifact,
			publication.Source.TreeID,
			&publication.Source,
			publication.Params,
		)
		if err != nil {
			return fmt.Errorf("publish changes artifact: %w", err)
		}
		var result protocol.PublishChangesArtifactResult
		if err := decodeResult(payload, &result); err != nil {
			return fmt.Errorf("decode changes artifact publication result: %w", err)
		}
		if err := result.Validate(); err != nil || result.ArtifactID != publication.Params.ArtifactID {
			return errors.New("broker returned a mismatched changes artifact publication result")
		}
		if err := s.client.changesArtifacts.AcknowledgeChangesArtifact(
			ctx, publication, result.Sequence,
		); err != nil {
			return fmt.Errorf("acknowledge changes artifact publication: %w", err)
		}
	}
	return nil
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
	for {
		select {
		case <-s.done:
			return
		case _, ok := <-s.client.artifactChanges:
			if !ok {
				s.close(errors.New("changes artifact notification channel closed"))
				return
			}
		}
		ctx, cancel := context.WithTimeout(s.context, connectTimeout)
		err := s.publishPendingChangesArtifacts(ctx)
		cancel()
		if err != nil {
			s.close(err)
			return
		}
	}
}
