package connector

import (
	"context"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/protocol"
)

const maximumPendingResultPackagePublications = 64

var (
	// ErrPermanentResultPackagePublication marks a local invariant violation or
	// authoritative broker rejection that must stop the connector instead of retrying.
	ErrPermanentResultPackagePublication = errors.New("permanent result package publication failure")
	errResultPackageNotificationsClosed  = errors.New("result package notification channel closed")
)

func (s *session) publishPendingResultPackages(ctx context.Context) error {
	listContext, cancelList := context.WithTimeout(ctx, s.client.artifactCallLimit)
	publications, err := s.client.resultSource.ListPendingResultPackagePublications(listContext)
	cancelList()
	if err != nil {
		return fmt.Errorf("list pending result package publications: %w", err)
	}
	var publicationErrors []error
	if len(publications) > maximumPendingResultPackagePublications {
		publicationErrors = append(publicationErrors, permanentResultPackageError(fmt.Errorf(
			"result package source returned more than %d pending publications",
			maximumPendingResultPackagePublications,
		)))
		publications = publications[:maximumPendingResultPackagePublications]
	}
	packageIDs := make(map[string]struct{}, len(publications))
	turns := make(map[string]struct{}, len(publications))
	for index, publication := range publications {
		manifest, err := s.validateResultPackagePublication(publication)
		if err != nil {
			publicationErrors = append(publicationErrors, permanentResultPackageError(fmt.Errorf(
				"pending result package %d: %w", index, err,
			)))
			continue
		}
		if _, duplicate := packageIDs[manifest.PackageID]; duplicate {
			publicationErrors = append(publicationErrors, permanentResultPackageError(fmt.Errorf(
				"pending result package %d: duplicate packageId", index,
			)))
			continue
		}
		packageIDs[manifest.PackageID] = struct{}{}
		turnKey := publication.Source.TreeID + "\x00" + publication.Source.AgentID +
			"\x00" + manifest.TurnID
		if _, duplicate := turns[turnKey]; duplicate {
			publicationErrors = append(publicationErrors, permanentResultPackageError(fmt.Errorf(
				"pending result package %d: duplicate worker turn", index,
			)))
			continue
		}
		turns[turnKey] = struct{}{}

		callContext, cancelCall := context.WithTimeout(ctx, s.client.artifactCallLimit)
		payload, err := s.call(
			callContext,
			protocol.MethodPublishResultPackage,
			publication.Source.TreeID,
			&publication.Source,
			publication.Params,
		)
		cancelCall()
		if err != nil {
			if permanentResultPackageRPCError(err) {
				err = permanentResultPackageError(err)
			}
			publicationErrors = append(publicationErrors, fmt.Errorf(
				"publish pending result package %d: %w", index, err,
			))
			continue
		}
		var result protocol.PublishResultPackageResult
		if err := decodeResult(payload, &result); err != nil {
			publicationErrors = append(publicationErrors, permanentResultPackageError(fmt.Errorf(
				"decode pending result package %d publication result: %w", index, err,
			)))
			continue
		}
		if err := result.Validate(); err != nil || result.PackageID != manifest.PackageID {
			publicationErrors = append(publicationErrors, permanentResultPackageError(fmt.Errorf(
				"pending result package %d: broker returned a mismatched publication result", index,
			)))
			continue
		}
		acknowledgeContext, cancelAcknowledge := context.WithTimeout(
			ctx, s.client.artifactCallLimit,
		)
		err = s.client.resultSource.AcknowledgeResultPackageMetadata(
			acknowledgeContext, publication,
		)
		cancelAcknowledge()
		if err != nil {
			publicationErrors = append(publicationErrors, fmt.Errorf(
				"acknowledge pending result package %d publication: %w", index, err,
			))
		}
	}
	return errors.Join(publicationErrors...)
}

func permanentResultPackageError(err error) error {
	return fmt.Errorf("%w: %w", ErrPermanentResultPackagePublication, err)
}

func permanentResultPackageRPCError(err error) bool {
	var rpcError *RPCError
	return errors.As(err, &rpcError) && rpcError.Code != protocol.ErrorUnavailable
}

func (s *session) validateResultPackagePublication(
	publication ResultPackagePublication,
) (protocol.ResultManifest, error) {
	if err := publication.Source.Validate(); err != nil {
		return protocol.ResultManifest{}, fmt.Errorf("source: %w", err)
	}
	if publication.Source.ControllerID != s.client.hello.ControllerID ||
		publication.Source.DeviceID != s.client.hello.DeviceID ||
		publication.Source.ParentAgentID == "" {
		return protocol.ResultManifest{}, errors.New(
			"result package source returned an unauthorized worker principal",
		)
	}
	if err := publication.Params.Validate(); err != nil {
		return protocol.ResultManifest{}, fmt.Errorf("metadata: %w", err)
	}
	manifest, err := publication.Params.Metadata.DecodeManifest()
	if err != nil {
		return protocol.ResultManifest{}, fmt.Errorf("manifest: %w", err)
	}
	if manifest.ControllerID != publication.Source.ControllerID ||
		manifest.TreeID != publication.Source.TreeID ||
		manifest.SourceAgentID != publication.Source.AgentID ||
		manifest.SourceDeviceID != publication.Source.DeviceID {
		return protocol.ResultManifest{}, errors.New(
			"result package manifest differs from its worker principal",
		)
	}
	return manifest, nil
}

func (s *session) resultPackagePublicationLoop() {
	retryDelay := s.client.reconnectMin
	for {
		err := s.publishPendingResultPackages(s.context)
		if err != nil {
			s.client.reportError(fmt.Errorf("publish pending result packages: %w", err))
			if errors.Is(err, ErrPermanentResultPackagePublication) {
				s.close(err)
				return
			}
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
		case _, ok := <-s.client.resultChanges:
			if !ok {
				s.close(errResultPackageNotificationsClosed)
				return
			}
		}
	}
}
