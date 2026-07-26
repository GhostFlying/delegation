package cli

import (
	"context"
	"errors"

	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/resultpackagefiles"
)

type managedResultPackageManager interface {
	ReadResultPackagePart(context.Context, resultpackagefiles.ReadRequest) (protocol.ReadResultPackagePartResult, error)
	BeginResultPackage(context.Context, resultpackagefiles.BeginRequest) (protocol.BeginResultPackageResult, error)
	WriteResultPackagePart(context.Context, resultpackagefiles.WriteRequest) (protocol.WriteResultPackagePartResult, error)
	FinishResultPackage(context.Context, resultpackagefiles.FinishRequest) (protocol.FinishResultPackageResult, error)
	CancelResultPackage(context.Context, resultpackagefiles.CancelRequest) (protocol.CancelResultPackageResult, error)
	AcknowledgeResultPackage(context.Context, resultpackagefiles.AcknowledgeRequest) (protocol.AcknowledgeResultPackageResult, error)
}

var _ connector.ResultPackageManager = managedWorkerSpawner{}

func (s managedWorkerSpawner) ReadResultPackagePart(
	ctx context.Context,
	request connector.ResultPackageReadRequest,
) (protocol.ReadResultPackagePartResult, error) {
	if s.resultPackages == nil {
		return protocol.ReadResultPackagePartResult{}, errors.New("result package runtime is unavailable")
	}
	return s.resultPackages.ReadResultPackagePart(ctx, resultpackagefiles.ReadRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) BeginResultPackage(
	ctx context.Context,
	request connector.ResultPackageBeginRequest,
) (protocol.BeginResultPackageResult, error) {
	if s.resultPackages == nil {
		return protocol.BeginResultPackageResult{}, errors.New("result package runtime is unavailable")
	}
	return s.resultPackages.BeginResultPackage(ctx, resultpackagefiles.BeginRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) WriteResultPackagePart(
	ctx context.Context,
	request connector.ResultPackageWriteRequest,
) (protocol.WriteResultPackagePartResult, error) {
	if s.resultPackages == nil {
		return protocol.WriteResultPackagePartResult{}, errors.New("result package runtime is unavailable")
	}
	return s.resultPackages.WriteResultPackagePart(ctx, resultpackagefiles.WriteRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) FinishResultPackage(
	ctx context.Context,
	request connector.ResultPackageFinishRequest,
) (protocol.FinishResultPackageResult, error) {
	if s.resultPackages == nil {
		return protocol.FinishResultPackageResult{}, errors.New("result package runtime is unavailable")
	}
	return s.resultPackages.FinishResultPackage(ctx, resultpackagefiles.FinishRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) CancelResultPackage(
	ctx context.Context,
	request connector.ResultPackageCancelRequest,
) (protocol.CancelResultPackageResult, error) {
	if s.resultPackages == nil {
		return protocol.CancelResultPackageResult{}, errors.New("result package runtime is unavailable")
	}
	return s.resultPackages.CancelResultPackage(ctx, resultpackagefiles.CancelRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) AcknowledgeResultPackage(
	ctx context.Context,
	request connector.ResultPackageAcknowledgeRequest,
) (protocol.AcknowledgeResultPackageResult, error) {
	if s.resultPackages == nil {
		return protocol.AcknowledgeResultPackageResult{}, errors.New("result package runtime is unavailable")
	}
	return s.resultPackages.AcknowledgeResultPackage(ctx, resultpackagefiles.AcknowledgeRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}
