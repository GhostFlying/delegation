package protocol

import (
	"errors"
	"math"
)

const PeerStateRollbackCode = "peer_worker_revision_rollback"

type PeerStateRollbackErrorData struct {
	Code                 string `json:"code"`
	PeerWorkerRevision   uint64 `json:"peerWorkerRevision"`
	BrokerWorkerRevision uint64 `json:"brokerWorkerRevision"`
}

func (d PeerStateRollbackErrorData) Validate() error {
	if d.Code != PeerStateRollbackCode {
		return errors.New("peer state rollback error code is invalid")
	}
	if d.BrokerWorkerRevision == 0 || d.BrokerWorkerRevision > math.MaxInt64 ||
		d.PeerWorkerRevision >= d.BrokerWorkerRevision {
		return errors.New("peer state rollback revisions are invalid")
	}
	return nil
}
