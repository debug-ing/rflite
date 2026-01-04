package raft

import (
	"rflite/internal/sql"
	"time"

	"github.com/hashicorp/raft"
)

func NewTestRaftNode(
	id string,
	bindAddr string,
	fsm *sql.SQLFSM,
	trans *raft.InmemTransport,
	bootstrap bool,
) (*raft.Raft, error) {

	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(id)
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	r, err := raft.NewRaft(
		cfg,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		trans,
	)
	if err != nil {
		return nil, err
	}

	if bootstrap {
		r.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(id),
					Address: raft.ServerAddress(bindAddr),
				},
			},
		})
	}

	return r, nil
}
