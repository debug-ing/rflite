package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"rflite/internal/sql"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// DBManager keeps track of all Raft nodes
type DBManager struct {
	Rafts map[string]*raft.Raft
	FSMs  map[string]*sql.SQLFSM
	mu    sync.RWMutex
}

// Command represents an operation for SQLFSM
type Command struct {
	SQL string
}

// NewDBManager initializes multiple Raft nodes (1 per DB) on a single port with multiplexing
func NewDBManager(basePath string, dbIDs []string, port int) (*DBManager, error) {
	manager := &DBManager{
		Rafts: make(map[string]*raft.Raft),
		FSMs:  make(map[string]*sql.SQLFSM),
	}
	//
	_, trans1 := raft.NewInmemTransport("node1")
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, err
	}
	log.Printf("Multiplexed Raft listener on port %d", port)

	for _, dbID := range dbIDs {
		dbPath := filepath.Join(basePath, dbID)
		if err := os.MkdirAll(dbPath, 0755); err != nil {
			return nil, err
		}

		// FSM for this DB
		fsm := sql.NewSQLFSM(filepath.Join(dbPath, "snapshot.sqlite"))
		manager.FSMs[dbID] = fsm

		// Raft stores
		logStore, err := raftboltdb.NewBoltStore(filepath.Join(dbPath, "raft-log.bolt"))
		if err != nil {
			return nil, err
		}
		stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dbPath, "raft-stable.bolt"))
		if err != nil {
			return nil, err
		}
		snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(dbPath, "snapshot"), 1, os.Stdout)
		if err != nil {
			return nil, err
		}

		cfg := raft.DefaultConfig()
		cfg.LocalID = raft.ServerID(dbID)
		cfg.HeartbeatTimeout = 100 * time.Millisecond
		cfg.ElectionTimeout = 100 * time.Millisecond
		cfg.LeaderLeaseTimeout = 100 * time.Millisecond
		cfg.SnapshotThreshold = 1024

		trans := NewMuxTransport(ln, dbID)
		r, err := raft.NewRaft(cfg, fsm, logStore, stableStore, snapshotStore, trans1)
		if err != nil {
			return nil, err
		}

		// Bootstrap cluster single node
		r.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(dbID),
					Address: trans.LocalAddr(),
				},
			},
		})

		manager.Rafts[dbID] = r
		log.Printf("Raft node %s initialized on muxed port %d", dbID, port)
	}

	return manager, nil
}

func (m *DBManager) ApplyCommand(dbID string, cmd Command) error {
	m.mu.RLock()
	r, ok := m.Rafts[dbID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("DB %s not found", dbID)
	}

	_, ok = m.FSMs[dbID]
	if !ok {
		return fmt.Errorf("FSM for DB %s not found", dbID)
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	if r.State() != raft.Leader {
		return fmt.Errorf("node %s is not leader", dbID)
	}

	future := r.Apply(data, 5*time.Second)
	return future.Error()
}

func (m *DBManager) AllLeadersOK() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, r := range m.Rafts {
		if r.State() != raft.Leader {
			return false
		}
	}
	return true
}

type MuxTransport struct {
	listener net.Listener
	dbID     string
}

func NewMuxTransport(ln net.Listener, dbID string) *MuxTransport {
	t := &MuxTransport{
		listener: ln,
		dbID:     dbID,
	}
	go t.acceptLoop()
	return t
}

func (t *MuxTransport) acceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			log.Printf("MuxTransport accept error: %v", err)
			return
		}
		go t.handleConn(conn)
	}
}

func (t *MuxTransport) handleConn(conn net.Conn) {
	defer conn.Close()
	idBuf := make([]byte, len(t.dbID))
	_, err := io.ReadFull(conn, idBuf)
	if err != nil {
		log.Printf("MuxTransport read DBID error: %v", err)
		return
	}
	recvDBID := string(idBuf)
	if recvDBID != t.dbID {
		return
	}
	// TODO: Now pass this connection to Raft and apply it
}

// raft.Transport interface
func (t *MuxTransport) LocalAddr() raft.ServerAddress {
	return raft.ServerAddress(t.listener.Addr().String())
}
func (t *MuxTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	return nil, nil
}
func (t *MuxTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	return nil
}
func (t *MuxTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	return nil
}
func (t *MuxTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse) error {
	return nil
}
func (t *MuxTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress, w io.Writer) error {
	return nil
}
func (t *MuxTransport) DecodePeer(r io.Reader) (raft.ServerID, raft.ServerAddress, error) {
	return "", "", nil
}
func (t *MuxTransport) Consumer() <-chan raft.RPC { return nil }
func (t *MuxTransport) Cancel()                   {}
