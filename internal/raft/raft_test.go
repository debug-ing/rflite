package raft

import (
	"database/sql"
	"fmt"
	s "rflite/internal/sql"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func waitForLeader(r *raft.Raft, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if r.State() == raft.Leader {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("leader not elected")
}

func TestRaftMultiNodeReplication(t *testing.T) {
	// os.RemoveAll("testdata")
	// os.MkdirAll("testdata", 0755)

	// --- transports
	rc1, trans1 := raft.NewInmemTransport("node1")
	rc2, trans2 := raft.NewInmemTransport("node2")

	trans1.Connect(rc2, trans2)
	trans2.Connect(rc1, trans1)

	// --- node1 (leader)
	fsm1 := s.NewSQLFSM("testdata/db1.db")
	r1, err := NewTestRaftNode("node1", "127.0.0.1:13000", fsm1, trans1, true)
	if err != nil {
		t.Fatal(err)
	}

	if err := waitForLeader(r1, 2*time.Second); err != nil {
		t.Fatal(err)
	}

	// --- node2 (follower)
	fsm2 := s.NewSQLFSM("testdata/db2.db")
	_, err = NewTestRaftNode("node2", "127.0.0.1:13000", fsm2, trans2, false)
	if err != nil {
		t.Fatal(err)
	}

	// join node2
	f := r1.AddVoter(
		raft.ServerID("node2"),
		trans2.LocalAddr(),
		0,
		0,
	)
	if err := f.Error(); err != nil {
		t.Fatal(err)
	}

	// --- write on leader
	if err := r1.Apply([]byte(
		`CREATE TABLE test (id INTEGER);`,
	), time.Second).Error(); err != nil {
		t.Fatal(err)
	}

	if err := r1.Apply([]byte(
		`INSERT INTO test (id) VALUES (99);`,
	), time.Second).Error(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	// --- read from follower
	db, err := sql.Open("sqlite3", "testdata/db2.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var v int
	if err := db.QueryRow(`SELECT id FROM test`).Scan(&v); err != nil {
		t.Fatal(err)
	}

	if v != 99 {
		t.Fatalf("expected 99, got %d", v)
	}
}

func TestSingleNodeRaftWrite(t *testing.T) {
	fsm := s.NewSQLFSM("testdata/db_single.db")

	_, trans := raft.NewInmemTransport("node1")

	r, err := NewTestRaftNode("node1", "127.0.0.1:13000", fsm, trans, true)
	if err != nil {
		t.Fatal(err)
	}

	if err := waitForLeader(r, 2*time.Second); err != nil {
		t.Fatal(err)
	}

	if err := r.Apply([]byte(`CREATE TABLE test (id INTEGER);`), 5*time.Second).Error(); err != nil {
		t.Fatal(err)
	}
	if err := r.Apply([]byte(`INSERT INTO test (id) VALUES (42);`), 5*time.Second).Error(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite3", "testdata/db_single.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var val int
	if err := db.QueryRow(`SELECT id FROM test`).Scan(&val); err != nil {
		t.Fatal(err)
	}

	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}
