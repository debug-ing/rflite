package raft

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestDBManager(t *testing.T) {
	dbIDs := []string{"db1", "db2", "db3", "db4", "db5"}

	manager, err := NewDBManager("testdata", dbIDs, 7100)
	if err != nil {
		t.Fatalf("failed to create DBManager: %v", err)
	}

	if len(manager.Rafts) != len(dbIDs) {
		t.Fatalf("expected %d Raft nodes, got %d", len(dbIDs), len(manager.Rafts))
	}

	time.Sleep(3 * time.Second) // wait for leaders to be elected

	if !manager.AllLeadersOK() {
		t.Fatalf("not all Raft nodes have a leader")
	}

	for _, db := range dbIDs {
		createCmd := Command{
			SQL: "CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY)",
		}
		if err := manager.ApplyCommand(db, createCmd); err != nil {
			t.Fatalf("failed to create table for %s: %v", db, err)
		}
	}

	for i, db := range dbIDs {
		cmd := Command{
			SQL: fmt.Sprintf("INSERT INTO test_table (id) VALUES (%d)", i+1),
		}
		if err := manager.ApplyCommand(db, cmd); err != nil {
			t.Fatalf("failed to apply command for %s: %v", db, err)
		}
	}

	for i, db := range dbIDs {
		fsm := manager.FSMs[db]
		if len(fsm.AppliedCommands) == 0 {
			t.Fatalf("no commands applied for %s", db)
		}
		lastCmd := fsm.AppliedCommands[len(fsm.AppliedCommands)-1]
		expected := fmt.Sprintf("INSERT INTO test_table (id) VALUES (%d)", i+1)
		if lastCmd.SQL != expected {
			t.Fatalf("expected last command '%s', got '%s'", expected, lastCmd.SQL)
		}
	}
}

func TestDBManagerMultiNodeConnectivity(t *testing.T) {
	dbIDs := []string{"db1", "db2", "db3", "db4", "db5", "db6", "db7"}
	manager, err := NewDBManager("testdata-multi", dbIDs, 7200)
	if err != nil {
		t.Fatalf("failed to create DBManager: %v", err)
	}

	time.Sleep(3 * time.Second)

	for _, db := range dbIDs {
		r := manager.Rafts[db]
		if r.Leader() == "" {
			t.Errorf("node %s has no leader", db)
		} else {
			t.Logf("node %s sees leader %s", db, r.Leader())
		}

		state := r.State()
		if state != raft.Leader && state != raft.Follower && state != raft.Candidate {
			t.Errorf("node %s has unexpected state %v", db, state)
		} else {
			t.Logf("node %s state: %v", db, state)
		}
	}

	if !manager.AllLeadersOK() {
		t.Log("Warning: not all nodes are leaders yet (normal in multi-node cluster)")
	} else {
		t.Log("All nodes have recognized a leader")
	}
}

// func TestMultipleDBManagersConnectivity(t *testing.T) {
// 	managerCount := 3
// 	dbIDs := []string{"db1", "db2", "db3"}

// 	managers := make([]*DBManager, 0, managerCount)

// 	for i := 0; i < managerCount; i++ {
// 		basePath := fmt.Sprintf("testdata-manager-%d", i+1)
// 		startPort := 7300 + i*10
// 		manager, err := NewDBManager(basePath, dbIDs, startPort)
// 		if err != nil {
// 			t.Fatalf("failed to create DBManager %d: %v", i+1, err)
// 		}
// 		managers = append(managers, manager)
// 	}

// 	time.Sleep(3 * time.Second)

// 	for mi, manager := range managers {
// 		for _, db := range dbIDs {
// 			r := manager.Rafts[db]
// 			if r.Leader() == "" {
// 				t.Errorf("manager %d, node %s has no leader", mi+1, db)
// 			} else {
// 				t.Logf("manager %d, node %s sees leader %s", mi+1, db, r.Leader())
// 			}
// 		}
// 	}
// }

func TestClusterLeaderFailoverAcrossDBs(t *testing.T) {
	dbIDs := []string{"db1", "db2", "db3"}

	nodeBasePaths := []string{"testdata-node1", "testdata-node2"}
	startPorts := []int{8000, 8100}

	type ClusterNode struct {
		Manager *DBManager
		ID      string
	}

	nodes := make([]ClusterNode, 0)

	for i, base := range nodeBasePaths {
		manager, err := NewDBManager(base, dbIDs, startPorts[i])
		if err != nil {
			t.Fatalf("failed to create DBManager for %s: %v", base, err)
		}
		nodes = append(nodes, ClusterNode{
			Manager: manager,
			ID:      fmt.Sprintf("node-%d", i+1),
		})
	}

	time.Sleep(3 * time.Second)

	var leaderNode *ClusterNode
	for _, node := range nodes {
		allOK := true
		for _, db := range dbIDs {
			r := node.Manager.Rafts[db]
			if r == nil || r.State() != raft.Leader {
				allOK = false
				break
			}
		}
		if allOK {
			leaderNode = &node
			break
		}
	}
	if leaderNode == nil {
		t.Fatalf("no leader node found initially")
	}
	t.Logf("initial leader node: %s", leaderNode.ID)

	for _, db := range dbIDs {
		future := leaderNode.Manager.Rafts[db].Shutdown()
		if err := future.Error(); err != nil {
			t.Fatalf("failed to shutdown leader raft for db %s: %v", db, err)
		}
	}
	t.Logf("leader node %s is now down", leaderNode.ID)

	time.Sleep(3 * time.Second)

	var newLeaderNode *ClusterNode
	for _, node := range nodes {
		if node.ID == leaderNode.ID {
			continue
		}
		allOK := true
		for _, db := range dbIDs {
			r := node.Manager.Rafts[db]
			if r == nil || r.State() != raft.Leader {
				allOK = false
				break
			}
		}
		if allOK {
			newLeaderNode = &node
			break
		}
	}
	if newLeaderNode == nil {
		t.Fatalf("no new leader node elected after original leader down")
	}
	t.Logf("new leader node: %s", newLeaderNode.ID)
}

func TestMultipleDBManagersConnectivity(t *testing.T) {
	base := "testdata-multi-manager"
	dbManagers := make([]*DBManager, 0)
	numManagers := 2
	dbIDs := []string{"db1", "db2", "db3"}
	startPorts := []int{9100, 9200}

	for i := 0; i < numManagers; i++ {
		manager, err := NewDBManager(fmt.Sprintf("%s-%d", base, i), dbIDs, startPorts[i])
		if err != nil {
			t.Fatalf("failed to create DBManager %d: %v", i, err)
		}
		dbManagers = append(dbManagers, manager)
	}

	time.Sleep(3 * time.Second)

	for i, manager := range dbManagers {
		if len(manager.Rafts) != len(dbIDs) {
			t.Fatalf("DBManager %d: expected %d Raft nodes, got %d", i, len(dbIDs), len(manager.Rafts))
		}

		for dbID, r := range manager.Rafts {
			state := r.State()
			leader := r.Leader()
			t.Logf("DBManager %d - Raft node %s: state=%s, leader=%s", i, dbID, state, leader)
			if leader == "" {
				t.Fatalf("DBManager %d - Raft node %s has no leader", i, dbID)
			}
		}
	}
}
