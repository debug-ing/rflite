package raft

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkRaftApplyInsert(b *testing.B) {
	dbIDs := []string{"db1", "db2", "db3"}
	base := "benchdata"
	startPort := 7600

	manager, err := NewDBManager(base, dbIDs, startPort)
	if err != nil {
		b.Fatalf("failed to create DBManager: %v", err)
	}

	time.Sleep(2 * time.Second)

	createCmd := Command{
		SQL: "CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, val TEXT)",
	}
	for _, db := range dbIDs {
		if err := manager.ApplyCommand(db, createCmd); err != nil {
			b.Fatalf("failed to create table for %s: %v", db, err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cmd := Command{
			SQL: fmt.Sprintf(
				"INSERT INTO test_table (id, val) VALUES (%d, 'value')",
				i+1,
			),
		}

		for _, db := range dbIDs {
			if err := manager.ApplyCommand(db, cmd); err != nil {
				b.Fatalf("apply failed for %s: %v", db, err)
			}
		}
	}
}
