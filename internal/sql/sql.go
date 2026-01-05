package sql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/hashicorp/raft"
	_ "github.com/mattn/go-sqlite3"
)

type SQLFSM struct {
	name            string
	DB              *sql.DB
	AppliedCommands []Command
}

type Command struct {
	SQL string
}

func NewSQLFSM(name string) *SQLFSM {
	db, err := sql.Open("sqlite3", name)
	if err != nil {
		log.Fatalf("failed to open SQLite DB: %v", err)
	}
	return &SQLFSM{
		DB:   db,
		name: name,
	}
}

func (f *SQLFSM) Close() error {
	return f.DB.Close()
}

func (f *SQLFSM) Apply(l *raft.Log) interface{} {
	// sqlStmt := string(l.Data)
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		log.Printf("failed to unmarshal command: %v", err)
		return nil
	}

	sqlStmt := cmd.SQL

	fmt.Println("Applying SQL:", sqlStmt)
	_, err := f.DB.Exec(sqlStmt)
	if err != nil {
		log.Printf("SQL Exec error: %v", err)
	}
	f.AppliedCommands = append(f.AppliedCommands, Command{SQL: sqlStmt})
	return nil
}

func (f *SQLFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &NoopSnapshot{}, nil
}
func (f *SQLFSM) Restore(rc io.ReadCloser) error {
	return nil
}

type NoopSnapshot struct{}

func (s *NoopSnapshot) Persist(sink raft.SnapshotSink) error {
	sink.Cancel()
	return nil
}

func (s *NoopSnapshot) Release() {}
