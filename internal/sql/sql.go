package sql

import (
	"database/sql"
	"io"
	"log"
	"rflite/internal/executer"
	"rflite/pkg"

	"github.com/hashicorp/raft"
	_ "github.com/mattn/go-sqlite3"
)

type SQLFSM struct {
	name string
	DB   *sql.DB
}

func NewSQLFSM(name string) *SQLFSM {
	return &SQLFSM{
		name: name,
	}
}

func (f *SQLFSM) Close() error {
	return f.DB.Close()
}

func (f *SQLFSM) Apply(l *raft.Log) interface{} {
	sqlStmt := string(l.Data)
	db, q, err := pkg.ParseUseQuery(sqlStmt)
	if err != nil {
		log.Printf("SQL Parse error: %v", err)
		return nil
	}
	exec := executer.NewExecuter(db)
	err = exec.Exec(q)
	if err != nil {
		log.Printf("SQL Exec error: %v", err)
	}
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
