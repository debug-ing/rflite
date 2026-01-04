package store

import (
	"os"
	"strings"
)

type Store struct {
}

func NewStore() *Store {
	return &Store{}
}

func (s *Store) Close() error {
	return nil
}

func (s *Store) ListDatabases() []string {
	var dbs []string
	files, err := os.ReadDir("./db")
	if err != nil {
		return dbs
	}
	for _, f := range files {
		if f.IsDir() {
			continue
		}

		name := f.Name()
		if strings.HasSuffix(name, ".db") {
			dbName := strings.TrimSuffix(name, ".db")
			dbs = append(dbs, dbName)
		}
	}
	return dbs
}

func (s *Store) DeleteDatabase(name string) error {
	path := "./db/" + name + ".db"
	return os.Remove(path)
}

func (s *Store) DatabaseExists(name string) bool {
	path := "./db/" + name + ".db"
	_, err := os.Stat(path)
	return err == nil
}
