package pkg

import (
	"reflect"
	"testing"
)

func TestParseUseQuery(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantDB  string
		wantQ   []string
		wantErr bool
	}{
		{
			name:    "single query",
			sql:     "USE mydb; SELECT * FROM foo;",
			wantDB:  "mydb",
			wantQ:   []string{"SELECT * FROM foo"},
			wantErr: false,
		},
		{
			name:    "multiple queries",
			sql:     "USE mydb; SELECT * FROM foo; INSERT INTO bar VALUES (1); UPDATE bar SET id=2;",
			wantDB:  "mydb",
			wantQ:   []string{"SELECT * FROM foo", "INSERT INTO bar VALUES (1)", "UPDATE bar SET id=2"},
			wantErr: false,
		},
		{
			name:    "no USE statement",
			sql:     "SELECT * FROM foo;",
			wantDB:  "",
			wantQ:   nil,
			wantErr: true,
		},
		{
			name:    "lowercase use",
			sql:     "use testdb; SELECT 1;",
			wantDB:  "testdb",
			wantQ:   []string{"SELECT 1"},
			wantErr: false,
		},
		{
			name:    "extra spaces",
			sql:     "  USE   dbname  ;   SELECT 1 ;  INSERT INTO t VALUES(2); ",
			wantDB:  "dbname",
			wantQ:   []string{"SELECT 1", "INSERT INTO t VALUES(2)"},
			wantErr: false,
		},
		{
			name:    "semicolon at end",
			sql:     "USE dbname; SELECT 1",
			wantDB:  "dbname",
			wantQ:   []string{"SELECT 1"},
			wantErr: false,
		},
		{
			name:    "only USE",
			sql:     "USE mydb;",
			wantDB:  "mydb",
			wantQ:   []string{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, q, err := ParseUseQuery(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseUseQuery() error = %v, wantErr %v", err, tt.wantErr)
			}
			if db != tt.wantDB {
				t.Errorf("ParseUseQuery() db = %v, want %v", db, tt.wantDB)
			}

			if len(tt.wantQ) != 0 && !reflect.DeepEqual(q, tt.wantQ) {
				t.Errorf("ParseUseQuery() queries = %v, want %v", q, tt.wantQ)
			}
		})
	}
}
