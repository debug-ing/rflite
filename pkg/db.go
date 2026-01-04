package pkg

import (
	"fmt"
	"regexp"
	"strings"
)

var ErrNoDatabase = fmt.Errorf("no USE statement found")

// ParseUseQuery parses a SQL string like "USE db; SQL1; SQL2;"
// and returns the database name and a slice of queries.
func ParseUseQuery(sql string) (db string, queries []string, err error) {
	re := regexp.MustCompile(`(?i)USE\s+([a-zA-Z0-9_]+)\s*;`)

	match := re.FindStringSubmatch(sql)
	loc := re.FindStringIndex(sql) // loc[0] = start, loc[1] = end
	if len(match) < 2 || loc == nil {
		return "", nil, ErrNoDatabase
	}

	db = match[1]

	rest := sql[loc[1]:]

	stmts := strings.Split(rest, ";")
	for _, s := range stmts {
		s = strings.TrimSpace(s)
		if s != "" {
			queries = append(queries, s)
		}
	}

	return db, queries, nil
}
