package executer

import (
	"database/sql"
	"log"
	"sync"

	"github.com/jacob2161/sqlitebp"
)

var rowMapPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]interface{})
	},
}

type Executer struct {
	db   string
	read *sql.DB
}

func NewExecuter(name string) *Executer {
	db, err := sqlitebp.OpenReadOnly(name)
	if err != nil {
		log.Fatal(err)
	}
	// return db, nil
	return &Executer{
		db:   name,
		read: db,
	}
}

func (e *Executer) Exec(sql []string) error {
	open, err := e.open(e.db)
	if err != nil {
		return err
	}
	for _, sql := range sql {
		_, err = open.Exec(sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Executer) ExecQuery(sqlStr string) ([]map[string]interface{}, error) {
	// e.openReadOnly(e.db)
	db, err := sqlitebp.OpenReadOnly(e.db)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(sqlStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	//
	return e.RowsToMap(rows)
}

func (f *Executer) open(name string) (*sql.DB, error) {
	db, err := sqlitebp.OpenReadWriteCreate(name)
	if err != nil {
		log.Fatal(err)
	}
	f.read = db
	return db, nil
}

func (f *Executer) openReadOnly(name string) (*sql.DB, error) {
	db, err := sqlitebp.OpenReadOnly(name)
	if err != nil {
		log.Fatal(err)
	}
	return db, nil
}

func (f *Executer) Close() error {
	return f.read.Close()
}

func (e *Executer) RowsToMap(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	row := make([]interface{}, len(columns))
	for i := range row {
		row[i] = new(sql.RawBytes)
	}

	var result []map[string]interface{}

	for rows.Next() {
		if err := rows.Scan(row...); err != nil {
			return nil, err
		}
		rowMap := make(map[string]interface{}, len(columns))
		for i, colName := range columns {
			if col, ok := row[i].(*sql.RawBytes); ok {
				// RawBytes → string
				rowMap[colName] = string(*col)
			} else {
				rowMap[colName] = nil
			}
		}
		result = append(result, rowMap)
	}

	return result, rows.Err()
}

func ReturnMapsToPool(maps []map[string]interface{}) {
	for _, m := range maps {
		rowMapPool.Put(m)
	}
}
