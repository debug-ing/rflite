package executer

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func BenchmarkSQLiteExecQueryConcurrent(b *testing.B) {
	dbFile := "test_load.db"
	exe := NewExecuter(dbFile)

	setupSQL := []string{
		`DROP TABLE IF EXISTS users;`,
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
	}
	if err := exe.Exec(setupSQL); err != nil {
		b.Fatalf("failed to setup db: %v", err)
	}

	// insert data
	insertSQL := []string{}
	for i := 0; i < 1000; i++ {
		insertSQL = append(insertSQL, fmt.Sprintf(`INSERT INTO users (name) VALUES ('user_%d');`, i))
	}
	if err := exe.Exec(insertSQL); err != nil {
		b.Fatalf("failed to insert data: %v", err)
	}
	fmt.Println("Data inserted success.")

	concurrency := 100
	queriesPerWorker := 60

	b.ResetTimer()
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < queriesPerWorker; j++ {
				sqlStr := fmt.Sprintf(`SELECT * FROM users WHERE id = %d;`, (workerID*queriesPerWorker+j)%1000)
				_, err := exe.ExecQuery(sqlStr)
				if err != nil {
					b.Errorf("worker %d failed: %v", workerID, err)
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)
	b.Logf("Executed %d SELECT queries in %v", concurrency*queriesPerWorker, elapsed)
}

func BenchmarkSQLiteMultiDBExecQuery(b *testing.B) {
	dbs := []string{
		"db1.db",
		"db2.db",
		"db3.db",
		"db4.db",
	}

	executers := make([]*Executer, len(dbs))
	for i, db := range dbs {
		executers[i] = NewExecuter(db)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for _, e := range executers {
				if _, err := e.ExecQuery("SELECT 1"); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

func setupTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	createTable := `
	CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT,
		age INTEGER
	);`
	_, err = db.Exec(createTable)
	if err != nil {
		t.Fatal(err)
	}

	insertData := `
	INSERT INTO users (name, age) VALUES
		('Alice', 30),
		('Bob', 25),
		('Charlie', 35);
	`
	_, err = db.Exec(insertData)
	if err != nil {
		t.Fatal(err)
	}

	return db
}

// func TestRowsToMap(t *testing.T) {
// 	db := setupTestDB(t)
// 	defer db.Close()

// 	rows, err := db.Query("SELECT id, name, age FROM users ORDER BY id")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer rows.Close()

// 	ex := &Executer{}
// 	result, err := ex.RowsToMap(rows)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	if len(result) != 3 {
// 		t.Fatalf("expected 3 rows, got %d", len(result))
// 	}

// 	expectedNames := []string{"Alice", "Bob", "Charlie"}
// 	for i, row := range result {
// 		if row["name"] != expectedNames[i] {
// 			t.Errorf("expected name %s, got %v", expectedNames[i], row["name"])
// 		}
// 	}
// }

func BenchmarkRowsToMap(b *testing.B) {
	db := setupTestDB(nil)
	defer db.Close()

	ex := &Executer{}

	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT id, name, age FROM users ORDER BY id")
		if err != nil {
			b.Fatal(err)
		}

		_, err = ex.RowsToMap(rows)
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}
