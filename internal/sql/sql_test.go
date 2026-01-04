package sql

// import (
// 	"testing"
// )

// func TestNewSQL(t *testing.T) {
// 	db, err := NewSQL(":memory:")
// 	if err != nil {
// 		t.Fatalf("NewSQL failed: %v", err)
// 	}
// 	if db.DB == nil {
// 		t.Fatal("DB should not be nil")
// 	}
// }

// func TestApply_InsertAndQuery(t *testing.T) {
// 	db, err := NewSQL("sample_test.db")
// 	if err != nil {
// 		t.Fatalf("NewSQL failed: %v", err)
// 	}

// 	// ایجاد جدول تست
// 	createTable := `
// 	CREATE TABLE users (
// 		id INTEGER PRIMARY KEY AUTOINCREMENT,
// 		name TEXT
// 	);`
// 	result := db.Apply(createTable)
// 	if result == nil {
// 		t.Fatal("Create table failed")
// 	}

// 	// درج یک رکورد
// 	insertQuery := `INSERT INTO users (name) VALUES ('Alice');`
// 	res := db.Apply(insertQuery)
// 	if res == nil {
// 		t.Fatal("Insert failed")
// 	}

// 	// بررسی تعداد رکوردها
// 	row := db.DB.QueryRow("SELECT COUNT(*) FROM users")
// 	var count int
// 	if err := row.Scan(&count); err != nil {
// 		t.Fatalf("QueryRow failed: %v", err)
// 	}
// 	if count != 1 {
// 		t.Fatalf("Expected 1 row, got %d", count)
// 	}
// }

// func TestApply_InvalidQuery(t *testing.T) {
// 	db, err := NewSQL("sample_test.db")
// 	if err != nil {
// 		t.Fatalf("NewSQL failed: %v", err)
// 	}

// 	res := db.Apply("INVALID SQL QUERY")
// 	if res != nil {
// 		t.Fatal("Expected nil result for invalid query")
// 	}
// }
