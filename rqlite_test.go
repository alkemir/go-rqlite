package rqlite_test

import (
	"testing"

	rqlite "github.com/alkemir/go-rqlite"
)

func TestParseOptions(t *testing.T) {
	_, err := rqlite.Open("")
	if err != nil {
		t.Fatal(err)
	}
}

func TestCreateWriteRead(t *testing.T) {
	db, err := rqlite.Open("http://localhost:4001")
	if err != nil {
		t.Fatal(err)
	}

	_, err := db.Write([]string{"CREATE TABLE foo (id integer not null primary key, name text)"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Write([]string{"INSERT INTO foo VALUES(1, 'one')"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Query([]string{"SELECT * FROM foo"})
	if err != nil {
		t.Fatal(err)
	}
}
