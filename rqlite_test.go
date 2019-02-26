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

	res, err := db.Execute([]string{"CREATE TABLE IF NOT EXISTS foo (id integer not null primary key, name text)"})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res.Results {
		if r.Err != "" {
			t.Fatal(r.Err)
		}
	}

	res, err = db.Execute([]string{"INSERT INTO foo VALUES(1, 'one')"})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res.Results {
		if r.Err != "" {
			t.Fatal(r.Err)
		}
	}

	resQ, err := db.Query([]string{"SELECT * FROM foo"})
	if err != nil {
		t.Fatal(err)
	}

	for _, r := range resQ.Results {
		if r.Err != "" {
			t.Fatal(r.Err)
		}
	}
}
