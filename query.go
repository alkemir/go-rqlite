package rqlite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type QueryResponse struct {
	Results []QueryResult
	Timing  float64 `json:"time"`
}

type QueryResult struct {
	Err     string `json:"error"` // TODO (br): Maybe this should be an error
	Columns []string
	Types   []string
	Values  []interface{}
	Timing  float64 `json:"time"`
}

func (db *DB) Query(sqlStatements []string) (*QueryResponse, error) {
	jStatements, err := json.Marshal(sqlStatements)
	if err != nil {
		return nil, err
	}

	pp := db.cluster.PeerList()
	if len(pp) < 1 {
		return nil, ErrNoPeers
	}

	for _, p := range pp {
		resp, err := db.request(apiQUERY, http.MethodPost, p, bytes.NewBuffer(jStatements))
		if err != nil {
			continue
		}

		ret := &QueryResponse{}
		if err = json.Unmarshal(resp, ret); err != nil { // TODO (br): use json.Decode and read directly from Body
			fmt.Println(err)
			continue
		}

		return ret, nil
	}

	return nil, ErrPeersUnavailable
}
