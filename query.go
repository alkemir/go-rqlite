package rqlite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// QueryResponse is the server response for an Query request. It contains results
// for each statement along with timing information.
type QueryResponse struct {
	Results []QueryResult
	Timing  float64 `json:"time"`
}

// QueryResult is the result of a single SQL query.
type QueryResult struct {
	Err     string `json:"error"` // TODO (br): Maybe this should be an error
	Columns []string
	Types   []string
	Values  []interface{}
	Timing  float64 `json:"time"`
}

// Query queries the cluster with SQL statements and returns the results of each statement. It
// returns a non-nil error if the cluster can not be reached or if it is unable to execute the
// statements. Errors for each statement are set in each QueryResponse.Results.
func (db *DB) Query(sqlStatements []string) (*QueryResponse, error) {
	jStatements, err := json.Marshal(sqlStatements)
	if err != nil {
		return nil, err
	}

	pp := db.PeerList()
	if len(pp) < 1 {
		return nil, ErrNoPeers
	}

	for _, p := range pp {
		resp, err := db.request(opQUERY, false, http.MethodPost, p, bytes.NewBuffer(jStatements))
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
