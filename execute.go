package rqlite

import (
	"bytes"
	"encoding/json"
	"net/http"
)

// ExecuteResponse is the server response for an Execute request. It contains results
// for each statement along with timing and Raft information.
type ExecuteResponse struct {
	Results []ExecuteResult
	Timing  float64 `json:"time"`
	Raft    RaftResponse
}

type RaftResponse struct {
	Index  int
	NodeID string `json:"node_id"`
}

// ExecuteResult is the result of executing a single SQL statement.
type ExecuteResult struct {
	Err          string  `json:"error"` // TODO (br): Maybe this should be an error
	LastInsertID int     `json:"last_insert_id"`
	RowsAffected int     `json:"rows_affected"`
	Timing       float64 `json:"time"`
}

// Execute executes zero or more statements against the leader. It returns a non-nil error
// if the cluster can not be reached or if it is unable to execute the statements. Errors
// for each statement execution are set in each ExecuteResponse.Results.
func (db *DB) Execute(sqlStatements []string) (*ExecuteResponse, error) {
	return db.execute(sqlStatements, false)
}

// ExecuteAtomic executes zero or more statements atomically against the leader. Please
// see Execute for further details.
func (db *DB) ExecuteAtomic(sqlStatements []string) (*ExecuteResponse, error) {
	return db.execute(sqlStatements, true)
}

func (db *DB) execute(sqlStatements []string, opAtomic bool) (*ExecuteResponse, error) {
	jStatements, err := json.Marshal(sqlStatements)
	if err != nil {
		return nil, err
	}

	pp := db.PeerList()
	if len(pp) < 1 {
		return nil, ErrNoPeers
	}

	for _, p := range pp {
		resp, err := db.request(opEXECUTE, opAtomic, http.MethodPost, p, bytes.NewBuffer(jStatements))
		if err != nil {
			continue
		}

		ret := &ExecuteResponse{}
		if err = json.Unmarshal(resp, ret); err != nil { // TODO (br): use json.Decode and read directly from Body
			continue
		}

		return ret, nil
	}

	return nil, ErrPeersUnavailable
}
