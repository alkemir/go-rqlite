package rqlite

import (
	"bytes"
	"encoding/json"
	"net/http"
)

type ExecuteResponse struct {
	Results []ExecuteResult
	Timing  float64 `json:"time"`
	Raft    RaftResponse
}

type RaftResponse struct {
	Index  int
	NodeID string `json:"node_id"`
}

type ExecuteResult struct {
	Err          string  `json:"error"` // TODO (br): Maybe this should be an error
	LastInsertID int     `json:"last_insert_id"`
	RowsAffected int     `json:"rows_affected"`
	Timing       float64 `json:"time"`
}

func (db *DB) Execute(sqlStatements []string) (*ExecuteResponse, error) {
	jStatements, err := json.Marshal(sqlStatements)
	if err != nil {
		return nil, err
	}

	pp := db.cluster.PeerList()
	if len(pp) < 1 {
		return nil, ErrNoPeers
	}

	for _, p := range pp {
		resp, err := db.request(apiWRITE, http.MethodPost, p, bytes.NewBuffer(jStatements))
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
