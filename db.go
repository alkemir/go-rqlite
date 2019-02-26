package rqlite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// TODO(br): Check if we should update the cluster status on redirects or node failures.

// DB represents a connection to a rqlite cluster
type DB struct {
	client     http.Client
	wantsHTTPS bool
	user       string
	password   string

	cluster Cluster
	level   consistencyLevel
}

// Open database and return a new connection.
//
// You can specify a DSN string using a URL.
//   test.db
//
// You scan specify values for the following options:
// timeout (100 ms)
// consistency_level (STRONG)
func Open(dsn string) (*DB, error) {
	u, errP := url.Parse(dsn)
	if errP != nil {
		return nil, errP
	}

	if u.Opaque != "" {
		return nil, fmt.Errorf("URL could not be fully understood: %q", u.Opaque)
	}

	timeOut := 100 * time.Millisecond
	conLevel := clSTRONG
	user := u.User.Username()
	password, _ := u.User.Password()

	params, errP := url.ParseQuery(u.RawQuery)
	if errP != nil {
		return nil, errP
	}

	if val := params.Get("timeout"); val != "" {
		iv, errI := strconv.ParseInt(val, 10, 64)
		if errI != nil {
			return nil, fmt.Errorf("Invalid timeout: %q: %v", val, errI)
		}
		timeOut = time.Duration(iv) * time.Millisecond
	}

	if val := params.Get("consistency_level"); val != "" {
		switch strings.ToLower(val) {
		case "none":
			conLevel = clNONE
		case "weak":
			conLevel = clWEAK
		case "strong":
			conLevel = clSTRONG
		default:
			return nil, fmt.Errorf("Invalid consistency level: %q", val)
		}
	}

	leaderHost := "localhost"
	leaderPort := "4001"
	if u.Host != "" {
		leaderHost = u.Host
		if h, p, err := net.SplitHostPort(u.Host); err == nil {
			leaderHost = h
			leaderPort = p
		}
	}

	db := &DB{
		client: http.Client{
			Timeout: timeOut,
		},
		user:       user,
		password:   password,
		level:      conLevel,
		wantsHTTPS: u.Scheme == "https",
		cluster:    Cluster{leader: Peer{hostname: leaderHost, port: leaderPort}},
	}

	if err := db.updateClusterInfo(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) assembleURL(apiOp apiOperation, p Peer) string {
	var sb strings.Builder
	// TODO(br): Move this code to peer initialization, as it never changes during its lifecycle

	if db.wantsHTTPS {
		sb.WriteString("https://")
	} else {
		sb.WriteString("http://")
	}

	if db.user != "" {
		sb.WriteString(db.user)
		sb.WriteString(":")
		sb.WriteString(db.password)
		sb.WriteString("@")
	}

	sb.WriteString(p.hostname)
	sb.WriteString(":")
	sb.WriteString(p.port)

	switch apiOp {
	case apiSTATUS:
		sb.WriteString("/status")
	case apiQUERY:
		sb.WriteString("/db/query")
	case apiWRITE:
		sb.WriteString("/db/execute")
	}

	if apiOp == apiQUERY || apiOp == apiWRITE {
		sb.WriteString("?timings&transaction&level=")
		sb.WriteString(string(db.level))
	}

	return sb.String()
}

func (db *DB) Status() (*ClusterStatus, error) {
	pp := db.cluster.PeerList()
	if len(pp) < 1 {
		return nil, ErrNoPeers
	}

	for _, p := range pp {
		resp, err := db.request(apiSTATUS, http.MethodGet, p, nil)
		if err != nil {
			continue
		}

		clStatus := ClusterStatus{}
		if err = json.Unmarshal(resp, &clStatus); err != nil {
			return nil, err
		}

		return &clStatus, nil
	}

	return nil, ErrPeersUnavailable
}

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

func (db *DB) Query(sqlStatements []string) ([]byte, error) {
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

		return resp, nil
	}

	return nil, ErrPeersUnavailable
}

func (db *DB) request(apiOP apiOperation, method string, p Peer, reqBody io.Reader) ([]byte, error) {
	req, err := http.NewRequest(method, db.assembleURL(apiOP, p), reqBody)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := db.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Println(string(responseBody))
		return nil, fmt.Errorf("status code: %d", resp.StatusCode)
	}

	return responseBody, nil
}
