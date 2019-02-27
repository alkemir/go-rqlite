package rqlite

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DB represents a connection to a rqlite cluster
type DB struct {
	client     http.Client
	wantsHTTPS bool
	user       string
	password   string

	cluster             cluster
	shouldUpdateCluster bool
	clusterMu           sync.Mutex

	level consistencyLevel
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
	}
	db.cluster = cluster{peers: []*peer{db.newPeer(leaderHost, leaderPort)}}
	db.client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		db.clusterMu.Lock()
		db.shouldUpdateCluster = true
		db.clusterMu.Unlock()
		return nil
	}

	if err := db.UpdateClusterInfo(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) assembleURL(p *peer, apiOp apiOperation, opAtomic bool) string {
	var sb strings.Builder

	sb.WriteString(p.URL)

	switch apiOp {
	case opSTATUS:
		sb.WriteString("/status")
	case opQUERY:
		sb.WriteString("/db/query")
		sb.WriteString("?timings&transaction&level=")
		sb.WriteString(string(db.level))
	case opEXECUTE:
		sb.WriteString("/db/execute")
		sb.WriteString("?timings&transaction&level=")
		sb.WriteString(string(db.level))
		if opAtomic {
			sb.WriteString("&atomic")
		}
	default:
		panic(fmt.Sprintf("unknown apiOperation %d", apiOp))
	}

	return sb.String()
}

func (db *DB) request(apiOP apiOperation, opAtomic bool, method string, p *peer, reqBody io.Reader) ([]byte, error) {
	req, err := http.NewRequest(method, db.assembleURL(p, apiOP, opAtomic), reqBody)
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
		return nil, fmt.Errorf("status code: %d", resp.StatusCode)
	}

	clusterChanged := false
	db.clusterMu.Lock()
	if db.shouldUpdateCluster {
		clusterChanged = true
		db.shouldUpdateCluster = false
	}
	db.clusterMu.Unlock()
	if clusterChanged {
		db.UpdateClusterInfo()
	}

	return responseBody, nil
}
