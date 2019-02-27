package rqlite

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

type cluster struct {
	peers []*peer // leader first
}

func (db *DB) PeerList() []*peer {
	db.clusterMu.Lock()
	pp := db.cluster.peers
	db.clusterMu.Unlock()
	return pp
}

func (db *DB) updateClusterInfo() error {
	status, err := db.Status()
	if err != nil {
		return err
	}

	leaderRaftAddr := status.Store.Leader

	var leader *peer
	var otherPeers []*peer

	for raftAddr, httpAddr := range status.Store.Meta.APIPeers {
		hostPort := strings.Split(httpAddr, ":")
		p := db.newPeer(hostPort[0], hostPort[1])

		if leaderRaftAddr == raftAddr {
			leader = p
		} else {
			otherPeers = append(otherPeers, p)
		}
	}

	if leader == nil {
		return ErrLeaderNotFound
	}

	pp := make([]*peer, 0, len(otherPeers)+1)
	pp = append(pp, leader)
	pp = append(pp, otherPeers...)

	db.clusterMu.Lock()
	db.cluster = cluster{peers: pp}
	db.clusterMu.Unlock()
	return nil
}

type ClusterStatus struct {
	Runtime    RuntimeStatus
	HTTP       HTTPStatus
	Node       NodeStatus
	Store      StoreStatus
	LastBackup time.Time `json:"last_backup_time"`
	Build      BuildStatus
}

type RuntimeStatus struct {
	GOARCH       string
	GOOS         string
	GOMAXPROCS   int
	NumCPU       int
	NumGoroutine int
	Version      string
}

type HTTPStatus struct {
	Addr            string
	Auth            string
	Redirect        string
	ConnIdleTimeout string `json:"conn_idle_timeout"`
	ConnTxTimeout   string `json:"conn_tx_timeout"`
}

type BuildStatus struct {
	Branch    string
	Commit    string
	Version   string
	BuildTime string `json:"build_time"` // TODO(br): Provide this as time.Time
}

type NodeStatus struct {
	StartTime time.Time `json:"start_time"`
	Uptime    string    // TODO(br): Provide this as time.Duration
}

type StoreStatus struct {
	Addr              string
	ApplyTimeout      string   `json:"apply_timeout"`
	DB                DBStatus `json:"db_conf"`
	Dir               string
	HeartBeatTimeout  string `json:"heartbeat_timeout"`
	Leader            string
	Meta              MetaStatus
	OpenTimeout       string `json:"open_timeout"`
	Peers             []string
	Raft              RaftStatus
	SnapshotThreshold int `json:"snapshot_threshold"`
	Sqlite3           Sqlite3Status
}

type DBStatus struct {
	DSN    string
	Memory bool
}

type MetaStatus struct {
	APIPeers map[string]string
}

type RaftStatus struct {
	AppliedIndex      string `json:"applied_index"`
	CommitIndex       string `json:"commit_index"`
	FsmPending        string `json:"fsm_pending"`
	LastContact       string `json:"last_contact"`
	LastLogIndex      string `json:"last_log_index"`
	LastLogTerm       string `json:"last_log_term"`
	LastSnapshotIndex string `json:"last_snapshot_index"`
	LastSnapshotTerm  string `json:"last_snapshot_term"`
	NumPeers          string `json:"num_peers"`
	State             string
	Term              string
}

type Sqlite3Status struct {
	DNS           string
	FKConstraints string
	Path          string
	Version       string
}

func (db *DB) Status() (*ClusterStatus, error) {
	pp := db.PeerList()
	if len(pp) < 1 {
		return nil, ErrNoPeers
	}

	for _, p := range pp {
		resp, err := db.request(opSTATUS, false, http.MethodGet, p, nil)
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
