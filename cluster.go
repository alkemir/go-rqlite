package rqlite

import (
	"errors"
	"strings"
	"time"
)

type Cluster struct {
	leader     Peer
	otherPeers []Peer
}

type Peer struct {
	hostname string
	port     string
}

// TODO(br): Cache this result to avoid allocating each time
// TODO(br): See whether using pointers would be safe
func (c *Cluster) PeerList() []Peer {
	pp := make([]Peer, 0, len(c.otherPeers)+1)
	pp = append(pp, c.leader)
	pp = append(pp, c.otherPeers...)
	return pp
}

func (db *DB) updateClusterInfo() error {
	status, err := db.Status()
	if err != nil {
		return err
	}

	leaderRaftAddr := status.Store.Leader
	nc := Cluster{}

	for raftAddr, httpAddr := range status.Store.Meta.APIPeers {
		var p Peer
		parts := strings.Split(httpAddr, ":")
		p.hostname = parts[0]
		p.port = parts[1]

		if leaderRaftAddr == raftAddr {
			nc.leader = p
		} else {
			nc.otherPeers = append(nc.otherPeers, p)
		}
	}

	if nc.leader.hostname == "" {
		return errors.New("could not determine leader from API status call")
	}

	db.cluster = nc
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
