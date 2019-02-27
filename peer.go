package rqlite

import "strings"

// peer represents a peer in the cluster.
type peer struct {
	URL string
}

func (db *DB) newPeer(hostname, port string) *peer {
	var sb strings.Builder

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

	sb.WriteString(hostname)
	sb.WriteString(":")
	sb.WriteString(port)

	return &peer{URL: sb.String()}
}
