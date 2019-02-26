package rqlite

import "errors"

type consistencyLevel string

const (
	clNONE   consistencyLevel = "none"
	clWEAK   consistencyLevel = "weak"
	clSTRONG consistencyLevel = "strong"
)

type apiOperation int

const (
	apiQUERY apiOperation = iota
	apiSTATUS
	apiWRITE
)

var ErrNoPeers = errors.New("no peers on cluster")
var ErrPeersUnavailable = errors.New("no peers available")
