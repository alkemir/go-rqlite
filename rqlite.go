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
	opQUERY apiOperation = iota
	opSTATUS
	opEXECUTE
)

var ErrNoPeers = errors.New("no peers on cluster")
var ErrPeersUnavailable = errors.New("no peers available")
var ErrLeaderNotFound = errors.New("could not determine leader from API status call")
