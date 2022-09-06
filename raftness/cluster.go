package raftness

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"raft-go/cache"
	"raft-go/options"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"go.uber.org/zap"
)

type RaftNode struct {
	Raft           *raft.Raft
	fsm            *fsm
	transport      *raft.NetworkTransport
	leaderNotifyCh chan bool
	opts           *options.Options
	*cache.CacheManager
}

func (r *RaftNode) Put(key, value string) error {
	event := EventData{Key: key, Value: value}
	eb, err := json.Marshal(event)
	if err != nil {
		r.opts.Log.Error("json marshal error", zap.Error(err))
		return err
	}
	af := r.Raft.Apply(eb, 5*time.Second)
	if err = af.Error(); err != nil {
		r.opts.Log.Error("raft apply error", zap.Error(err))
	}
	return err
}

func newRaftTransport(opts *options.Options) (*raft.NetworkTransport, error) {
	address, err := net.ResolveTCPAddr("tcp", opts.RaftAddr)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(opts.RaftAddr, address, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}
	return transport, nil
}

func NewRaftNode(opts *options.Options, cm *cache.CacheManager) (*RaftNode, error) {
	logOpts := &hclog.LoggerOptions{
		Name: "raft",
	}
	logger := hclog.FromStandardLogger(zap.NewStdLog(opts.Log), logOpts)
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(opts.RaftAddr)
	raftConfig.SnapshotInterval = opts.SnapshotInterval
	raftConfig.SnapshotThreshold = opts.SnapshotThreshold
	raftConfig.Logger = logger
	leaderNotifyCh := make(chan bool, 1)
	raftConfig.NotifyCh = leaderNotifyCh

	transport, err := newRaftTransport(opts)
	if err != nil {
		return nil, err
	}
	fsm := newFSM(cm, opts.Log)

	snapshotStore, err := raft.NewFileSnapshotStoreWithLogger(opts.DataDir, 2, logger)
	if err != nil {
		return nil, err
	}
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(opts.DataDir, "raft-log.bolt"))
	if err != nil {
		return nil, err
	}
	stablesStore, err := raftboltdb.NewBoltStore(filepath.Join(opts.DataDir, "raft-stable.bolt"))
	if err != nil {
		return nil, err
	}
	rn, err := raft.NewRaft(raftConfig, fsm, logStore, stablesStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}
	if opts.Bootsrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		rn.BootstrapCluster(configuration)
	}
	return &RaftNode{
		Raft:           rn,
		fsm:            fsm,
		transport:      transport,
		leaderNotifyCh: leaderNotifyCh,
		opts:           opts,
		CacheManager:   cm,
	}, nil
}

func JoinRaftCluster(opts options.Options) error {
	url := fmt.Sprintf("http://%s/v1/join?peerAddress=%s", opts.JoinAddr, opts.RaftAddr)
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if string(body) != "ok" {
		return fmt.Errorf("error joining cluster: %s", body)
	}
	return nil
}