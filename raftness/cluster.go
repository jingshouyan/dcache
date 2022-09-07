package raftness

import (
	"dcache/cache"
	"dcache/options"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"go.uber.org/zap"
)

type RaftNode struct {
	raft           *raft.Raft
	fsm            *fsm
	transport      *raft.NetworkTransport
	leaderNotifyCh chan bool
	leader         bool
	opts           *options.Options
	*cache.CacheManager
}

func (r *RaftNode) Join(nodeName, peerAddress string) error {
	af := r.raft.AddVoter(raft.ServerID(nodeName), raft.ServerAddress(peerAddress), 0, 0)
	err := af.Error()
	if err != nil {
		r.opts.Log.Error("raft add voter failed", zap.Error(err))
	}
	return err
}

func (r *RaftNode) Put(key, value string) error {
	event := eventData{Key: key, Value: value}
	eb, err := json.Marshal(event)
	if err != nil {
		r.opts.Log.Error("json marshal error", zap.Error(err))
		return err
	}
	af := r.raft.Apply(eb, 5*time.Second)
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
	raftConfig.LocalID = raft.ServerID(opts.RaftNode)
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
	if opts.JoinAddr != "" {
		JoinRaftCluster(opts)
	}
	raft := &RaftNode{
		raft:           rn,
		fsm:            fsm,
		transport:      transport,
		leaderNotifyCh: leaderNotifyCh,
		opts:           opts,
		CacheManager:   cm,
	}
	go raft.monitor()
	return raft, nil
}

func (r *RaftNode) monitor() {
	for leader := range r.leaderNotifyCh {
		r.leader = leader
		r.opts.Log.Info("leader change", zap.Bool("leader", r.leader))
	}
}

func JoinRaftCluster(opts *options.Options) error {
	url := fmt.Sprintf("http://%s/v1/join?peerAddress=%s&node=%s", opts.JoinAddr, opts.RaftAddr, opts.RaftNode)
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
