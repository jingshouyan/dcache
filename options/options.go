package options

import (
	"time"

	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

type Options struct {
	HttpAddr          string
	RaftAddr          string
	DataDir           string
	Bootsrap          bool
	JoinAddr          string
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64
	Log               *zap.Logger
}

var (
	httpAddr  string
	raftAddr  string
	node      string
	dataDir   string
	bootstrap bool
	joinAddr  string
)

func init() {
	pflag.StringVarP(&httpAddr, "http", "e", "127.0.0.1:6000", "HTTP address")
	pflag.StringVarP(&raftAddr, "raft", "r", "127.0.0.1:7000", "raft tcp address")
	pflag.StringVarP(&node, "node", "n", "node0", "node name")
	pflag.StringVarP(&dataDir, "folder", "f", "", "data folder")
	pflag.BoolVarP(&bootstrap, "bootstrap", "b", false, "start as raft cluster")
	pflag.StringVarP(&joinAddr, "join", "j", "", "join address for raft cluster")
	pflag.Parse()
}

func NewOptions() *Options {
	dir := dataDir
	if dir == "" {
		dir = "./data/" + node
	}
	log, _ := zap.NewProduction()
	opts := &Options{
		HttpAddr:          httpAddr,
		RaftAddr:          raftAddr,
		DataDir:           dir,
		Bootsrap:          bootstrap,
		JoinAddr:          joinAddr,
		SnapshotInterval:  30 * time.Second,
		SnapshotThreshold: 5,
		Log:               log,
	}

	return opts
}
