package raftness

import (
	"dcache/cache"
	"encoding/json"
	"io"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

func newFSM(cm *cache.CacheManager, log *zap.Logger) *fsm {
	return &fsm{
		cm:  cm,
		log: log,
	}
}

var _ raft.FSM = &fsm{}

type fsm struct {
	cm  *cache.CacheManager
	log *zap.Logger
}

type eventData struct {
	Key   string
	Value string
}

func (f *fsm) Apply(logEntry *raft.Log) interface{} {
	d := eventData{}
	if err := json.Unmarshal(logEntry.Data, &d); err != nil {
		f.log.Error("failed unmarshal log", zap.ByteString("data", logEntry.Data))
	}
	err := f.cm.Put(d.Key, d.Value)
	return err
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return newSnapshot(f.cm), nil
}

func (f *fsm) Restore(snapshot io.ReadCloser) error {
	return f.cm.Unmarshal(snapshot)
}
