package raftness

import (
	"raft-go/cache"

	"github.com/hashicorp/raft"
)

func newSnapshot(cm *cache.CacheManager) *snapshot {
	return &snapshot{cm: cm}
}

var _ raft.FSMSnapshot = &snapshot{}

type snapshot struct {
	cm *cache.CacheManager
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	bytes, err := s.cm.Marshal()
	if err != nil {
		sink.Cancel()
		return err
	}
	if _, err = sink.Write(bytes); err != nil {
		sink.Cancel()
		return err
	}
	if err = sink.Close(); err != nil {
		sink.Cancel()
		return err
	}
	return nil
}

func (s *snapshot) Release() {}
