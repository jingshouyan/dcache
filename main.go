package main

import (
	"dcache/cache"
	"dcache/options"
	"dcache/raftness"
	"time"

	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func main() {
	opts := options.NewOptions()
	r := gin.New()

	r.Use(ginzap.Ginzap(opts.Log, time.RFC3339, true))
	r.Use(ginzap.RecoveryWithZap(opts.Log, true))

	v1 := r.Group("/v1")
	cm := cache.NewCacheManager()
	raft, err := raftness.NewRaftNode(opts, cm)
	if err != nil {
		opts.Log.Panic("new raft node failed", zap.Error(err))
	}
	h := NewHandler(raft)
	h.register(v1)

	r.Run(opts.HttpAddr)
}
