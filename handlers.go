package main

import (
	"net/http"
	"raft-go/raftness"

	"github.com/gin-gonic/gin"
)

func NewHandler(raft *raftness.RaftNode) *handler {
	h := &handler{
		raft: raft,
	}
	return h
}

type handler struct {
	raft *raftness.RaftNode
}

func (h *handler) register(r gin.IRouter) {
	r.GET("/value", h.get)
	r.POST("/value", h.put)
	r.GET("join", h.join)
}

func (h *handler) get(c *gin.Context) {
	key := c.Query("key")
	value, _ := h.raft.Get(key)
	c.String(http.StatusOK, value)
}

func (h *handler) put(c *gin.Context) {
	key := c.Query("key")
	value := c.Query("value")

	err := h.raft.Put(key, value)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	} else {
		c.String(http.StatusOK, "ok")
	}
}

func (h *handler) join(c *gin.Context) {
	peerAddress := c.Query("peerAddress")
	nodeName := c.Query("node")
	if peerAddress == "" {
		c.String(http.StatusBadRequest, "invaild peerAdress")
	} else if nodeName == "" {
		c.String(http.StatusBadRequest, "invaild node")
	} else {
		err := h.raft.Join(nodeName, peerAddress)
		if err != nil {
			c.String(http.StatusInternalServerError, "join failed")
		} else {
			c.String(http.StatusOK, "ok")
		}
	}
}
