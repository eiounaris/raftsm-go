package kvraft

import (
	"encoding/gob"
	"log"
	"sync"
	"time"

	"go-raft-server/kvdb"
	"go-raft-server/peer"
	"go-raft-server/raft"
	"go-raft-server/util"
)

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	stateMachine *KVVDB
	notifyChs    map[int]chan *CommandReply
}

func (kv *KVServer) ExecuteCommand(args *CommandArgs, reply *CommandReply) error {
	index, _, isLeader := kv.rf.Start(Command{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return nil
	}
	kv.mu.Lock()
	ch := kv.getNotifyCh(index)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		reply.Value, reply.Version, reply.Err = result.Value, result.Version, result.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}
	go func() {
		kv.mu.Lock()
		kv.deleteNotifyCh(index)
		kv.mu.Unlock()
	}()
	return nil
}

func (kv *KVServer) getNotifyCh(index int) chan *CommandReply {
	if _, ok := kv.notifyChs[index]; !ok {
		kv.notifyChs[index] = make(chan *CommandReply, 1)
	}
	return kv.notifyChs[index]
}

func (kv *KVServer) deleteNotifyCh(index int) {
	delete(kv.notifyChs, index)
}

func (kv *KVServer) applyLogToStateMachine(command Command) *CommandReply {
	reply := new(CommandReply)
	switch command.Op {
	case OpGet:
		reply.Value, reply.Version, reply.Err = kv.stateMachine.Get(command.Key)
	case OpSet:
		reply.Err = kv.stateMachine.Set(command.Key, command.Value, command.Version)
	case OpDelete:
		reply.Err = kv.stateMachine.Delete(command.Key, command.Version)
	}
	return reply
}

func (kv *KVServer) applier() {
	for message := range kv.applyCh {
		log.Printf("{Node %v} tries to apply message %v\n", kv.rf.GetId(), message)
		if message.CommandValid {
			kv.mu.Lock()
			reply := new(CommandReply)
			command := message.Command.(Command) // type assertion
			reply = kv.applyLogToStateMachine(command)
			// just notify related channel for currentTerm's log when node is leader
			if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
				ch := kv.getNotifyCh(message.CommandIndex)
				ch <- reply
			}
			kv.mu.Unlock()
		} else {
			log.Fatalf("Invalid ApplyMsg %v", message)
		}
	}
}

func StartKVServer(servers []peer.Peer, me int, logdb *kvdb.KVDB, kvvdb *KVVDB) *KVServer {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Command{})
	applyCh := make(chan raft.ApplyMsg)

	kv := &KVServer{
		mu:           sync.RWMutex{},
		me:           me,
		rf:           raft.Make(servers, me, logdb, applyCh),
		applyCh:      applyCh,
		stateMachine: kvvdb,
		notifyChs:    make(map[int]chan *CommandReply),
	}

	go kv.applier()

	err := util.RegisterRPCService(kv)
	if err != nil {
		log.Fatalf("注册节点 KVRaft rpc 服务出错： %v\n", err)
	}
	return kv
}
