package kvraft

import (
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"go-raft-server/kvdb"
	"go-raft-server/peer"
	"go-raft-server/raft"
	"go-raft-server/util"
)

type bufferedCommands struct {
	cmds []Command
	chs  []chan *CommandReply
}

type KVServer struct {
	mu           sync.RWMutex
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	stateMachine *KVVDB
	notifyChs    map[int][]chan *CommandReply

	buffer         bufferedCommands
	executeTimeout time.Duration
	batchSize      int
	batchTimeout   time.Duration
}

func (kv *KVServer) ExecuteCommand(args *CommandArgs, reply *CommandReply) error {
	if _, ok := kv.rf.GetState(); !ok {
		reply.Err = ErrWrongLeader
		return nil
	}
	ch := make(chan *CommandReply, 1)
	kv.mu.Lock()
	kv.buffer.cmds = append(kv.buffer.cmds, Command{args})
	kv.buffer.chs = append(kv.buffer.chs, ch)

	if len(kv.buffer.cmds) >= kv.batchSize {
		cmds := kv.buffer.cmds
		chs := kv.buffer.chs
		kv.buffer.cmds = kv.buffer.cmds[:0]
		kv.buffer.chs = kv.buffer.chs[:0]
		kv.mu.Unlock()
		go kv.submitBatch(cmds, chs)
	} else {
		kv.mu.Unlock()
	}

	select {
	case result := <-ch:
		reply.Value, reply.Version, reply.Err = result.Value, result.Version, result.Err
	case <-time.After(kv.executeTimeout):
		reply.Err = ErrTimeout
	}
	return nil
}

func (kv *KVServer) submitBatch(cmds []Command, chs []chan *CommandReply) {
	if len(cmds) == 0 {
		return
	}
	index, _, isLeader := kv.rf.Start(cmds)
	if !isLeader {
		for _, ch := range chs {
			ch <- &CommandReply{Err: ErrWrongLeader}
		}
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.notifyChs[index] = chs
}

func (kv *KVServer) applyLogToStateMachine(cmd Command) *CommandReply {
	reply := new(CommandReply)
	switch cmd.Op {
	case OpGet:
		reply.Value, reply.Version, reply.Err = kv.stateMachine.Get(cmd.Key)
	case OpSet:
		reply.Err = kv.stateMachine.Set(cmd.Key, cmd.Value, cmd.Version)
	case OpDelete:
		reply.Err = kv.stateMachine.Delete(cmd.Key, cmd.Version)
	}
	return reply
}

func (kv *KVServer) applier() {
	for message := range kv.applyCh {
		util.DPrintf("{Node %v} tries to apply message %v\n", kv.rf.GetId(), message)
		if message.CommandValid {
			kv.mu.Lock()
			switch cmd := message.Command.(type) {
			case []Command:
				var replies []*CommandReply
				for _, c := range cmd {
					reply := kv.applyLogToStateMachine(c)
					replies = append(replies, reply)
				}

				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					if chs, ok := kv.notifyChs[message.CommandIndex]; ok {
						for i, ch := range chs {
							ch <- replies[i]
						}
					}
				}
				delete(kv.notifyChs, message.CommandIndex)
			default:
				panic(fmt.Sprintf("unknown cmd type: %T\n", cmd))
			}
			kv.mu.Unlock()
		} else {
			panic(fmt.Sprintf("Invalid ApplyMsg %v\n", message))
		}
	}
}

func StartKVServer(servers []peer.Peer, me int, logdb *kvdb.KVDB, kvvdb *KVVDB, executeTimeout, batchSize, batchTimeout int) *KVServer {
	gob.Register([]Command{})
	applyCh := make(chan raft.ApplyMsg)

	kv := &KVServer{
		mu:             sync.RWMutex{},
		rf:             raft.Make(servers, me, logdb, applyCh),
		applyCh:        applyCh,
		stateMachine:   kvvdb,
		notifyChs:      make(map[int][]chan *CommandReply),
		buffer:         bufferedCommands{},
		executeTimeout: time.Duration(executeTimeout) * time.Millisecond,
		batchSize:      batchSize,
		batchTimeout:   time.Duration(batchTimeout) * time.Millisecond,
	}

	go kv.applier()

	go kv.periodicBatchSubmit()

	// register kv *kvraft.KVServer service
	if err := util.RegisterRPCService(kv); err != nil {
		panic(fmt.Sprintf("error when register KVServer rpc service: %v\n", err))
	}

	return kv
}

func (kv *KVServer) periodicBatchSubmit() {
	for {
		time.Sleep(kv.batchTimeout)
		kv.mu.Lock()
		if len(kv.buffer.cmds) > 0 {
			cmds := kv.buffer.cmds
			chs := kv.buffer.chs
			kv.buffer.cmds = kv.buffer.cmds[:0]
			kv.buffer.chs = kv.buffer.chs[:0]
			kv.mu.Unlock()
			go kv.submitBatch(cmds, chs)
		} else {
			kv.mu.Unlock()
		}
	}
}
