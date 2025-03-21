package main

import (
	"bufio"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"go-raft-server/kvdb"
	"go-raft-server/kvraft"
	"go-raft-server/peer"
	"go-raft-server/raft"
	"go-raft-server/util"
)

func main() {

	// 加载 .env 文件环境变量
	envFiles := []string{".env"}
	env, err := util.LoadEnv(envFiles)
	if err != nil {
		panic(err)
	}
	envMe := env.Me
	util.Debug = env.Debug
	peersPath := env.PeersPath
	persistentConfigPath := env.PersistentConfigPath

	// 加载节点配置信息
	peers, err := peer.LoadPeers(peersPath)
	if err != nil {
		panic(err)
	}

	// 加载系统配置信息
	persistentConfig, err := util.LoadPersistentConfig(persistentConfigPath)
	if err != nil {
		panic(err)
	}
	raft.ElectionTimeout = persistentConfig.ElectionTimeout
	raft.HeartbeatTimeout = persistentConfig.HeartbeatTimeout

	// 使用 -me flag 重置环境变量 me
	flagMe := flag.Int("me", envMe, "peer id")
	flag.Parse()
	me := *flagMe

	// 启动节点 Raft logdb 数据库
	logdb, err := kvdb.MakeKVDB(fmt.Sprintf("data/logdb%v", me))
	if err != nil {
		panic(err)
	}

	// 创建节点 Raft ApplyMsg 通道
	applyCh := make(chan raft.ApplyMsg)

	// 注册 Command transferred
	gob.Register([]*kvraft.CommandArgs{})

	// 启动节点 Raft
	service := raft.Make(peers, me, logdb, applyCh)

	// 启动 rpc 服务
	if _, err = util.StartRPCServer(fmt.Sprintf(":%v", peers[me].Port)); err != nil {
		panic(fmt.Sprintf("error when start rpc service: %v\n", err))
	}

	// 打印节点启动日志
	log.Printf("peer Raft service started, lisening addr: %v:%v\n", peers[me].Ip, peers[me].Port)

	// 启动命令行程序
	// 1. 创建通道
	inputCh := make(chan string)

	// 2. 启动协程读取输入
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() { // 循环读取每一行
			input := scanner.Text()
			if input == "" {
				continue
			}
			if input == "exit" { // 输入 exit 时退出
				inputCh <- input
				return
			}

			if input == "test command" { // 输入 test command 测试共识命令切片时的 TPS
				blockOfCommands := make([]*kvraft.CommandArgs, 100)
				for index := range blockOfCommands {
					blockOfCommands[index] = &kvraft.CommandArgs{Key: []byte("testKey"), Value: []byte("testValue"), Version: 0, Op: kvraft.OpGet}
				}
				clients := 10
				requestNums := 100
				wg := new(sync.WaitGroup)
				tBegin := time.Now()
				for i := range clients {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for j := range requestNums {
							// 调用 raft 服务
							service.Start(blockOfCommands)
							msg := <-applyCh
							log.Printf("client {%v} received num {%v} Raft ApplyMsg(%v)\n", i, j, msg)
						}
					}()
				}
				wg.Wait()
				tEnd := time.Now()
				fmt.Printf("TPS: %v\n", (float64(clients*len(blockOfCommands)*requestNums))/(tEnd.Sub(tBegin).Seconds()))
				continue
			}

			if input == "test string" { // 输入 test string 测试共识字符串时的 TPS
				blockOfString := make([]string, 100)
				for index := range blockOfString {
					blockOfString[index] = "testString"
				}
				clients := 10
				requestNums := 100
				wg := new(sync.WaitGroup)
				tBegin := time.Now()
				for i := range clients {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for j := range requestNums {
							// 调用 raft 服务
							service.Start(blockOfString)
							msg := <-applyCh
							log.Printf("client {%v} received num {%v} Raft ApplyMsg(%v)\n", i, j, msg)
						}
					}()
				}
				wg.Wait()
				tEnd := time.Now()
				fmt.Printf("TPS: %v\n", (float64(clients*len(blockOfString)*requestNums))/(tEnd.Sub(tBegin).Seconds()))
				continue
			}

			// 调用 raft 服务
			service.Start(input)
			msg := <-applyCh
			log.Printf("client received Raft ApplyMsg(%v)\n", msg)
		}
	}()

	// 3. 主线程处理输入
	for input := range inputCh {
		if input == "exit" {
			fmt.Println("exit...")
			return
		}
	}
}

// package main

// import (
// 	"flag"
// 	"fmt"
// 	"log"

// 	"go-raft-server/kvdb"
// 	"go-raft-server/kvraft"
// 	"go-raft-server/peer"
// 	"go-raft-server/util"
// )

// func main() {

// 	// 加载 .env 文件环境变量
// 	envFiles := []string{".env"}
// 	env, err := util.LoadEnv(envFiles)
// 	if err != nil {
// 		log.Fatalln(err)
// 	}
// 	envMe := env.Me
// 	util.Debug = env.Debug
// 	peersInfoFilePath := env.PeersInfoFilePath

// 	// 加载节点配置信息
// 	peers, err := peer.LoadPeers(peersInfoFilePath)
// 	if err != nil {
// 		log.Fatalln(err)
// 	}

// 	// 使用 -me flag 重置环境变量 me
// 	flagMe := flag.Int("me", envMe, "节点 id")
// 	flag.Parse()
// 	me := *flagMe

// 	// 加载持久存储 服务
// 	logdb, err := kvdb.MakeKVDB(fmt.Sprintf("data/logdb%v", me))
// 	if err != nil {
// 		log.Fatalln(err)
// 	}
// 	kvvdb, err := kvraft.MakeKVVDB(fmt.Sprintf("data/kvvdb%v", me))
// 	if err != nil {
// 		log.Fatalln(err)
// 	}

// 	kvraft.StartKVServer(peers, me, logdb, kvvdb)

// 	// 启动 rpc 服务
// 	if _, err = util.StartRPCServer(fmt.Sprintf("%v:%v", peers[me].Ip, peers[me].Port)); err != nil {
// 		log.Fatalln(err)
// 	}
// 	log.Printf("节点 {%v}, 启动 Raft 和 KVRaft 服务，监听地址：%v:%v\n", me, peers[me].Ip, peers[me].Port)
// 	select {}
// }
