// package main

// import (
// 	"bufio"
// 	"encoding/gob"
// 	"flag"
// 	"fmt"
// 	"log"
// 	"os"

// 	"go-raft-server/kvdb"
// 	"go-raft-server/kvraft"
// 	"go-raft-server/peer"
// 	"go-raft-server/raft"
// 	"go-raft-server/util"
// )

// func main() {
// 	// 加载.env文件环境变量
// 	env, err := util.LoadEnv()
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

// 	// 启动节点 Raft 服务
// 	logdb, err := kvdb.MakeKVDB(fmt.Sprintf("data/logdb%v", me))
// 	if err != nil {
// 		log.Fatalln(err)
// 	}
// 	applyCh := make(chan raft.ApplyMsg)
// 	go func() {
// 		for msg := range applyCh {
// 			fmt.Printf("收到 Raft ApplyMsg 回复：%v\n", msg)
// 		}
// 	}()
// 	gob.Register([]kvraft.Command{})
// 	service := raft.Make(peers, me, logdb, applyCh)

// 	// 启动 rpc 服务
// 	if _, err = util.StartRPCServer(fmt.Sprintf(":%v", peers[me].Port)); err != nil {
// 		log.Fatalf("启动节点 rpc 服务出错：%v\n", err)
// 	}
// 	log.Printf("节点 Raft 服务启动，监听地址：%v:%v\n", peers[me].Ip, peers[me].Port)

// 	// 启动命令行程序
// 	// 1. 创建通道
// 	inputCh := make(chan string)

// 	// 2. 启动协程读取输入
// 	go func() {
// 		scanner := bufio.NewScanner(os.Stdin)
// 		for scanner.Scan() { // 循环读取每一行
// 			input := scanner.Text()
// 			if input == "" {
// 				continue
// 			}
// 			if input == "exit" { // 输入 exit 时退出
// 				inputCh <- input
// 				return
// 			}

// 			if input == "test command" { // 输入 test command 时测试 tps
// 				blockOfCommands := make([]kvraft.Command, 100)
// 				for index := range blockOfCommands {
// 					blockOfCommands[index].CommandArgs = &kvraft.CommandArgs{Key: []byte("testKey"), Value: []byte("testValue"), Op: kvraft.OpGet}
// 				}
// 				for {
// 					// 调用 raft 服务
// 					fmt.Println(service.Start(blockOfCommands))
// 				}
// 			}
// 			if input == "test string" { // 输入 test string 时测试 tps
// 				blockOfInputs := make([]int, 100)
// 				for {
// 					// 调用 raft 服务
// 					fmt.Println(service.Start(blockOfInputs))
// 				}
// 			}
// 			// 调用 raft 服务
// 			fmt.Println(service.Start(input))
// 		}
// 	}()

// 	// 3. 主线程处理输入
// 	for input := range inputCh {
// 		if input == "exit" {
// 			fmt.Println("退出程序...")
// 			return
// 		}
// 	}
// }

package main

import (
	"flag"
	"fmt"
	"log"

	"go-raft-server/kvdb"
	"go-raft-server/kvraft"
	"go-raft-server/peer"
	"go-raft-server/util"
)

func main() {

	// 加载.env文件环境变量
	env, err := util.LoadEnv()
	if err != nil {
		log.Fatalln(err)
	}
	envMe := env.Me
	util.Debug = env.Debug
	peersInfoFilePath := env.PeersInfoFilePath

	// 加载节点配置信息
	peers, err := peer.LoadPeers(peersInfoFilePath)
	if err != nil {
		log.Fatalln(err)
	}

	// 使用 -me flag 重置环境变量 me
	flagMe := flag.Int("me", envMe, "节点 id")
	flag.Parse()
	me := *flagMe

	// 加载持久存储 服务
	logdb, err := kvdb.MakeKVDB(fmt.Sprintf("data/logdb%v", me))
	if err != nil {
		log.Fatalln(err)
	}
	kvvdb, err := kvraft.MakeKVVDB(fmt.Sprintf("data/kvvdb%v", me))
	if err != nil {
		log.Fatalln(err)
	}

	kvraft.StartKVServer(peers, me, logdb, kvvdb)

	// 启动 rpc 服务
	if _, err = util.StartRPCServer(fmt.Sprintf("%v:%v", peers[me].Ip, peers[me].Port)); err != nil {
		log.Fatalln(err)
	}
	log.Printf("节点 {%v}, 启动 Raft 和 KVRaft 服务，监听地址：%v:%v\n", me, peers[me].Ip, peers[me].Port)
	select {}
}
