package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go-raft-client/kvraft"
	"go-raft-client/peer"
	"go-raft-client/util"
)

func main() {

	// 加载.env文件环境变量
	envFiles := []string{".env"}
	env, err := util.LoadEnv(envFiles)
	if err != nil {
		panic(err)
	}

	// 加载节点配置信息
	peers, err := peer.LoadPeers(env.PeersPath)
	if err != nil {
		log.Fatalln(err)
	}

	// 启动节点 Clerk 服务
	ck := kvraft.MakeClerk(peers)

	// 启动命令行程序
	fmt.Println("1. get <key>                         - 查询键值")
	fmt.Println("2. set <key> <value> <version>       - 设置键值")
	fmt.Println("2. delete <key> <version>            - 删除键值")
	fmt.Println("3. exit                              - 退出程序")
	fmt.Println("4. test get <key>                    - 测试 TPS")
	fmt.Println("4. test set <key> <value> <version>  - 测试 TPS")

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}
		if input == "exit" {
			fmt.Println("退出程序...")
			return
		}
		parts := strings.Fields(input)
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "get":
			if len(parts) != 2 {
				fmt.Println("参数错误，使用方式: get <key>")
				continue
			}
			key := parts[1]
			fmt.Println("等待 get 命令执行")
			reply := ck.Get([]byte(key))
			fmt.Printf("查询结果: %v\n", reply)

		case "set":
			if len(parts) != 4 {
				fmt.Println("参数错误，使用方式: set <key> <value> <version>")
				continue
			}
			key := parts[1]
			value := parts[2]
			version, err := strconv.Atoi(parts[3])
			if err != nil || version < 1 {
				fmt.Println("参数错误：version 字段为大于 0 的数字")
			}
			fmt.Println("等待 set 命令执行 ")
			reply := ck.Set([]byte(key), []byte(value), version)
			fmt.Printf("执行结果: %v\n", reply)

		case "delete":
			if len(parts) != 3 {
				fmt.Println("参数错误，使用方式: delete <key> <version>")
				continue
			}
			key := parts[1]
			version, err := strconv.Atoi(parts[2])
			if err != nil || version < 1 {
				fmt.Println("参数错误：version 字段为大于 0 的数字")
			}
			fmt.Println("等待 put 命令执行")
			reply := ck.Delete([]byte(key), version)
			fmt.Printf("执行结果: %v\n", reply)

		case "test":
			if len(parts) != 3 {
				fmt.Println("参数错误：参数错误，使用方式: test get <key>")
				continue
			}
			key := parts[2]
			goroutinenums := 100
			wg := new(sync.WaitGroup)
			tBegin := time.Now()
			for i := range goroutinenums {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := range 100 {
						fmt.Printf("用户 {%v} 等待第 %v 次 get 命令执行\n", i, j)
						reply := ck.Get([]byte(key))
						fmt.Printf("用户 {%v} 查询结果: %v\n", i, reply)
					}
				}()
			}
			wg.Wait()
			tEnd := time.Now()
			fmt.Printf("TPS: %v\n", 100*100/tEnd.Sub(tBegin).Seconds())

		default:
			fmt.Println("未知命令，支持命令格式如下:")
			fmt.Println("1. get <key>                         - 查询键值")
			fmt.Println("2. set <key> <value> <version>       - 设置键值")
			fmt.Println("2. delete <key> <version>            - 删除键值")
			fmt.Println("3. exit                              - 退出程序")
			fmt.Println("4. test get <key>                    - 测试 TPS")
			fmt.Println("4. test set <key> <value> <version>  - 测试 TPS")
		}
	}
}
