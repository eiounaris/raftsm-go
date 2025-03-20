package util

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// === Debugging

var Debug = false

func DPrintf(format string, a ...any) {
	if Debug {
		log.Printf(format, a...)
	}
}

// === Env

type Env struct {
	Me                int
	Debug             bool
	PeersInfoFilePath string
}

func LoadEnv(envFiles []string) (*Env, error) {
	err := godotenv.Load(envFiles...)
	if err != nil {
		return nil, err
	}

	meString, ok := os.LookupEnv("me")
	if !ok {
		return nil, errors.New("there is no env of \"me\"")
	}
	me, err := strconv.Atoi(meString)
	if err != nil {
		return nil, err
	}

	debugString, ok := os.LookupEnv("debug")
	if !ok {
		return nil, errors.New("there is no env of \"debug\"")
	}
	debug, err := strconv.ParseBool(debugString)
	if err != nil {
		return nil, err
	}

	peersInfoFilePath, ok := os.LookupEnv("peersInfoFilePath")
	if !ok {
		return nil, errors.New("there is no env of \"peersInfoFilePath\"")
	}
	return &Env{Me: me, Debug: debug, PeersInfoFilePath: peersInfoFilePath}, nil
}

// === RegisterRPCService

func RegisterRPCService(service any) error {
	err := rpc.Register(service)
	if err != nil {
		return err
	}
	return nil
}

// === StartRPCServer

func StartRPCServer(address string) (net.Listener, error) {
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	go http.Serve(listener, nil)

	return listener, nil
}
