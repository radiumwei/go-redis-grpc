package main

import (
	"flag"
	"fmt"
	"os"

	"go-redis-grpc/cmd"
	"go-redis-grpc/server"
)

const (
	optionNamePort    = "listen"
	optionNameHost    = "redisaddr"
	optionNameSaveLog = "savelog"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: go-redis-grpc --redisaddr=[HOST:PORT] --listen=[PORT]\n")
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	var port uint
	var host string
	flag.UintVar(&port, optionNamePort, 50051, "--listen=[PORT] 'Listen on port'")
	flag.StringVar(&host, optionNameHost, "localhost:6379", "--redisaddr=[HOST:PORT] 'Target redis host to proxy from'")
	saveLog := flag.Bool(optionNameSaveLog, false, "save log to file")

	flag.Usage = usage
	flag.Parse()

	if !(port < 65536) {
		fmt.Println("Invalid port", port)
		os.Exit(1)
	}

	cmd.InitLog(*saveLog)
	server.StartServer(host, port)
}
