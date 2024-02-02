// mrworker.go
package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one master.
//
// go run mrworker.go ../mrapps/wc.so
//

import (
	"fmt"
	"log"
	"os"
	"plugin"

	"github.com/Januslll/mit-go/src/mr"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker ../mrapps/xxx.so\n")
		os.Exit(1)
	}

	// 加载map和reduce方法
	mapf, reducef := loadPlugin(os.Args[1])

	// 调用worker.go的worker方法
	// 入参为map和reduce方法
	mr.Worker(mapf, reducef)
}

//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
