package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/go-distributed/epaxos/data"
	"github.com/go-distributed/epaxos/replica"
	"github.com/go-distributed/epaxos/test"
)

var _ = fmt.Printf

func main() {
	addrs := []string{
		":9000", ":9001", ":9002",
		":9003", ":9004",
	}

	if len(os.Args) < 2 {
		log.Fatal("Usage: ./server [id]")
	}

	id, _ := strconv.Atoi(os.Args[1])

	param := &replica.Param{
		Addrs:        addrs,
		ReplicaId:    uint8(id),
		Size:         uint8(len(addrs)),
		StateMachine: new(test.DummySM),
	}

	r, err := replica.New(param)
	if err != nil {
		log.Fatal(err)
	}

	err = r.Start()
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(time.Millisecond * 100)

	r.Propose(data.Command("a"))
}
