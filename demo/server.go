package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/go-distributed/epaxos/data"
	"github.com/go-distributed/epaxos/replica"
)

var _ = fmt.Printf

const (
	chars = "ABCDEFG"
)

type Voter struct {
}

// NOTE: This is not idempotent.
//      Same command might be executed for multiple times
func (v *Voter) Execute(c []data.Command) ([]interface{}, error) {
	fmt.Println(string(c[0]))
	return nil, nil
}

func (v *Voter) HaveConflicts(c1 []data.Command, c2 []data.Command) bool {
	return true
}

func main() {
	addrs := []string{
		":9000", ":9001", ":9002",
		//":9003", ":9004",
	}

	if len(os.Args) < 2 {
		log.Fatal("Usage: ./server [id]")
	}

	id, _ := strconv.Atoi(os.Args[1])

	param := &replica.Param{
		Addrs:        addrs,
		ReplicaId:    uint8(id),
		Size:         uint8(len(addrs)),
		StateMachine: new(Voter),
	}

	r, err := replica.New(param)
	if err != nil {
		log.Fatal(err)
	}

	err = r.Start()
	if err != nil {
		log.Fatal(err)
	}

	rand.Seed(time.Now().UTC().UnixNano())
	for {
		time.Sleep(time.Millisecond * 500)
		c := "From: " + os.Args[1] + ", Command: " + string(chars[rand.Intn(len(chars))]) + ", " + time.Now().String()

		cmds := make([]data.Command, 0)
		cmds = append(cmds, data.Command(c))
		r.BatchPropose(cmds)
	}
}
