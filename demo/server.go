package main

import (
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/go-distributed/epaxos/message"
	"github.com/go-distributed/epaxos/replica"
	"github.com/go-distributed/epaxos/transporter"
	"github.com/golang/glog"
)

var _ = fmt.Printf

const (
	chars = "ABCDEFG"
)

type Voter struct {
}

// NOTE: This is not idempotent.
//      Same command might be executed for multiple times
func (v *Voter) Execute(c []message.Command) ([]interface{}, error) {
	fmt.Println(string(c[0]))
	return nil, nil
}

func (v *Voter) HaveConflicts(c1 []message.Command, c2 []message.Command) bool {
	return true
}

func main() {
	var id int

	flag.IntVar(&id, "id", -1, "id of the server")
	flag.Parse()

	if id < 0 {
		fmt.Println("id is required!")
		flag.PrintDefaults()
		return
	}

	addrs := []string{
		":9000", ":9001", ":9002",
		//":9003", ":9004",
	}

	tr, err := transporter.NewUDPTransporter(addrs, uint8(id), len(addrs))
	if err != nil {
		panic(err)
	}
	param := &replica.Param{
		Addrs:        addrs,
		ReplicaId:    uint8(id),
		Size:         uint8(len(addrs)),
		StateMachine: new(Voter),
		Transporter:  tr,
	}

	r, err := replica.New(param)
	if err != nil {
		glog.Fatal(err)
	}

	err = r.Start()
	if err != nil {
		glog.Fatal(err)
	}

	rand.Seed(time.Now().UTC().UnixNano())
	for {
		time.Sleep(time.Millisecond * 500)
		c := "From: " + strconv.Itoa(id) + ", Command: " + string(chars[rand.Intn(len(chars))]) + ", " + time.Now().String()

		cmds := make([]message.Command, 0)
		cmds = append(cmds, message.Command(c))
		r.Propose(cmds...)
	}
}
