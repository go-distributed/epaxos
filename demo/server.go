package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/go-distributed/epaxos/message"
	"github.com/go-distributed/epaxos/replica"
	"github.com/go-distributed/epaxos/transporter"
	"github.com/golang/glog"
)

var _ = fmt.Printf

const (
	chars           = "ABCDEFG"
	prepareInterval = 1 // 1 seconds
)

type Voter struct{}

// NOTE: This is not idempotent.
//      Same command might be executed for multiple times
//      but the exection is slow now, so it is unlikely to happen
func (v *Voter) Execute(c []message.Command) ([]interface{}, error) {
	if c == nil || len(c) == 0 {
		fmt.Fprintln(os.Stderr, "No op")
	} else {
		for i := range c {
			fmt.Fprintln(os.Stderr, string(c[i]))
		}
	}
	return nil, nil
}

func (v *Voter) HaveConflicts(c1 []message.Command, c2 []message.Command) bool {
	return true
}

func main() {
	var id int
	var restore bool

	flag.IntVar(&id, "id", -1, "id of the server")
	flag.BoolVar(&restore, "restore", false, "if recover")

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
		Addrs:            addrs,
		ReplicaId:        uint8(id),
		Size:             uint8(len(addrs)),
		StateMachine:     new(Voter),
		Transporter:      tr,
		EnablePersistent: true,
		Restore:          restore,
		TimeoutInterval:  time.Second,
		//ExecuteInterval:  time.Second,
	}

	fmt.Println("====== Spawn new replica ======")
	r, err := replica.New(param)
	if err != nil {
		glog.Fatal(err)
	}

	fmt.Println("Done!")
	fmt.Printf("Wait %d seconds to start\n", prepareInterval)
	time.Sleep(prepareInterval * time.Second)
	err = r.Start()
	if err != nil {
		glog.Fatal(err)
	}
	fmt.Println("====== start ======")

	rand.Seed(time.Now().UTC().UnixNano())
	counter := 1
	for {
		time.Sleep(time.Millisecond * 500)
		c := "From: " + strconv.Itoa(id) + ", Command: " + strconv.Itoa(id) + ":" + strconv.Itoa(counter) + ", " + time.Now().String()
		counter++

		cmds := make([]message.Command, 0)
		cmds = append(cmds, message.Command(c))
		r.Propose(cmds...)
	}
}
