package livetest

import (
	"github.com/go-distributed/epaxos/replica"
)

type DummyTransporter struct {
	Nodes      []*replica.Replica
	Self       uint8
	FastQuorum uint8
	All        uint8
}

// non-block
func (tr *DummyTransporter) Send(to uint8, msg replica.Message) {
	go func() {
		r := tr.Nodes[to]
		r.MessageEventChan <- msg
	}()
}

func (tr *DummyTransporter) MulticastFastquorum(msg replica.Message) {
	skip := 0
	for i := 0; i < int(tr.FastQuorum); i++ {
		rid := uint8(i + skip)
		if rid == tr.Self {
			rid++
			skip = 1
		}

		tr.Send(rid, msg)
	}
}

func (tr *DummyTransporter) Broadcast(msg replica.Message) {
	for i := 0; i < int(tr.All); i++ {
		if i == int(tr.Self) {
			continue
		}
		tr.Send(uint8(i), msg)
	}
}

func (tr *DummyTransporter) RegisterChannel(ch chan *replica.MessageEvent) {
}

func (tr *DummyTransporter) Start() {
}

func (tr *DummyTransporter) Stop() {
}
