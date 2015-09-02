package transporter

import (
	"github.com/sargun/epaxos/message"
)

type DummyTransporter struct {
	Chs        []chan message.Message
	Self       uint8
	FastQuorum uint8
	All        uint8
}

func NewDummyTR(self uint8, size int) *DummyTransporter {
	dm := &DummyTransporter{
		Chs:        make([]chan message.Message, size),
		Self:       self,
		FastQuorum: uint8(size) - 2,
		All:        uint8(size),
	}
	return dm
}

// non-block
func (tr *DummyTransporter) Send(to uint8, msg message.Message) {
	go func() {
		tr.Chs[to] <- msg
	}()
}

func (tr *DummyTransporter) MulticastFastquorum(msg message.Message) {
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

func (tr *DummyTransporter) Broadcast(msg message.Message) {
	for i := 0; i < int(tr.All); i++ {
		if i == int(tr.Self) {
			continue
		}
		tr.Send(uint8(i), msg)
	}
}

func (tr *DummyTransporter) RegisterChannel(ch chan message.Message) {}
func (tr *DummyTransporter) Start() error                            { return nil }
func (tr *DummyTransporter) Stop()                                   {}

// only for dummyTR
func (tr *DummyTransporter) RegisterChannels(chs []chan message.Message) {
	for i := range chs {
		tr.Chs[i] = chs[i]
	}
}
