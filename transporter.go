package epaxos

import (
	"github.com/go-distributed/epaxos/message"
)

type Transporter interface {
	// all the operations below are non-blocking except for Stop()
	Send(to uint8, msg message.Message)
	MulticastFastquorum(msg message.Message)
	Broadcast(msg message.Message)
	RegisterChannel(ch chan *message.MessageEvent)
	Start()
	Stop()
}
