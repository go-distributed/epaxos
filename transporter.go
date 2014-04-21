package epaxos

import (
	"github.com/go-distributed/epaxos/message"
)

type Transporter interface {
	// non-blocking send
	Send(to uint8, msg message.Message)

	// non-blocking multicast
	MulticastFastquorum(msg message.Message)

	// non-blocking broadcast
	Broadcast(msg message.Message)

	// register a channel to communicate with replica
	RegisterChannel(ch chan message.Message)

	// start the transporter, it's non-blocking
	Start() error

	// stop the transporter
	Stop()
}
