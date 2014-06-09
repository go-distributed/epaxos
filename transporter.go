package epaxos

import (
	"github.com/go-distributed/epaxos/message"
)

type Transporter interface {
	// Send one message, it will block.
	Send(to uint8, msg message.Message)

	// Send one message to multiple receivers, it will block.
	MulticastFastquorum(msg message.Message)

	// Send one message to all receivers, it will block.
	Broadcast(msg message.Message)

	// Register a channel to communicate with replica.
	RegisterChannel(ch chan message.Message)

	// Start the transporter, it will block until success or failure.
	Start() error

	// Stop the transporter.
	Stop()
}
