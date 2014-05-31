package epaxos

import (
	"github.com/go-distributed/epaxos/message"
)

// A codec interface that defines what a codec should implement.
// A codec should be able to marshal/unmarshal through the given
// connection.
type Codec interface {
	// Init a codec.
	Initial() error

	// Marshal a message into bytes.
	Marshal(msg message.Message) ([]byte, error)

	// Unmarshal a message from bytes.
	Unmarshal(mtype uint8, data []byte) (message.Message, error)

	// Destroy a codec, release the resource.
	Destroy() error
}
