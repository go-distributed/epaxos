package epaxos

// A transporter provides only simple primitives, including
// Send and Recv.
type Transporter interface {
	// Send an encoded message to the host:port.
	// This will block. We need the msgType here because
	// we assume the message is not self-explained.
	Send(hostport string, msgType uint8, b []byte) error

	// Receive an encoded message from some peer.
	// Return the type of the message and the content.
	// We need the msgType here because we assume the
	// message is not self-explained.
	Recv() (msgType uint8, b []byte, err error)

	// Start the transporter, this will block if succeeds,
	// or return an error if it fails.
	Start() error

	// Stop the transporter.
	Stop() error
}
