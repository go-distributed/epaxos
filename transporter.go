package epaxos

// A transporter provides only simple primitives, including
// Send and Recv.
type Transporter interface {
	// Send an encoded message to the host:port.
	// This will block. We don't need the msgType here
	// because we assume the message is self-explained.
	Send(hostport string, b []byte) error

	// Receive an encoded message from some peer.
	// Return the bytes form of the message.
	Recv() (b []byte, err error)

	// Start the transporter, this will block if succeeds,
	// or return an error if it fails.
	Start() error

	// Stop the transporter.
	Stop() error

	// Destroy the transporter.
	Destroy() error
}
