package replica

type Transporter interface {
	Send(to uint8, msg Message)
	MulticastFastquorum(msg Message)
	Broadcast(msg Message)
}
