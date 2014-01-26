package replica

type Message interface {
	Type() uint8
}
