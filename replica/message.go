package replica

type Message interface {
	Type() uint8
	Content() interface{}
}
