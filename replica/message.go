package replica

type Message interface {
	//Type() uint8 // TODO: we don't need Type()
	Content() interface{}
	Replica() uint8
	Instance() uint64
}
