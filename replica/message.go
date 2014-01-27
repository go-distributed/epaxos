package replica

type Message interface {
	Type() uint8 // TODO: we don't need Type()
	Content() interface{}
	// TODO: ReplicaId() uint8
	// TODO: InstanceId() uint64
}
