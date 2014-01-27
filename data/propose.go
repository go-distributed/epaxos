package data

type Propose struct {
	Cmds Commands
}

func (p *Propose) Type() uint8 {
	return ProposeMsg
}
func (p *Propose) Content() interface{} {
	return p
}
