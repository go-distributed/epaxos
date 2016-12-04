package message

import (
	"bytes"
)

type Command []byte

type Commands []Command

func (self Command) Compare(other Command) int {
	return bytes.Compare(self, other)
}

func (c Commands) Clone() Commands {
	if c == nil {
		return nil
	}
	cmds := make(Commands, len(c))
	for i := range c {
		cmds[i] = make(Command, len(c[i]))
		copy(cmds[i], c[i])
	}
	return cmds
}

func (c Commands) ToBytesSlice() [][]byte {
	b := make([][]byte, len(c))
	for i := range c {
		b[i] = c[i].ToBytes()
	}
	return b
}

func (c *Commands) FromBytesSlice(b [][]byte) {
	*c = make([]Command, len(b))
	for i := range b {
		(*c)[i].FromBytes(b[i])
	}
}

func (c Command) Clone() Command {
	cmd := make(Command, len(c))
	copy(cmd, c)
	return cmd
}

func (c Command) ToBytes() []byte {
	b := make([]byte, len(c))
	copy(b, c)
	return b
}
func (c *Command) FromBytes(b []byte) {
	*c = make([]byte, len(b))
	copy(*c, b)
}
