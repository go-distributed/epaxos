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
	for i := range cmds {
		cmds[i] = make(Command, len(c[i]))
		copy(cmds[i], c[i])
	}
	return cmds
}

func (c Command) Clone() Command {
	cmd := make(Command, len(c))
	copy(cmd, c)
	return cmd
}
