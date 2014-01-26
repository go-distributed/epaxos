package data

import (
	"bytes"
)

type Command []byte

func (cmd1 Command) Compare(cmd2 Command) int {
	return bytes.Compare(cmd1, cmd2)
}
