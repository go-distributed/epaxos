package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestType(t *testing.T) {
	p := &PreAccept{}
	pr := &PreAcceptReply{}
	po := &PreAcceptOk{}
	a := &Accept{}
	ar := &AcceptReply{}
	c := &Commit{}
	pp := &Prepare{}
	ppr := &PrepareReply{}
	pps := &Propose{}

	assert.Equal(t, p.Type(), PreAcceptMsg)
	assert.Equal(t, pr.Type(), PreAcceptReplyMsg)
	assert.Equal(t, po.Type(), PreAcceptOkMsg)
	assert.Equal(t, a.Type(), AcceptMsg)
	assert.Equal(t, ar.Type(), AcceptReplyMsg)
	assert.Equal(t, c.Type(), CommitMsg)
	assert.Equal(t, pp.Type(), PrepareMsg)
	assert.Equal(t, ppr.Type(), PrepareReplyMsg)
	assert.Equal(t, pps.Type(), ProposeMsg)
}

func TestContent(t *testing.T) {
	p := &PreAccept{}
	pr := &PreAcceptReply{}
	po := &PreAcceptOk{}
	a := &Accept{}
	ar := &AcceptReply{}
	c := &Commit{}
	pp := &Prepare{}
	ppr := &PrepareReply{}
	pps := &Propose{}

	assert.Equal(t, p.Content(), p)
	assert.Equal(t, pr.Content(), pr)
	assert.Equal(t, po.Content(), po)
	assert.Equal(t, a.Content(), a)
	assert.Equal(t, ar.Content(), ar)
	assert.Equal(t, c.Content(), c)
	assert.Equal(t, pp.Content(), pp)
	assert.Equal(t, ppr.Content(), ppr)
	assert.Equal(t, pps.Content(), pps)
}
