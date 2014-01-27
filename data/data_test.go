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

	assert.Equal(t, p.Type(), preAcceptType)
	assert.Equal(t, pr.Type(), preAcceptReplyType)
	assert.Equal(t, po.Type(), preAcceptOKType)
	assert.Equal(t, a.Type(), acceptType)
	assert.Equal(t, ar.Type(), acceptReplyType)
	assert.Equal(t, c.Type(), commitType)
	assert.Equal(t, pp.Type(), prepareType)
	assert.Equal(t, ppr.Type(), prepareReplyType)
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

	assert.Equal(t, p.Content(), p)
	assert.Equal(t, pr.Content(), pr)
	assert.Equal(t, po.Content(), po)
	assert.Equal(t, a.Content(), a)
	assert.Equal(t, ar.Content(), ar)
	assert.Equal(t, c.Content(), c)
	assert.Equal(t, pp.Content(), pp)
	assert.Equal(t, ppr.Content(), ppr)
}
