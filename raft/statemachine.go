package raft

type StateMachine interface {
	Apply(command []byte) ([]byte, error)
}
