package raft

type StateMachine interface {
	Apply(command []byte) ([]byte, error)
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}
