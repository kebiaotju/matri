package cluster

import (
	"fmt"
)

type StrictSelectMode struct {
	ServerType uint16
	ServerIds  map[uint16]bool
}

func (m *StrictSelectMode) Select() (*ClusterClientAgent, error) {
	for id := range m.ServerIds {
		return FindClusterClientAgentStrict(m.ServerType, id)
	}
	return nil, fmt.Errorf("StrictSelectMode: empty ServerIds")
}

func NewStrictSelectMode(serverType uint16, serverIds ...uint16) SelectMode {
	m := new(StrictSelectMode)
	m.ServerType = serverType
	m.ServerIds = make(map[uint16]bool)
	for _, id := range serverIds {
		m.ServerIds[id] = true
	}
	return m
}
