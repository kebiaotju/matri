package cluster

type RandomSelectMode struct {
	ServerType uint16
}

func (m *RandomSelectMode) Select() (*ClusterClientAgent, error) {
	return FindClusterClientAgentRandom(m.ServerType)
}

func NewRandomSelectMode(serverType uint16) SelectMode {
	m := new(RandomSelectMode)
	m.ServerType = serverType
	return m
}
