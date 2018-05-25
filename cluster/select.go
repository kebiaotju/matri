package cluster

type SelectMode interface {
	Select() (*ClusterClientAgent, error)
}

type Selector struct {
	modes map[uint16]SelectMode
}

func NewSelector() *Selector {
	s := new(Selector)
	s.modes = make(map[uint16]SelectMode)
	return s
}

func (s *Selector) Get(serverType uint16) SelectMode {
	if s == nil {
		return nil
	}
	return s.modes[serverType]
}

func (s *Selector) Set(serverType uint16, mode SelectMode) {
	if s == nil {
		return
	}
	s.modes[serverType] = mode
}

func (s *Selector) Select(serverType uint16) (*ClusterClientAgent, error) {
	mode := s.Get(serverType)
	if mode == nil {
		return FindClusterClientAgentRandom(serverType)
	}
	return mode.Select()
}
