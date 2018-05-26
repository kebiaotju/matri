package cluster

import "time"

type ClientAgent struct {
	uid     uint64
	cg      *ClusterGate
	a       *ClusterServerAgent
	In      chan interface{}
	exitSig chan bool

	lastAccessTime time.Time
}

func NewClientAgent(id uint64, d *ClusterGate, a *ClusterServerAgent) *ClientAgent {
	ca := new(ClientAgent)
	ca.uid = id
	ca.cg = d
	ca.a = a
	ca.In = make(chan interface{}, 100)
	ca.exitSig = make(chan bool)
	return ca
}

func (ca *ClientAgent) GetUID() uint64 {
	return ca.uid
}

func (ca *ClientAgent) GetNetAgent() *ClusterServerAgent {
	return ca.a
}

func (ca *ClientAgent) Start() {
	go func() {
		for {
			select {
			case message := <-ca.In:
				// TODO need optimization
				ca.lastAccessTime = time.Now()
				ca.HandleMsg(message)
			case <-ca.exitSig:
				ca.Destroy()
				return
			}
		}
	}()
}

func (ca *ClientAgent) Destroy() {
	close(ca.exitSig)
	close(ca.In)
}

func (ca *ClientAgent) HandleMsg(msg interface{}) {
	ca.cg.Processor.Route(msg, ca)
}

func (ca *ClientAgent) WriteMsg(msg interface{}) {
	hm := HallMsg{uid: ca.uid, msg: msg}
	ca.a.WriteMsg(hm)
}
