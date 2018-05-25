package cluster

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/qumi/matrix/network"

	"github.com/qumi/matrix/log"
)

type ClusterGate struct {
	MaxConnNum int
	MaxMsgLen  uint32
	Processor  network.Processor

	PendingWriteNum int

	// tcp
	TCPAddr      string
	LenMsgLen    int
	LittleEndian bool

	l      sync.RWMutex
	agents map[uint64]*ClientAgent
	In     chan DisMsg

	Manager *AgentManager
	ticker  *time.Ticker
}

type DisMsg struct {
	uid uint64
	msg interface{}
	a   *ClusterServerAgent
}

type HallMsg struct {
	uid uint64
	msg interface{}
}

func (cg *ClusterGate) Run(closeSig chan bool) {
	cg.agents = make(map[uint64]*ClientAgent)
	cg.In = make(chan DisMsg, 2000)

	var tcpServer *network.TCPServer
	if cg.TCPAddr != "" {
		tcpServer = new(network.TCPServer)
		tcpServer.Addr = cg.TCPAddr
		tcpServer.LenMsgLen = cg.LenMsgLen
		tcpServer.MaxMsgLen = cg.MaxMsgLen
		tcpServer.LittleEndian = cg.LittleEndian
		tcpServer.PendingWriteNum = cg.PendingWriteNum
		tcpServer.MaxConnNum = cg.MaxConnNum
		tcpServer.NewAgent = func(conn *network.TCPConn) network.Agent {
			a := &ClusterServerAgent{conn: conn, cg: cg}

			return a
		}
	}

	if tcpServer != nil {
		tcpServer.Start()
	}
	cg.Start()
	<-closeSig

	if tcpServer != nil {
		tcpServer.Close()
	}
}

func (cg *ClusterGate) checkUnActiveClientAgent() {
	cg.ticker = time.NewTicker(time.Minute)
	go func() {
		for t := range cg.ticker.C {
			_ = t
			cg.l.Lock()
			log.Debug("cg agents size %d", len(cg.agents))
			for uid, agent := range cg.agents {
				if time.Now().Sub(agent.lastAccessTime) > retainTime {
					cg.DeleteClientAgent(uid)
				}
			}

			cg.l.Unlock()

		}
	}()
}

func (cg *ClusterGate) Start() {
	cg.checkUnActiveClientAgent()
	go func() {
		for {
			select {
			case m := <-cg.In:
				cg.DispatchToClient(m)
			}
		}
	}()
}

func (cg *ClusterGate) DispatchToClient(m DisMsg) {
	if m.uid == 0 {
		return
	}

	cg.l.Lock()

	ca, ok := cg.agents[m.uid]
	if !ok {
		ca = NewClientAgent(m.uid, cg, m.a)
		ca.Start()
		cg.agents[m.uid] = ca
	} else {
		if ca.a != m.a {
			ca.a = m.a
		}
	}
	cg.l.Unlock()
	ca.In <- m.msg
}

func (cg *ClusterGate) GetUIDData(data []byte) (uint64, []byte) {
	if len(data) < uidLength {
		return 0, data
	}

	var uid uint64
	if cg.LittleEndian {
		uid = binary.LittleEndian.Uint64(data)
	} else {
		uid = binary.BigEndian.Uint64(data)
	}

	d := make([]byte, len(data)-uidLength)
	copy(d[0:], data[uidLength:])
	return uid, d

}

func (cg *ClusterGate) RouteToClient(uid uint64, msg interface{}, a *ClusterServerAgent) {
	m := DisMsg{uid: uid, msg: msg, a: a}
	cg.In <- m
}

func (cg *ClusterGate) DeleteClientAgent(uid uint64) {

	ca, ok := cg.agents[uid]
	if !ok {
		return
	}
	ca.exitSig <- true
	delete(cg.agents, uid)
}

func (cg *ClusterGate) OnDestroy() {
	close(cg.In)
	cg.ticker.Stop()
}
