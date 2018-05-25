package cluster

import (
	"github.com/qumi/matrix/chanrpc"
	"github.com/qumi/matrix/log"
	"github.com/qumi/matrix/network"
	"time"
)

type HallGate struct {
	MaxConnNum      int
	PendingWriteNum int
	MaxMsgLen       uint32
	Processor       network.Processor
	AgentChanRPC    *chanrpc.Server

	// tcp
	TCPAddr      string
	LenMsgLen    int
	LittleEndian bool

	Manager *AgentManager

	ticker         *time.Ticker
	TimeOutSeconds time.Duration
	//
	HeartbeatHandler  func(uid KEY, agent interface{})
	TickerTimeSeconds time.Duration

	//agent关闭时的回调，清理agent相关数据
	AgentCloseCallback func(uid KEY, agent interface{})

	//用于启动hall的需要依赖gate数据的自定义功能，比如hall监听 rabbitMq，对agent数据进行处理
	CustomHandler func(hg *HallGate)

	OnClusterClientAgentClose func(uid KEY, serverType uint16, server_id uint16)
}

func (gate *HallGate) newHallClientAgent(conn *network.TCPConn) network.Agent {
	a := &HallClientAgent{
		conn:         conn,
		Gate:         gate,
		State:        CREATE,
		remoteAgents: make(map[uint16]*ClusterClientAgent),
		Selector:     NewSelector(),
	}

	return a
}

func (gate *HallGate) HeartbeatAgent() {
	gate.ticker = time.NewTicker(gate.TickerTimeSeconds)
	go func() {
		for t := range gate.ticker.C {
			_ = t
			log.Debug("gate.Manager size %d", gate.Manager.Len())
			for uid, agent := range gate.Manager.Clone() {
				gate.HeartbeatHandler(uid, agent)
			}
		}
	}()
}

func (gate *HallGate) Run(closeSig chan bool) {
	var tcpServer *network.TCPServer
	if gate.TCPAddr != "" {
		tcpServer = new(network.TCPServer)
		tcpServer.Addr = gate.TCPAddr
		tcpServer.MaxConnNum = gate.MaxConnNum
		tcpServer.PendingWriteNum = gate.PendingWriteNum
		tcpServer.LenMsgLen = gate.LenMsgLen
		tcpServer.MaxMsgLen = gate.MaxMsgLen
		tcpServer.LittleEndian = gate.LittleEndian
		tcpServer.NewAgent = func(conn *network.TCPConn) network.Agent {
			return gate.newHallClientAgent(conn)
		}
	}

	if tcpServer != nil {
		tcpServer.Start()
	}

	gate.HeartbeatAgent()

	if gate.CustomHandler != nil {
		gate.CustomHandler(gate)
	}

	<-closeSig

	if tcpServer != nil {
		tcpServer.Close()
	}
}

func (gate *HallGate) OnDestroy() {}
