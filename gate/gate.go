package gate

import (
	"github.com/qumi/matrix/chanrpc"
	"github.com/qumi/matrix/log"
	"github.com/qumi/matrix/network"
	"net"
	"reflect"

	"encoding/binary"
	"fmt"
)

const TypeLength = 2

type Gate struct {
	MaxConnNum      int
	PendingWriteNum int
	MaxMsgLen       uint32
	Processor       network.Processor
	AgentChanRPC    *chanrpc.Server

	ServerType uint16

	// tcp
	TCPAddr      string
	LenMsgLen    int
	LittleEndian bool
}

func (gate *Gate) Run(closeSig chan bool) {

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
			a := &agent{conn: conn, gate: gate}
			if gate.AgentChanRPC != nil {
				gate.AgentChanRPC.Call0("NewAgent", a)
			}
			return a
		}
	}

	if tcpServer != nil {
		tcpServer.Start()
	}
	<-closeSig

	if tcpServer != nil {
		tcpServer.Close()
	}
}

func (gate *Gate) OnDestroy() {}

func (gate *Gate) GetServerType(data []byte) (uint16, error) {
	if len(data) < int(TypeLength) {
		return 0, fmt.Errorf("error! data len:%d < type length%d", len(data), TypeLength)
	}

	var t uint16
	if gate.LittleEndian {
		t = binary.LittleEndian.Uint16(data[:TypeLength])
	} else {
		t = binary.BigEndian.Uint16(data[:TypeLength])
	}

	return t, nil
}

type agent struct {
	conn     network.Conn
	gate     *Gate
	userData interface{}
}

func (a *agent) Run() {
	for {
		data, err := a.conn.ReadMsg()
		if err != nil {
			log.Debug("read message: %v", err)
			break
		}

		st, err := a.gate.GetServerType(data)
		if err != nil {
			log.Debug("read message: %v", err)
			break
		}

		if st != a.gate.ServerType {
			log.Error("server type:%d error! not process", st)
			break
		}

		if a.gate.Processor != nil {
			msg, err := a.gate.Processor.Unmarshal(data[TypeLength:])
			if err != nil {
				log.Debug("unmarshal message error: %v", err)
				break
			}
			err = a.gate.Processor.Route(msg, a)
			if err != nil {
				log.Debug("route message error: %v", err)
				break
			}
		}
	}
}

func (a *agent) OnClose() {
	if a.gate.AgentChanRPC != nil {
		err := a.gate.AgentChanRPC.Call0("CloseAgent", a)
		if err != nil {
			log.Error("chanrpc error: %v", err)
		}
	}
}

func (a *agent) WriteMsg(msg interface{}) {
	if a.gate.Processor != nil {
		data, err := a.gate.Processor.Marshal(msg)
		if err != nil {
			log.Error("marshal message %v error: %v", reflect.TypeOf(msg), err)
			return
		}

		mt := make([]byte, TypeLength)
		if a.gate.ServerType != 0 {
			if a.gate.LittleEndian {
				binary.LittleEndian.PutUint16(mt, a.gate.ServerType)
			} else {
				binary.BigEndian.PutUint16(mt, a.gate.ServerType)
			}
		}

		d := [][]byte{mt}
		d = append(d, data...)

		err = a.conn.WriteMsg(d...)
		if err != nil {
			log.Error("write message %v error: %v", reflect.TypeOf(msg), err)
		}
	}
}

func (a *agent) LocalAddr() net.Addr {
	return a.conn.LocalAddr()
}

func (a *agent) RemoteAddr() net.Addr {
	return a.conn.RemoteAddr()
}

func (a *agent) Close() {
	a.conn.Close()
}

func (a *agent) Destroy() {
	a.conn.Destroy()
}

func (a *agent) UserData() interface{} {
	return a.userData
}

func (a *agent) SetUserData(data interface{}) {
	a.userData = data
}
