package cluster

import (
	"encoding/binary"
	"net"
	"reflect"

	"github.com/qumi/matrix/log"
	"github.com/qumi/matrix/network"
	"time"
)

const (
	CREATE = iota
	LOGIN_SUCCESSED
)

type HallClientAgent struct {
	conn network.Conn
	Gate *HallGate

	userData interface{}

	State interface{}

	Uid uint64

	remoteAgents map[uint16]*ClusterClientAgent

	*Selector
}

func (a *HallClientAgent) Run() {
	for {
		// msg_len|msg_type|id|data

		timeoutDuration := a.Gate.TimeOutSeconds
		if timeoutDuration > 0 {
			a.conn.SetReadDeadline(time.Now().Add(timeoutDuration))
		}
		data, err := a.conn.ReadMsg()
		if err != nil {
			log.Debug("HallClientAgent read message: %v", err)
			break
		}

		var t uint16
		if a.Gate.LittleEndian {
			t = binary.LittleEndian.Uint16(data[:typeLength])
		} else {
			t = binary.BigEndian.Uint16(data[:typeLength])
		}

		// msg processed in hall
		if t == 0 {
			if a.Gate.Processor != nil {
				msg, err := a.Gate.Processor.Unmarshal(data[typeLength:])
				if err != nil {
					log.Error("unmarshal message error: %v", err)
					break
				}
				err = a.Gate.Processor.Route(msg, a)
				if err != nil {
					log.Error("route message error: %v", err)
					break
				}
			}
		} else {
			// not forward msg when user not login
			if a.Uid == 0 {
				log.Error("a.Uid == 0 route message error")
				return
			}

			ra, exist := a.remoteAgents[t]
			if exist {
				e := ra.Forward(a.Uid, data[typeLength:])
				if e != nil {
					log.Error("forward return :" + e.Error())
				}
			} else {
				remoteAgent, err := a.Select(t)
				if err != nil {
					//close ?
					log.Error("a.Uid:%v route message error:%v", a.Uid, err)

					return
				}

				a.remoteAgents[t] = remoteAgent
				remoteAgent.l.Lock()
				remoteAgent.HallClientAgents[a.Uid] = a
				remoteAgent.l.Unlock()
				remoteAgent.Forward(a.Uid, data[typeLength:])
			}

		}
	}
}

func (a *HallClientAgent) OnClose() {
	if a.Uid == 0 {
		return
	}
	a.Gate.Manager.RemoveAgent(a)
	a.Gate.AgentCloseCallback(a.Uid, a)
}

func (a *HallClientAgent) WriteMsg(msg interface{}) {
	if a.Gate.Processor != nil {
		data, err := a.Gate.Processor.Marshal(msg)
		if err != nil {
			log.Error("marshal message %v error: %v", reflect.TypeOf(msg), err)
			return
		}
		err = a.conn.WriteMsg(data...)
		if err != nil {
			log.Error("write message %v error: %v", reflect.TypeOf(msg), err)
		}
	}
}

func (a *HallClientAgent) WriteMsgWithType(msgType uint16, msg interface{}) {
	if a.Gate.Processor != nil {
		data, err := a.Gate.Processor.Marshal(msg)
		if err != nil {
			log.Error("marshal message %v error: %v", reflect.TypeOf(msg), err)
			return
		}
		mt := make([]byte, typeLength)
		if a.Gate.LittleEndian {
			binary.LittleEndian.PutUint16(mt, msgType)
		} else {
			binary.BigEndian.PutUint16(mt, msgType)
		}

		d := [][]byte{mt}
		d = append(d, data...)
		err = a.conn.WriteMsg(d...)
		if err != nil {
			log.Error("write message %v  type %d error: %v", reflect.TypeOf(msg), msgType, err)
		}
	}
}

func (a *HallClientAgent) WriteWithType(msgType uint16, bytes []byte) {

	mt := make([]byte, typeLength)
	if a.Gate.LittleEndian {
		binary.LittleEndian.PutUint16(mt, msgType)
	} else {
		binary.BigEndian.PutUint16(mt, msgType)
	}

	d := [][]byte{mt}
	d = append(d, bytes)
	err := a.conn.WriteMsg(d...)
	if err != nil {
		log.Error("write message type %d error: %v", msgType, err)
	}
}

func (a *HallClientAgent) Write(bytes []byte) {
	err := a.conn.WriteMsg(bytes)
	if err != nil {
		log.Error("write message error: %v", err)
	}
}

func (a *HallClientAgent) WriteMsgToAllRemote(bytes []byte) {
	for _, agent := range a.remoteAgents {
		err := agent.Forward(a.Uid, bytes)
		if err != nil {
			log.Error("WriteMsgToAllRemote error: %v", err)
		}
	}
}

func (a *HallClientAgent) LocalAddr() net.Addr {
	return a.conn.LocalAddr()
}

func (a *HallClientAgent) RemoteAddr() net.Addr {
	return a.conn.RemoteAddr()
}

func (a *HallClientAgent) Close() {
	if a.Uid == 0 {
		return
	}
	a.Gate.Manager.Remove(a.Uid)
	a.Gate.AgentCloseCallback(a.Uid, a)
	a.conn.Close()
}

func (a *HallClientAgent) KickClose() {
	if a.Uid == 0 {
		return
	}
	a.conn.Close()
}

func (a *HallClientAgent) Destroy() {
	a.conn.Destroy()
}

func (a *HallClientAgent) UserData() interface{} {
	return a.userData
}

func (a *HallClientAgent) SetUserData(data interface{}) {
	a.userData = data
}

func (a *HallClientAgent) GetClusterClientAgent(svrType uint16) *ClusterClientAgent {
	agent, exist := a.remoteAgents[svrType]
	if !exist {
		return nil
	}
	return agent
}

func (a *HallClientAgent) SetNewRemoteAgent(agent *ClusterClientAgent, serverType uint16) {

	a.remoteAgents[serverType] = agent
	agent.l.Lock()
	agent.HallClientAgents[a.Uid] = a
	agent.l.Unlock()
}
