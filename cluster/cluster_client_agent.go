package cluster

import (
	"encoding/binary"
	"errors"
	"net"
	"sync"

	"github.com/qumi/matrix/log"
	"github.com/qumi/matrix/network"
)

type ClusterClientAgent struct {
	conn network.Conn

	sendMutex sync.RWMutex
	writeChan chan [][]byte
	stopChan  chan int

	c *ClusterTCPClient

	userData interface{}

	l                sync.RWMutex
	HallClientAgents map[uint64]*HallClientAgent

	serverType uint16
	serverId   uint16
}

func (a *ClusterClientAgent) GetServerId() uint16 {
	return a.serverId
}

func (a *ClusterClientAgent) SetClusterTCPClient(client *ClusterTCPClient) {
	a.c = client
}

func (a *ClusterClientAgent) Forward(uid uint64, bytes []byte) error {

	u := make([]byte, uidLength)
	if a.c == nil {
		return errors.New("ClusterClientAgent Forward nil cluster tcp client")
	}
	if a.c.LittleEndian {
		binary.LittleEndian.PutUint64(u, uid)
	} else {
		binary.BigEndian.PutUint64(u, uid)
	}

	d := [][]byte{u}
	d = append(d, bytes)

	select {
	case a.writeChan <- d:
		return nil
	default:
		return errors.New("ClusterClientAgent Forward writeChan full")
	}
}

func (a *ClusterClientAgent) sendLoop() {
	for {
		select {
		case msg, ok := <-a.writeChan:
			if !ok || a.conn.WriteMsg(msg...) != nil {
				log.Error("sendLoop write message error")
				return
			}
		case <-a.stopChan:
			return
		}
	}
}

func (a *ClusterClientAgent) Run() {
	go a.sendLoop()

	for {
		// len|uid|id|data
		data, err := a.conn.ReadMsg()

		if err != nil {
			a.stopChan <- 0
			log.Error("ClusterClientAgent read message: %v", err)
			break
		}

		var uid uint64
		if a.c.LittleEndian {
			uid = binary.LittleEndian.Uint64(data)
		} else {
			uid = binary.BigEndian.Uint64(data)
		}

		a.l.RLock()
		hca, exist := a.HallClientAgents[uid]
		a.l.RUnlock()
		if exist {
			hca.WriteWithType(a.serverType, data[uidLength:])
		} else {
			log.Error("HallClientAgent not found for uid %d", uid)
		}
	}
}

func (a *ClusterClientAgent) OnClose() {
	log.Error("ClusterClientAgent serverType:%v serverId:%v OnClose",a.serverType,a.serverId)
	for _,agent := range a.HallClientAgents{
		agent.Gate.OnClusterClientAgentClose(agent.Uid,a.serverType,a.serverId)
	}
}

func (a *ClusterClientAgent) WriteMsg(msg interface{}) {

}

func (a *ClusterClientAgent) LocalAddr() net.Addr {
	return a.conn.LocalAddr()
}

func (a *ClusterClientAgent) RemoteAddr() net.Addr {
	return a.conn.RemoteAddr()
}

func (a *ClusterClientAgent) Close() {
	a.conn.Close()
	// TODO CLOSE ALL
}

func (a *ClusterClientAgent) Destroy() {
	a.conn.Destroy()
}

func (a *ClusterClientAgent) UserData() interface{} {
	return a.userData
}

func (a *ClusterClientAgent) SetUserData(data interface{}) {
	a.userData = data
}
