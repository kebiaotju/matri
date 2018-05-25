package cluster

import (
	"github.com/qumi/matrix/log"
	"github.com/qumi/matrix/network"

	"encoding/binary"
	"net"
	"reflect"
)

func Destroy() {
}

func (a *ClusterServerAgent) Destroy() {
}

type ClusterServerAgent struct {
	conn *network.TCPConn
	cg   *ClusterGate
}

func newClusterServerAgent(conn *network.TCPConn) network.Agent {
	a := new(ClusterServerAgent)
	a.conn = conn
	return a
}

func (a *ClusterServerAgent) WriteMsg(msg interface{}) {
	var ha_msg = msg.(HallMsg)
	if a.cg.Processor != nil {
		data, error := a.cg.Processor.Marshal(ha_msg.msg)

		if error != nil {
			log.Error("marshal message %v error: %v", reflect.TypeOf(msg), error)
			return
		}
		uid := make([]byte, uidLength)
		if a.cg.LittleEndian {
			binary.LittleEndian.PutUint64(uid, ha_msg.uid)
		} else {
			binary.BigEndian.PutUint64(uid, ha_msg.uid)
		}

		d := [][]byte{uid}
		d = append(d, data...)
		err := a.conn.WriteMsg(d...)
		if err != nil {
			log.Error("write message %v error: %v", reflect.TypeOf(msg), err)
		}
	}
}

func (a *ClusterServerAgent) Run() {
	for {
		data, err := a.conn.ReadMsg()
		if err != nil {
			log.Error("read message error:%v", err)
			break
		}
		uid, d := a.cg.GetUIDData(data)

		if a.cg.Processor != nil {
			msg, err := a.cg.Processor.Unmarshal(d)
			if err != nil {
				log.Error("unmarshal msg error, error:%v", err)
				continue
			}

			a.cg.RouteToClient(uid, msg, a)

		}
	}
}

func (a *ClusterServerAgent) LocalAddr() net.Addr {
	return a.conn.LocalAddr()
}

func (a *ClusterServerAgent) RemoteAddr() net.Addr {
	return a.conn.RemoteAddr()
}

func (a *ClusterServerAgent) Close() {
	a.conn.Close()
}

func (a *ClusterServerAgent) OnClose() {

}
