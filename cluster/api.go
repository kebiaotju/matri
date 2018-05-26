package cluster

import (
	"fmt"
	"github.com/qumi/matrix/network"
	"math"
	"sync"
	"time"
	//"github.com/qumi/matrix/log"
	"math/rand"
)

const uidLength = 8
const typeLength = 2

// client_agent保留内存时间
const retainTime = time.Hour * 12

var (
	// servertype -> serverId -> agent
	lock    sync.RWMutex
	clients map[uint16]map[uint16]*ClusterClientAgent
)

func init() {
	clients = make(map[uint16]map[uint16]*ClusterClientAgent)
}

func Init() {
}

func FetchAgent(conn *network.TCPConn, serverType uint16, serverId uint16, client *ClusterTCPClient) network.Agent {
	lock.Lock()
	defer lock.Unlock()
	a := clients[serverType][serverId]
	a.c = client
	a.conn = conn
	return a
}

// use in hall
func RemoveServer(serverType uint16, serverId uint16) {

}

func DialServer(network, addr string, serverType uint16, serverId uint16) (*ClusterClientAgent, error) {

	agent := &ClusterClientAgent{
		HallClientAgents: make(map[uint64]*HallClientAgent),
		serverType:       serverType,
		serverId:         serverId,
		writeChan:        make(chan [][]byte, 250000),
		stopChan:         make(chan int, 1)}
	lock.Lock()
	m, exist := clients[serverType]
	if !exist {
		clients[serverType] = make(map[uint16]*ClusterClientAgent)
		m = clients[serverType]
	}

	m[serverId] = agent
	lock.Unlock()

	client := new(ClusterTCPClient)
	client.Addr = addr
	client.ConnNum = 1
	client.ConnectInterval = 3 * time.Second
	// PendingWriteNum 参数非常重要，它决定了hall能够容忍Game server 临时断线的时间长度，也取决于当前hall的在线玩家负载
	client.PendingWriteNum = 250000
	client.LenMsgLen = 2
	client.MaxMsgLen = math.MaxUint32
	client.FetchAgent = FetchAgent
	client.AutoReconnect = true
	client.LittleEndian = true

	client.serverType = serverType
	client.serverId = serverId

	client.Start()

	return agent, nil
}

func FindClusterClientAgent(serverType uint16, uid uint64) (*ClusterClientAgent, error) {
	lock.Lock()
	defer lock.Unlock()
	m, exist := clients[serverType]
	if !exist {
		return nil, fmt.Errorf("not exist serverType %d", serverType)
	}

	a, exist := m[0]
	if !exist {
		return nil, fmt.Errorf("not exist serverType %d, serverId %d", serverType, 0)
	}

	return a, nil
}

func FindClusterClientAgentStrict(serverType uint16, serverId uint16) (*ClusterClientAgent, error) {
	lock.Lock()
	defer lock.Unlock()
	m, exist := clients[serverType]
	if !exist || m == nil {
		return nil, fmt.Errorf("not exist serverType %d", serverType)
	}
	for _, v := range m {
		if v.serverId == serverId {
			return v, nil
		}
	}
	return nil, fmt.Errorf("not exist serverType %d serverId", serverType, serverId)
}

func FindClusterClientAgentRandom(serverType uint16) (*ClusterClientAgent, error) {
	lock.Lock()
	defer lock.Unlock()
	m, exist := clients[serverType]
	if !exist {
		return nil, fmt.Errorf("not exist serverType %d random", serverType)
	}

	size := len(m)
	if size == 0 {
		return nil, fmt.Errorf("not exist serverType %d random", serverType)
	}

	index := rand.Int() % size
	i := 0
	for _, v := range m {
		if i == index {
			return v, nil
		}
		i++
	}
	return nil, fmt.Errorf("not exist serverType %d random", serverType)
}

func SendToGames(uid uint64, data []byte, except_serverType uint16) {
	for serverType, agent := range clients {
		if serverType == except_serverType {
			continue
		}
		for _, gameAgent := range agent {
			gameAgent.Forward(uid, data)
		}
	}
}
