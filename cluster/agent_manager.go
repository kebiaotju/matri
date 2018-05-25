package cluster

import (
	"sync"
)

type KEY interface{}

type AgentManager struct {
	mutex  sync.RWMutex
	agents map[KEY]interface{}
}

func NewAgentManager() *AgentManager {
	return &AgentManager{
		agents: make(map[KEY]interface{}),
	}
}

func (manager *AgentManager) Clone() map[KEY]interface{} {
	newAgents := make(map[KEY]interface{})

	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	for k, v := range manager.agents {
		newAgents[k] = v
	}

	return newAgents
}

func (manager *AgentManager) Len() int {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()
	return len(manager.agents)
}

func (manager *AgentManager) Get(key KEY) interface{} {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()
	agent, _ := manager.agents[key]
	return agent
}

func (manager *AgentManager) Put(key KEY, agent interface{}) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if _, exists := manager.agents[key]; exists {
		manager.remove(key)
	}

	manager.agents[key] = agent
}

func (manager *AgentManager) remove(key KEY) {
	delete(manager.agents, key)
}

func (manager *AgentManager) Remove(key KEY) bool {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	_, exists := manager.agents[key]
	if exists {
		manager.remove(key)
	}
	return exists
}

func (manager *AgentManager) RemoveAgent(agent interface{})  {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	key := agent.(*HallClientAgent).Uid
	currAgent, exists := manager.agents[key]
	if exists && currAgent ==  agent {
		manager.remove(key)
	}
}

func (manager *AgentManager) Close() {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	for key, _ := range manager.agents {
		manager.remove(key)
	}
}
