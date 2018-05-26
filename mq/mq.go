package mq

//Producer is interface for message sender
type Producer interface {
	Init(string) error
	Destroy()
	Push(string, string, []byte) error
}

//Consumer is interface for message receiver
type Consumer interface {
}
