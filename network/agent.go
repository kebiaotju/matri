package network

type Agent interface {
	Run()
	OnClose()
	WriteMsg(data interface{})
}
