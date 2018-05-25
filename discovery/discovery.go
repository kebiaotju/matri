package discovery

type DataPair struct {
	Data     string
	Metadata string
}

type Handler interface {
	Handle(pairs []*DataPair)
}

var _ Handler = HandlerFunc(nil)

type HandlerFunc func(pairs []*DataPair)

func (f HandlerFunc) Handle(pairs []*DataPair) {
	f(pairs)
}

type Discovery interface {
	Register(basePath string, data string, metadata string) (err error)
	UnRegister(basePath string, data string)
	Watch(basePath string, handler HandlerFunc)
}
