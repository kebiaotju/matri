package discovery

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/etcd"
	"github.com/qumi/matrix/log"
	"sync"
)

func init() {
	etcd.Register()
}

type EtcdDiscovery struct {
	etcdServers []string

	// data->metadata
	dataLock sync.RWMutex
	dataMap  map[string]string

	interval time.Duration

	options *store.Config
	kv      store.Store
}

func NewDiscoveryUseEtcd(etcdAddr []string, interval time.Duration, options *store.Config) Discovery {

	d := &EtcdDiscovery{etcdServers: etcdAddr, options: options, interval: interval, dataMap: make(map[string]string)}

	return d
}

func (d *EtcdDiscovery) Watch(basePath string, handler HandlerFunc) {

	if d.kv == nil {
		kv, err := libkv.NewStore(store.ETCD, d.etcdServers, d.options)
		if err != nil {
			log.Error("cannot create etcd registry: %v", err)
			return
		}
		d.kv = kv
	}

	c, err := d.kv.WatchTree(basePath, nil)
	if err != nil {
		log.Error("can not watchtree: %s: %v", basePath, err)
		return
	}

	for {
		for ps := range c {
			var pairs []*DataPair // latest DATA
			prefix := "/" + basePath + "/"
			for _, p := range ps {
				k := strings.TrimPrefix(p.Key, prefix)
				pairs = append(pairs, &DataPair{Data: k, Metadata: string(p.Value)})
			}

			handler(pairs)
		}

		log.Error(" watchtree exit: %s: %d", basePath, len(c))

		c, err = d.kv.WatchTree(basePath, nil)
		if err != nil {
			log.Error("can not watchtree: %s: %v", basePath, err)
			return
		}
	}

}

func (d *EtcdDiscovery) UnRegister(basePath string, data string) {
	if "" == strings.TrimSpace(data) {
		return
	}

	if d.kv == nil {
		return
	}

	d.dataLock.Lock()
	delete(d.dataMap, data)
	d.dataLock.Unlock()

	nodePath := fmt.Sprintf("%s/%s", basePath, data)
	err := d.kv.Delete(nodePath)
	if err != nil {
		log.Error("cannot delete etcd path %s: %v", nodePath, err)
		return
	}
}

// not goroutine safe
func (d *EtcdDiscovery) Register(basePath string, data string, metadata string) error {

	if "" == strings.TrimSpace(data) {
		err := errors.New("Register service `name` can't be empty")
		return err
	}

	if d.kv == nil {
		kv, err := libkv.NewStore(store.ETCD, d.etcdServers, d.options)
		if err != nil {
			log.Error("cannot create etcd registry: %v", err)
			return err
		}
		d.kv = kv

		if d.interval > 0 {

			ticker := time.NewTicker(d.interval)
			go func() {
				defer d.kv.Close()

				// refresh service TTL
				for range ticker.C {
					d.dataLock.Lock()
					t := make(map[string]string)
					for k, v := range d.dataMap {
						t[k] = v
					}
					d.dataLock.Unlock()

					for name, metadata := range t {
						nodePath := fmt.Sprintf("%s/%s", basePath, name)
						kvPair, err := d.kv.Get(nodePath)

						if err != nil {
							log.Release("can't get data of node: %s, because of %v", nodePath, err.Error())
							err = d.kv.Put(nodePath, []byte(metadata), &store.WriteOptions{TTL: d.interval * 2})

							if err != nil {
								log.Error("cannot re-create etcd path %s: %v", nodePath, err)
							}

						} else {
							d.kv.Put(nodePath, kvPair.Value, &store.WriteOptions{TTL: d.interval * 2})
						}
					}

				}
			}()
		}
	}

	if b, _ := d.kv.Exists(basePath); !b {
		err := d.kv.Put(basePath, []byte("ed_path"), &store.WriteOptions{IsDir: true})
		if err != nil && !strings.Contains(err.Error(), "Not a file") {
			log.Error("cannot create etcd path %s: %v", basePath, err)
			return err
		}
	}

	nodePath := fmt.Sprintf("%s/%s", basePath, data)
	err := d.kv.Put(nodePath, []byte(metadata), &store.WriteOptions{TTL: d.interval * 2})
	if err != nil {
		log.Error("cannot create etcd path %s: %v", nodePath, err)
		return err
	}

	d.dataMap[data] = metadata

	return nil
}
