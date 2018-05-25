package cluster_test

import (
	"github.com/qumi/matrix/cluster"
	"testing"
)

func Test(t *testing.T) {
	const serverType = 1
	var randomS *cluster.Selector
	var strictS *cluster.Selector
	randomS = cluster.NewSelector()
	randomS.Set(serverType, cluster.NewRandomSelectMode(serverType))
	strictS = cluster.NewSelector()
	strictS.Set(serverType, cluster.NewStrictSelectMode(serverType, 1, 2, 3))

	testCases := []struct {
		desc string
		s    *cluster.Selector
		typ  uint16
	}{
		{desc: "", s: nil, typ: serverType},
		{desc: "", s: randomS, typ: serverType},
		{desc: "", s: strictS, typ: serverType},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			if _, err := tC.s.Select(tC.typ); err != nil {
				t.Errorf("%v: err[%v]", tC, err)
			}
		})
	}
}
