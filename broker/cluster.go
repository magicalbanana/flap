//  Copyright © 2018 Sunface <CTO@188.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package broker

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"sort"
	"strings"

	"github.com/imdevlab/g/utils"

	"github.com/imdevlab/flap/pkg/config"
	"github.com/imdevlab/g"
	"github.com/weaveworks/mesh"
	"go.uber.org/zap"
)

type cluster struct {
	bk     *Broker
	closed chan struct{}
	peer   *peer
}

func (c *cluster) Init() {
	c.closed = make(chan struct{})
	// get hardware address
	hwaddr, err := utils.HardwareAddr()
	if err != nil {
		g.L.Fatal("get hardware addr error", zap.Error(err))
	}

	// get host name
	nickname, err := utils.Hostname()
	if err != nil {
		g.L.Fatal("get hostname error", zap.Error(err))
	}

	name, err := mesh.PeerNameFromString(hwaddr)
	if err != nil {
		g.L.Fatal("hardware addr invalid", zap.Error(err), zap.String("hardware_addr", hwaddr))
	}

	peers := stringset{}
	for _, peer := range config.Conf.Cluster.SeedPeers {
		peers[peer] = struct{}{}
	}

	router, err := mesh.NewRouter(mesh.Config{
		Host:               "0.0.0.0",
		Port:               config.Conf.Cluster.Port,
		ProtocolMinVersion: mesh.ProtocolMinVersion,
		ConnLimit:          64,
		PeerDiscovery:      true,
		TrustedSubnets:     []*net.IPNet{},
	}, name, nickname, mesh.NullOverlay{}, log.New(ioutil.Discard, "", 0))
	if err != nil {
		g.L.Fatal("Could not create cluster", zap.Error(err))
	}

	peer := newPeer(name, c.bk)
	gossip, err := router.NewGossip("default", peer)
	if err != nil {
		g.L.Fatal("Could not create cluster gossip", zap.Error(err))
	}

	peer.register(gossip)
	c.peer = peer

	func() {
		g.L.Debug("cluster starting", zap.String("hwaddr", hwaddr), zap.Int("port", config.Conf.Cluster.Port))
		router.Start()
	}()

	defer func() {
		g.L.Debug("cluster stopping")
		router.Stop()
	}()

	router.ConnectionMaker.InitiateConnections(peers.slice(), true)

	// loop to get the running time of other nodes
	// go func() {
	// 	submsg := SubMessage{CLUSTER_RUNNING_TIME_REQ, nil, 0, []byte("")}

	// 	syncmsg := make([]byte, 5)
	// 	syncmsg[4] = CLUSTER_SUBS_SYNC_REQ
	// 	c.bk.cluster.peer.longestRunningTime = uint64(c.bk.runningTime.Unix())
	// 	n := 0
	// 	for {
	// 		if n > 3 {
	// 			break
	// 		}
	// 		time.Sleep(5 * time.Second)
	// 		if c.bk.subSynced {
	// 			break
	// 		}
	// 		c.bk.cluster.peer.send.GossipBroadcast(submsg)
	// 		time.Sleep(3 * time.Second)
	// 		// sync the subs from the longest running node
	// 		if c.bk.cluster.peer.longestRunningTime < uint64(c.bk.runningTime.Unix()) {
	// 			c.bk.cluster.peer.send.GossipUnicast(c.bk.cluster.peer.longestRunningName, syncmsg)
	// 			continue
	// 		}
	// 		// 没有节点比本地节点运行时间更久，为了以防万一，我们做4次循环
	// 		n++
	// 	}
	// }()

	select {
	case <-c.closed:

	}
}

func (c *cluster) Close() {
	c.closed <- struct{}{}
}

// Peer encapsulates state and implements mesh.Gossiper.
// It should be passed to mesh.Router.NewGossip,
// and the resulting Gossip registered in turn,
// before calling mesh.Router.Start.
type peer struct {
	name mesh.PeerName
	bk   *Broker
	send mesh.Gossip

	longestRunningName mesh.PeerName
	longestRunningTime uint64
}

// peer implements mesh.Gossiper.
var _ mesh.Gossiper = &peer{}

// Construct a peer with empty state.
// Be sure to register a channel, later,
// so we can make outbound communication.
func newPeer(pn mesh.PeerName, b *Broker) *peer {
	p := &peer{
		name: pn,
		bk:   b,
		send: nil, // must .register() later
	}
	return p
}

// register the result of a mesh.Router.NewGossip.
func (p *peer) register(send mesh.Gossip) {
	p.send = send
}

func (p *peer) stop() {

}

// Return a copy of our complete state.
func (p *peer) Gossip() (complete mesh.GossipData) {
	fmt.Println("send gossip")
	return complete
}

// Merge the gossiped data represented by buf into our state.
// Return the state information that was modified.
func (p *peer) OnGossip(buf []byte) (delta mesh.GossipData, err error) {
	fmt.Println("on gossip:", buf)
	return
}

// Merge the gossiped data represented by buf into our state.
// Return the state information that was modified.
func (p *peer) OnGossipBroadcast(src mesh.PeerName, buf []byte) (received mesh.GossipData, err error) {
	// var msg SubMessage
	// err = gob.NewDecoder(bytes.NewReader(buf)).Decode(&msg)
	// if err != nil {
	// 	g.L.Info("on gossip broadcast decode error", zap.Error(err))
	// 	return
	// }

	// switch msg.TP {
	// case CLUSTER_SUB:
	// 	p.bk.subtrie.Subscribe(msg.Topic, msg.Cid, src, msg.UserName)
	// case CLUSTER_UNSUB:
	// 	p.bk.subtrie.UnSubscribe(msg.Topic, msg.Cid, src)
	// case CLUSTER_RUNNING_TIME_REQ:
	// 	t := make([]byte, 13)
	// 	t[4] = CLUSTER_RUNNING_TIME_RESP
	// 	binary.PutUvarint(t[5:], uint64(p.bk.runningTime.Unix()))
	// 	p.send.GossipUnicast(src, t)
	// }

	return
}

// Merge the gossiped data represented by buf into our state.
func (p *peer) OnGossipUnicast(src mesh.PeerName, buf []byte) error {
	// switch buf[4] {
	// case CLUSTER_RUNNING_TIME_RESP:
	// 	t, _ := binary.Uvarint(buf[5:])
	// 	if t < p.longestRunningTime {
	// 		p.longestRunningName = src
	// 		p.longestRunningTime = t
	// 	}

	// case CLUSTER_SUBS_SYNC_REQ:
	// 	b := p.bk.subtrie.Encode()[0]
	// 	p.send.GossipUnicast(src, b)

	// case CLUSTER_SUBS_SYNC_RESP:
	// 	set := NewSubTrie()
	// 	err := gob.NewDecoder(bytes.NewReader(buf[5:])).Decode(&set)
	// 	if err != nil {
	// 		g.L.Info("on gossip decode error", zap.Error(err))
	// 		return err
	// 	}

	// 	p.bk.subtrie = set
	// 	p.bk.subSynced = true

	// case CLUSTER_MSG_ROUTE:
	// 	p.bk.router.recvRoute(src, buf[5:])
	// }

	return nil
}

type stringset map[string]struct{}

func (ss stringset) Set(value string) error {
	ss[value] = struct{}{}
	return nil
}

func (ss stringset) String() string {
	return strings.Join(ss.slice(), ",")
}

func (ss stringset) slice() []string {
	slice := make([]string, 0, len(ss))
	for k := range ss {
		slice = append(slice, k)
	}
	sort.Strings(slice)
	return slice
}
