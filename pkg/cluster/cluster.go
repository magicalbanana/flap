package cluster

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"sort"
	"strings"
	"sync"

	"github.com/imdevlab/flap/pkg/config"
	"github.com/imdevlab/flap/pkg/message"
	"github.com/imdevlab/g"
	"github.com/imdevlab/g/utils"
	"github.com/weaveworks/mesh"
	"go.uber.org/zap"
)

type Cluster struct {
	sync.Mutex
	name   mesh.PeerName
	gossip mesh.Gossip

	OnSubscribe   func(topic []byte, cid uint64) bool
	OnUnSubscribe func(topic []byte, cid uint64) bool
	OnMessage     func(*message.Pub)
}

// Cluster implements mesh.Gossiper
var _ mesh.Gossiper = &Cluster{}

func New() *Cluster {
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

	c := &Cluster{
		name: name,
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

	router.Peers.OnGC(func(peer *mesh.Peer) {
		c.onPeerOffline(peer.Name)
	})
	gossip, err := router.NewGossip("default", c)
	if err != nil {
		g.L.Fatal("Could not create cluster gossip", zap.Error(err))
	}

	c.gossip = gossip

	g.L.Debug("cluster starting", zap.String("hwaddr", hwaddr), zap.Int("port", config.Conf.Cluster.Port))
	router.Start()

	// init connections to seeds
	peers := stringset{}
	for _, peer := range config.Conf.Cluster.SeedPeers {
		peers[peer] = struct{}{}
	}

	router.ConnectionMaker.InitiateConnections(peers.slice(), true)

	// When a node comes online, it will broadcast the online message to all peers
	c.gossip.GossipBroadcast(OnlineMessage{})
	return c
}

// Cluster methods
// when peer offline, we need to unsubscribe the channels in that peer
func (c *Cluster) onPeerOffline(peer mesh.PeerName) {
	fmt.Println("peer offline:", peer)
}

//implements the mesh.Gossiper
// Return a copy of our complete state.
func (c *Cluster) Gossip() (complete mesh.GossipData) {
	fmt.Println("send gossip")
	return complete
}

// Merge the gossiped data represented by buf into our state.
// Return the state information that was modified.
func (c *Cluster) OnGossip(buf []byte) (delta mesh.GossipData, err error) {
	fmt.Println("on gossip:", buf)
	return
}

// Merge the gossiped data represented by buf into our state.
// Return the state information that was modified.
func (c *Cluster) OnGossipBroadcast(src mesh.PeerName, buf []byte) (received mesh.GossipData, err error) {
	fmt.Println("recv broadcast:", src, buf)
	return
}

// Merge the gossiped data represented by buf into our state.
func (c *Cluster) OnGossipUnicast(src mesh.PeerName, buf []byte) error {

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
