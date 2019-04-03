package broker

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/imdevlab/flap/pkg/message"
	"github.com/imdevlab/flap/pkg/network/mqtt"
	"github.com/imdevlab/g"
	"go.uber.org/zap"
)

// For controlling dynamic buffer sizes.
const (
	headerSize  = 4
	maxBodySize = 65536 * 16 // 1MB
)

type client struct {
	id   uint64 // do not exceeds max(int32)
	conn net.Conn
	bk   *Broker

	sender chan []*message.Pub

	subs map[uint32]struct{}

	username []byte // username

	closed  bool
	closech chan struct{}
}

func newClient(conn net.Conn, b *Broker) *client {
	id := atomic.AddUint64(&b.gid, 1)
	c := &client{
		id:      id,
		conn:    conn,
		bk:      b,
		sender:  make(chan []*message.Pub, MAX_CHANNEL_LEN),
		subs:    make(map[uint32]struct{}),
		closech: make(chan struct{}),
	}
	b.Lock()
	b.clients[id] = c
	b.Unlock()

	return c
}

func (c *client) process() {
	defer func() {
		c.bk.Lock()
		delete(c.bk.clients, c.id)
		c.bk.Unlock()

		c.closed = true
		c.closech <- struct{}{}
		c.conn.Close()
		g.L.Debug("client closed", zap.Uint64("conn_id", c.id))
	}()

	// must waiting for connect first
	err := c.waitForConnect()
	if err != nil {
		g.L.Debug("cant receive connect packet from client", zap.Uint64("cid", c.id), zap.Error(err))
		return
	}

	g.L.Debug("new user online", zap.Uint64("cid", c.id), zap.String("username", string(c.username)), zap.String("ip", c.conn.RemoteAddr().String()))

	// start a goroutine for other clients sending msg to this client
	go c.sendLoop()

	// waiting for client's message
	reader := bufio.NewReaderSize(c.conn, 65536)
	for !c.closed {
		c.conn.SetDeadline(time.Now().Add(time.Second * message.MAX_IDLE_TIME))
		msg, err := mqtt.DecodePacket(reader)
		if err != nil {
			g.L.Info("Decode packet error", zap.Uint64("cid", c.id), zap.Error(err))
			return
		}

		// Handle the receive
		if err := c.onReceive(msg); err != nil {
			g.L.Info("handle receive error", zap.Uint64("cid", c.id), zap.Error(err))
			return
		}
	}
}

func (c *client) onReceive(msg mqtt.Message) error {
	switch msg.Type() {
	case mqtt.TypeOfSubscribe:
		packet := msg.(*mqtt.Subscribe)

		ack := mqtt.Suback{
			MessageID: packet.MessageID,
			Qos:       make([]uint8, 0, len(packet.Subscriptions)),
		}

		// Subscribe for each subscription
		for _, sub := range packet.Subscriptions {
			if err := c.onSubscribe(sub.Topic); err != nil {
				ack.Qos = append(ack.Qos, 0x80) // 0x80 indicate subscription failure
				// c.notifyError(err, packet.MessageID)
				continue
			}

			// Append the QoS
			ack.Qos = append(ack.Qos, sub.Qos)
		}

		if _, err := ack.EncodeTo(c.conn); err != nil {
			return err
		}
	case mqtt.TypeOfUnsubscribe:
	case mqtt.TypeOfPingreq:
		ack := mqtt.Pingresp{}
		if _, err := ack.EncodeTo(c.conn); err != nil {
			return err
		}
	case mqtt.TypeOfDisconnect:
	case mqtt.TypeOfPublish:

	}

	return nil
}

func (c *client) onSubscribe(topic []byte) error {
	tid := message.HashTopic(topic)
	c.subs[tid] = struct{}{}

	c.bk.cluster.Subscribe(tid, c.id)

	return nil
}
func (c *client) sendLoop() {
	defer func() {
		// when disconnect, automaticly unsubscribe the topic
		fmt.Println("here1111:", c.subs)
		for tid := range c.subs {
			c.bk.cluster.Unsubscribe(tid, c.id)
		}
		c.closed = true
		c.conn.Close()
		if err := recover(); err != nil {
			g.L.Warn("panic happend in write loop", zap.Error(err.(error)), zap.Stack("stack"), zap.Uint64("cid", c.id))
			return
		}
	}()

	for {
		select {
		case _ = <-c.sender:
		case <-c.closech:
			return
		}
	}
}

func (c *client) waitForConnect() error {
	reader := bufio.NewReaderSize(c.conn, 65536)
	c.conn.SetDeadline(time.Now().Add(time.Second * message.MAX_IDLE_TIME))

	msg, err := mqtt.DecodePacket(reader)
	if err != nil {
		return err
	}

	if msg.Type() == mqtt.TypeOfConnect {
		packet := msg.(*mqtt.Connect)
		if len(packet.Username) <= 0 {
			return errors.New("no username exist")
		}
		c.username = packet.Username

		// reply the connect ack
		ack := mqtt.Connack{ReturnCode: 0x00}
		if _, err := ack.EncodeTo(c.conn); err != nil {
			return err
		}
		return nil
	}

	return errors.New("first packet is not MSG_CONNECT")
}
