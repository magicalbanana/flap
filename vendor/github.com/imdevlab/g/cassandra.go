package g

import (
	"time"

	"github.com/gocql/gocql"
)

// Cassandra ...
type Cassandra struct {
	Session *gocql.Session
}

// NewCassandra ...
func NewCassandra() *Cassandra {
	return &Cassandra{}
}

// Init ...
func (c *Cassandra) Init(numConns int, keyspace string, addrs []string) error {
	// connect to the cluster
	cluster := gocql.NewCluster(addrs...)
	cluster.Keyspace = keyspace
	//设置连接池的数量,默认是2个（针对每一个host,都建立起NumConns个连接）
	cluster.NumConns = numConns
	cluster.Timeout = 5 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	c.Session = session

	return nil
}

// Close ...
func (c *Cassandra) Close() {
	if c.Session != nil {
		c.Session.Close()
	}
}
