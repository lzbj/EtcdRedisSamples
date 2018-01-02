package main

import (
	"context"
	"fmt"
	"time"
	"strings"
	"strconv"
	"os"
	"github.com/garyburd/redigo/redis"
	"github.com/kelseyhightower/confd/log"
)

// Client is a wrapper around the redis client
type Client struct {
	client   redis.Conn
	machines []string
	password string
}

// Iterate through `machines`, trying to connect to each in turn.
// Returns the first successful connection or the last error encountered.
// Assumes that `machines` is non-empty.
func tryConnect(machines []string, password string) (redis.Conn, error) {
	var err error
	for _, address := range machines {
		var conn redis.Conn
		var db int

		idx := strings.Index(address, "/")
		if idx != -1 {
			// a database is provided
			db, err = strconv.Atoi(address[idx+1:])
			if err == nil {
				address = address[:idx]
			}
		}

		network := "tcp"
		if _, err = os.Stat(address); err == nil {
			network = "unix"
		}
		log.Debug(fmt.Sprintf("Trying to connect to redis node %s", address))

		dialops := []redis.DialOption{
			redis.DialConnectTimeout(time.Second),
			redis.DialReadTimeout(time.Second),
			redis.DialWriteTimeout(time.Second),
			redis.DialDatabase(db),
		}

		if password != "" {
			dialops = append(dialops, redis.DialPassword(password))
		}

		conn, err = redis.Dial(network, address, dialops...)

		if err != nil {
			continue
		}
		return conn, nil
	}
	return nil, err
}

// Retrieves a connected redis client from the client wrapper.
// Existing connections will be tested with a PING command before being returned. Tries to reconnect once if necessary.
// Returns the established redis connection or the error encountered.
func (c *Client) connectedClient() (redis.Conn, error) {
	if c.client != nil {
		log.Debug("Testing existing redis connection.")

		resp, err := c.client.Do("PING")
		if (err != nil && err == redis.ErrNil) || resp != "PONG" {
			log.Error(fmt.Sprintf("Existing redis connection no longer usable. "+
				"Will try to re-establish. Error: %s", err.Error()))
			c.client = nil
		}
	}

	// Existing client could have been deleted by previous block
	if c.client == nil {
		var err error
		c.client, err = tryConnect(c.machines, c.password)
		if err != nil {
			return nil, err
		}
	}

	return c.client, nil
}

// listenPubSubChannels listens for messages on Redis pubsub channels. The
// onStart function is called after the channels are subscribed. The onMessage
// function is called for each message.
func listenPubSubChannels(ctx context.Context, redisServerAddr string,
	onStart func() error,
	onMessage func(channel string, data []byte) error,
	channels ...string) error {
	// A ping is set to the server with this period to test for the health of
	// the connection and server.
	const healthCheckPeriod = time.Minute

	c, err := redis.Dial("tcp", redisServerAddr,
		// Read timeout on server should be greater than ping period.
		redis.DialReadTimeout(healthCheckPeriod+10*time.Second),
		redis.DialWriteTimeout(10*time.Second))
	if err != nil {
		return err
	}
	defer c.Close()

	psc := redis.PubSubConn{Conn: c}

	if err := psc.Subscribe(redis.Args{}.AddFlat(channels)...); err != nil {
		return err
	}

	done := make(chan error, 1)

	// Start a goroutine to receive notifications from the server.
	go func() {
		for {
			switch n := psc.Receive().(type) {
			case error:
				done <- n
				return
			case redis.Message:
				if err := onMessage(n.Channel, n.Data); err != nil {
					done <- err
					return
				}
			case redis.Subscription:
				switch n.Count {
				case len(channels):
					// Notify application when all channels are subscribed.
					if err := onStart(); err != nil {
						done <- err
						return
					}
				case 0:
					// Return from the goroutine when all channels are unsubscribed.
					done <- nil
					return
				}
			}
		}
	}()

	ticker := time.NewTicker(healthCheckPeriod)
	defer ticker.Stop()
loop:
	for err == nil {
		select {
		case <-ticker.C:
			// Send ping to test health of connection and server. If
			// corresponding pong is not received, then receive on the
			// connection will timeout and the receive goroutine will exit.
			if err = psc.Ping(""); err != nil {
				break loop
			}
		case <-ctx.Done():
			break loop
		case err := <-done:
			// Return error from the receive goroutine.
			return err
		}
	}

	// Signal the receiving goroutine to exit by unsubscribing from all channels.
	psc.Unsubscribe()

	// Wait for goroutine to complete.
	return <-done
}

// NewRedisClient returns an *redis.Client with a connection to named machines.
// It returns an error if a connection to the cluster cannot be made.
func NewRedisClient(machines []string, password string) (*Client, error) {
	var err error
	clientWrapper := &Client{machines: machines, password: password, client: nil}
	clientWrapper.client, err = tryConnect(machines, password)
	return clientWrapper, err
}

func publish() {
	c, err := NewRedisClient([]string{"127.0.0.1:6379"}, "")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c.client.Close()

	c.client.Do("PUBLISH", "c1", "hello")
	c.client.Do("PUBLISH", "c2", "world")
	time.Sleep(time.Second * 2)
	c.client.Do("PUBLISH", "c1", "goodbye")
}

// This example shows how receive pubsub notifications with cancelation and
// health checks.
func main() {
	ctx, cancel := context.WithCancel(context.Background())

	err := listenPubSubChannels(ctx,
		"127.0.0.1:6379",
		func() error {
			// The start callback is a good place to backfill missed
			// notifications. For the purpose of this example, a goroutine is
			// started to send notifications.
			go publish()
			return nil
		},
		func(channel string, message []byte) error {
			fmt.Printf("channel: %s, message: %s\n", channel, message)

			// For the purpose of this example, cancel the listener's context
			// after receiving last message sent by publish().
			if string(message) == "goodbye" {
				cancel()
			}
			return nil
		},
		"c1", "c2")

	if err != nil {
		fmt.Println(err)
		return
	}

}
