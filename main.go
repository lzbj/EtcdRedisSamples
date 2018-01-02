package main

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"log"
	"strconv"
	"sync"
	"time"
)

var (
	dialTimeout    = 2 * time.Second
	requestTimeout = 10 * time.Second
	wg             = sync.WaitGroup{}
)

func WatchDemo(ctx context.Context, cli *clientv3.Client, kv clientv3.KV) {
	fmt.Println("*** WatchDemo()")
	// Delete all keys
	kv.Delete(ctx, "key", clientv3.WithPrefix())
	finishedInsertionChan := make(chan interface{})

	go func() {
		watchChan := cli.Watch(ctx, "key", clientv3.WithPrefix())
		for {
			select {
			case <-finishedInsertionChan:
				fmt.Println("finished insertion")
			default:
				continue
			}
			go func() {
				var index int
				for {
					select {

					case result := <-watchChan:

						for _, ev := range result.Events {
							fmt.Printf("channel events: %s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
							index++
							fmt.Printf("received %d events\n", index)
						}
					default:
						continue
					}
				}

			}()

			go func() {
				for {
					select {
					case <-time.After(10 * time.Second):
						fmt.Println("time out")
						wg.Done()
					}
				}
			}()
		}
	}()
	go func() {
		//Insert some keys
		for i := 0; i < 100; i++ {
			go func(i int) {
				k := fmt.Sprintf("keyold_%02d", i)
				kv.Put(ctx, k, strconv.Itoa(i))
				fmt.Printf("put the key %s with value %s\n", k, strconv.Itoa(i))
			}(i)

		}
		finishedInsertionChan <- 1
	}()

}

func main() {
	ctx, _ := context.WithTimeout(context.Background(), requestTimeout)
	cli, err := clientv3.New(clientv3.Config{
		DialTimeout: dialTimeout,
		Endpoints:   []string{"127.0.0.1:2379"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()
	kv := clientv3.NewKV(cli)

	wg.Add(1)
	WatchDemo(ctx, cli, kv)

	wg.Wait()
}
