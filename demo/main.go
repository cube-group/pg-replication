package main

import (
	"context"
	"github.com/cube-group/pgx-replication/core"
	"github.com/cube-group/pgx-replication/core/adapter"
	"github.com/jackc/pgx"
	"log"
)

var syncer *core.ReplicationSyncer

func main() {
	syncer = core.NewReplicationSyncer(
		core.ReplicationOption{
			SlotName: "test",
			ConnConfig: pgx.ConnConfig{
				Host:     "192.168.4.157",
				Port:     30433,
				Database: "web",
				User:     "postgres",
				Password: "default",
			},
			Adapter: adapter.NewLsnFileAdapter("."),
			//Adapter: adapter.NewRedisLsnAdapter(&redis.Options{
			//	Addr:     "192.168.4.157:30379",
			//	DB:       0,
			//	Password: "xx",
			//	PoolSize: 1,
			//}),
			Tables:              []string{"lin"},
			MonitorUpdateColumn: true,
		},
		handle,
	)
	log.Fatalf("sync err: %v", syncer.Debug().Start(context.Background()))
}

func handle(msg core.ReplicationMessage) error {
	log.Printf("[%v.%v] (%v) %+v %+v", msg.SchemaName, msg.TableName, msg.EventType, msg.Columns, msg.Body)
	return nil
}
