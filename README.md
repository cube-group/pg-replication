# pgx-replication-listen
golang postgres replication logical slot analyse

### postgres.conf
* wal_level = logical
* max_replication_slots = 10

### help doc
* logical replication decoding: [demo](https://www.postgresql.org/docs/current/logicaldecoding-example.html)

### run demo
```go
package main

import (
	"context"
	"github.com/cube-group/pg-replication/core"
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
			Tables:              []string{"sync"},
			MonitorUpdateColumn: true, //update操作是否监听其操作列
		},
		handle,
	)
	log.Fatalf("sync err: %v", syncer.Debug().Start(context.Background()))
}

func handle(msg core.ReplicationMessage) core.DMLHandlerStatus {
	switch msg.EventType {
	case core.EventType_READY:
		//TODO READY LISTEN
		log.Println("Ready")
	case core.EventType_INSERT, core.EventType_UPDATE, core.EventType_DELETE:
		log.Printf("[%v.%v] (%v) %+v %+v", msg.SchemaName, msg.TableName, msg.EventType, msg.Columns, msg.Body)
	}
	//return core.DMLHandlerStatusContinue //继续但不记录此次游标
	return core.DMLHandlerStatusSuccess //继续并记录此次游标
	//return core.DMLHandlerStatusError    //报错且继续拉取且不记录此次游标
}


```