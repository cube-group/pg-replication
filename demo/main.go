package main

import (
	"context"
	"github.com/cube-group/pg-replication/core"
	"github.com/jackc/pgx"
	"log"
	"time"
)

var syncer *core.ReplicationSyncer

func main() {
	syncer = core.NewReplicationSyncer(
		core.ReplicationOption{
			SlotName: "test", //复制槽和发布流名称
			ConnConfig: pgx.ConnConfig{
				Host:     "192.168.4.157",
				Port:     30433,
				Database: "web",
				User:     "postgres",
				Password: "default",
			},
			Tables:              []string{"sync"}, //复制槽关心的表，若
			MonitorUpdateColumn: true, //update操作是否监听其操作列
		},
		dmlHandler,
	)
	log.Fatalf("sync err: %v", syncer.Debug().Start(context.Background()))
}

func dmlHandler(msg ...core.ReplicationMessage) core.DMLHandlerStatus {
	for _, m := range msg {
		switch m.EventType {
		case core.EventType_READY:
			//TODO READY LISTEN
			log.Println("Ready")
		case core.EventType_INSERT, core.EventType_UPDATE, core.EventType_DELETE:
			log.Printf(
				"[%v.%v] (%v) %+v %+v",
				m.SchemaName, //数据库或空间
				m.TableName,  //表名
				m.EventType,  //DML类型(INSERT/UPDATE/DELETE)
				m.Columns,    //updated columns只针对MonitorUpdateColumn=true
				m.Body,       //完成数据条
			)
		}
	}
	time.Sleep(time.Second)
	return core.DMLHandlerStatusSuccess //继续并记录此次游标
	//return core.DMLHandlerStatusContinue //继续但不记录此次游标，多用于批量处理
}
