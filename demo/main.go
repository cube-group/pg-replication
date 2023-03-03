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
		"test", //复制槽和发布流名称
		pgx.ConnConfig{
			Host:     "192.168.4.157",
			Port:     30433,
			Database: "web",
			User:     "postgres",
			Password: "default",
		},
	)
	syncer.Debug()
	// 创建逻辑复制槽位
	if err := syncer.CreateReplication(); err != nil {
		log.Fatal(err)
	}
	// 创建发布流
	if err := syncer.CreatePublication([]string{"sync", "test*"}); err != nil {
		log.Fatal(err)
	}
	// 设置复制标识
	if err := syncer.SetReplicaIdentity([]string{"sync", "test"}, core.ReplicaIdentityFull); err != nil {
		log.Fatal(err)
	}
	log.Fatalf("sync err: %v", syncer.Start(context.Background(), dmlHandler))
}

func dmlHandler(msg ...core.ReplicationMessage) core.DMLHandlerStatus {
	for _, m := range msg {
		switch m.EventType {
		case core.EventType_READY:
			//TODO READY LISTEN
			log.Println("Ready")
		case core.EventType_INSERT, core.EventType_UPDATE, core.EventType_DELETE, core.EventType_TRUNCATE:
			log.Printf(
				"[%v.%v] (%v) %+v %+v",
				m.SchemaName, //数据库或空间
				m.TableName,  //表名
				m.EventType,  //DML类型(INSERT/UPDATE/DELETE/TRUNCATE)
				m.Columns,    //updated columns只针对MonitorUpdateColumn=true
				m.Body,       //完成数据条
			)
		}
	}
	return core.DMLHandlerStatusSuccess //继续并记录此次游标
	//return core.DMLHandlerStatusContinue //继续但不记录此次游标，多用于批量处理
}
