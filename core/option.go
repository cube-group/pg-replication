package core

import (
	"errors"
	"github.com/jackc/pgx"
	"regexp"
	"strings"
)

type EventType int

const (
	EventType_READY    EventType = 0
	EventType_INSERT   EventType = 1
	EventType_UPDATE   EventType = 2
	EventType_DELETE   EventType = 3
	EventType_TRUNCATE EventType = 4
)

type ReplicationMessage struct {
	EventType  EventType
	SchemaName string
	TableName  string
	Body       map[string]interface{}
	Columns    []string
}

type DMLHandlerStatus int

const (
	DMLHandlerStatusSuccess  DMLHandlerStatus = 0 //wal lsn游标将会变动
	DMLHandlerStatusContinue DMLHandlerStatus = 1 //wal lsn游标不会变动
)

type ReplicationDMLHandler func(msg ...ReplicationMessage) DMLHandlerStatus

type ReplicationOption struct {
	ConnConfig pgx.ConnConfig
	// replication slot name
	// publication name
	SlotName string
	// publication tables 为空则为FOR ALL TABLES监听所有表DML
	// Demo: CREATE PUBLICATION name FOR ALL TABLES;
	// Demo: CREATE PUBLICATION name FOR TABLE users, departments;
	// Demo: CREATE PUBLICATION name FOR TABLE mydata WITH (publish = 'insert'); // 仅监听表mydata的insert操作
	// Demo: CREATE PUBLICATION name FOR TABLE abc_*; // abc_开头的所有表，不适用于分区表
	Tables []string
	// 需要开启replica identity full权限的表
	// 逻辑复制-更改复制标识
	// 默认情况下，复制标识就是主键（如果有主键）。
	// 也可以在复制标识上设置另一个唯一索引（有特定的额外要求）。
	// 如果表没有合适的键，那么可以设置成复制标识“full”，它表示整个行都成为那个键。
	// 不过，这样做效率很低，只有在没有其他方案的情况下才应该使用。
	TablesReplicaIdentityFull []string
}

func (t *ReplicationOption) valid() error {
	if !regexp.MustCompile(`[a-z0-9_]{3,64}`).MatchString(t.SlotName) {
		return errors.New("slotName invalid")
	}
	return nil
}

func (t *ReplicationOption) PublicationTables() (res string) {
	if t.Tables == nil || len(t.Tables) == 0 {
		return "ALL TABLES"
	} else {
		return "TABLE " + strings.Join(t.Tables, ",")
	}
}
