package core

type EventType int

const (
	EventType_READY    EventType = 0
	EventType_INSERT   EventType = 1
	EventType_UPDATE   EventType = 2
	EventType_DELETE   EventType = 3
	EventType_TRUNCATE EventType = 4
	EventType_COMMIT   EventType = 10
)

type ReplicationMessage struct {
	Lsn        uint64
	RelationID uint32
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
