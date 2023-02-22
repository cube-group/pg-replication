package core

import (
	"errors"
	"github.com/jackc/pgx"
	"regexp"
	"strings"
)

type EventType int

const (
	EventType_READY  EventType = 0
	EventType_INSERT EventType = 1
	EventType_UPDATE EventType = 2
	EventType_DELETE EventType = 3
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
	DMLHandlerStatusError    DMLHandlerStatus = 2 //wal lsn游标不会变动，且退出
)

type ReplicationDMLHandler func(msg ReplicationMessage) DMLHandlerStatus

type ReplicationOption struct {
	ConnConfig          pgx.ConnConfig
	SlotName            string
	Tables              []string
	MonitorUpdateColumn bool
}

func (t *ReplicationOption) valid() error {
	if !regexp.MustCompile(`[a-z0-9_]{3,64}`).MatchString(t.SlotName) {
		return errors.New("slotName invalid")
	}
	if t.Tables == nil || len(t.Tables) == 0 {
		if t.MonitorUpdateColumn {
			return errors.New("all tables not support monitor updated column")
		}
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
