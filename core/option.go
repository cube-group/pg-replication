package core

import (
	"errors"
	"github.com/cube-group/pg-replication/core/adapter"
	"github.com/jackc/pgx"
	"regexp"
	"strings"
)

type EventType int

const (
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

type ReplicationDMLHandler func(msg ReplicationMessage) error

type ReplicationOption struct {
	ConnConfig          pgx.ConnConfig
	Adapter             adapter.LsnAdapter
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
			return errors.New("all tables not support update column")
		}
	}
	if t.Adapter == nil {
		return errors.New("adpater is nil")
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
