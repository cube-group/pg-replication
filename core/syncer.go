package core

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	"log"
	"strings"
)

type ReplicationMessageType string

const (
	ReplicationMessageTypeInsert ReplicationMessageType = "insert"
	ReplicationMessageTypeUpdate ReplicationMessageType = "update"
	ReplicationMessageTypeDelete ReplicationMessageType = "delete"
)

type ReplicationMessage struct {
	MessageType ReplicationMessageType
	Namespace   string
	Table       string
	Body        map[string]interface{}
	Wal         uint64
}

type ReplicationHandler func(msg ReplicationMessage)

type messageJson struct {
	ID            int64  `json:"ID"`
	ReplicationID int64  `json:"Replication"`
	Schema        string `json:"Namespace"`
	Table         string `json:"Name"`
}

type ReplicationSyncer struct {
	_debug bool
	_conn  *pgx.ReplicationConn

	pgCfg   pgx.ConnConfig
	set     *RelationSet
	handler ReplicationHandler
}

func NewReplicationSyncer(pgCfg pgx.ConnConfig, handler ReplicationHandler) *ReplicationSyncer {
	i := new(ReplicationSyncer)
	i.pgCfg = pgCfg
	i.set = NewRelationSet()
	i.handler = handler
	return i
}

func (t *ReplicationSyncer) Debug() *ReplicationSyncer {
	t._debug = true
	return t
}

func (t *ReplicationSyncer) log(any ...interface{}) {
	if t._debug {
		log.Println(any...)
	}
}

func (t *ReplicationSyncer) conn() (*pgx.ReplicationConn, error) {
	if t._conn == nil {
		conn, err := pgx.ReplicationConnect(t.pgCfg)
		if err != nil {
			return nil, err
		}
		t._conn = conn
	}
	return t._conn, nil
}

func (t *ReplicationSyncer) dump(messageType ReplicationMessageType, relation uint32, row []Tuple) (msg ReplicationMessage, err error) {
	namespace, table, values, err := t.set.Values(relation, row)
	if err != nil {
		err = fmt.Errorf("error parsing values: %s", err)
		return
	}
	body := make(map[string]interface{}, 0)
	for name, value := range values {
		val := value.Get()
		body[name] = val
	}
	msg = ReplicationMessage{
		MessageType: messageType,
		Namespace:   namespace,
		Table:       table,
		Body:        body,
	}
	return
}

func (t *ReplicationSyncer) handle(wal uint64, m Message) error {
	var msg ReplicationMessage
	var err error
	switch v := m.(type) {
	case Relation:
		t.set.Add(v)
	case Insert:
		msg, err = t.dump(ReplicationMessageTypeInsert, v.RelationID, v.Row)
		if err != nil {
			log.Println(err)
		}
	case Update:
		msg, err = t.dump(ReplicationMessageTypeUpdate, v.RelationID, v.Row)
		if err != nil {
			log.Println(err)
		}
	case Delete:
		msg, err = t.dump(ReplicationMessageTypeDelete, v.RelationID, v.Row)
		if err != nil {
			log.Println(err)
		}
	default:
		msg = ReplicationMessage{}
	}
	msg.Wal = wal
	t.log("wal: ", wal)
	t.handler(msg)
	return nil
}

func (t *ReplicationSyncer) Start(replicationName string, tables []string, wal uint64) (err error) {
	conn, err := t.conn()
	if err != nil {
		return
	}
	// 创建发布流
	if err = t.createPublication(replicationName, tables); err != nil {
		return
	}

	s := NewSubscription(replicationName, replicationName)
	return s.Start(context.TODO(), conn, wal, t.handle)
}

func (t *ReplicationSyncer) createPublication(replicationName string, tables []string) error {
	conn, err := t.conn()
	if err != nil {
		return err
	}
	// publication exist check
	rows, err := conn.Exec(fmt.Sprintf("SELECT oid FROM pg_publication WHERE pubname='%s'", replicationName))
	if err != nil {
		return err
	}
	if rows.RowsAffected() > 0 {
		t.log("publication: ", replicationName, " has existed")
		return nil
	}else{
		t.log("publication: ", replicationName, " not exist and will be created")
	}

	var forTables string
	// create publication
	if tables == nil || len(tables) == 0 {
		forTables = "ALL TABLES"
	} else {
		forTables = "TABLE " + strings.Join(tables, ",")
	}
	_, err = conn.Exec(fmt.Sprintf("CREATE PUBLICATION %s FOR %s", replicationName, forTables))
	return err
}
