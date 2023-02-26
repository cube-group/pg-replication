package core

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"log"
	"time"
)

type ReplicationSyncer struct {
	_debug    bool
	_conn     *pgx.ReplicationConn
	_flushMsg []ReplicationMessage

	option     ReplicationOption
	dmlHandler ReplicationDMLHandler
	set        *RelationSet
}

func NewReplicationSyncer(option ReplicationOption, dmlHandler ReplicationDMLHandler) *ReplicationSyncer {
	i := new(ReplicationSyncer)
	i.option = option
	i.dmlHandler = dmlHandler
	i.set = NewRelationSet()
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
		conn, err := pgx.ReplicationConnect(t.option.ConnConfig)
		if err != nil {
			return nil, err
		}
		t._conn = conn
	}
	return t._conn, nil
}

func (t *ReplicationSyncer) dump(eventType EventType, relation uint32, row, oldRow []Tuple) (msg ReplicationMessage, err error) {
	msg.EventType = eventType
	msg.SchemaName, msg.TableName = t.set.Assist(relation)
	if row == nil && oldRow == nil {
		return
	}
	values, err := t.set.Values(relation, row)
	if err != nil {
		err = fmt.Errorf("error parsing values: %s", err)
		return
	}
	if oldRow != nil {
		if oldValues, er := t.set.Values(relation, oldRow); er == nil {
			msg.Columns = t.dumpColumns(values, oldValues)
		}
	}
	body := make(map[string]interface{}, 0)
	for name, value := range values {
		val := value.Get()
		body[name] = val
	}
	msg.Body = body
	return
}

func (t *ReplicationSyncer) dumpColumns(values, oldValues map[string]pgtype.Value) (res []string) {
	if oldValues == nil || values == nil {
		return nil
	}
	for k, v := range oldValues {
		if newV, ok := values[k]; !ok || newV.Get() != v.Get() {
			res = append(res, k)
		}
	}
	return
}

func (t *ReplicationSyncer) handle(message *pgx.WalMessage) error {
	msg, err := Parse(message.WalData)
	if err != nil {
		return fmt.Errorf("invalid pgoutput message: %s", err)
	}
	var m ReplicationMessage
	switch v := msg.(type) {
	case Relation:
		t._flushMsg = make([]ReplicationMessage, 0)
		t.set.Add(v)
	case Insert:
		m, err = t.dump(EventType_INSERT, v.RelationID, v.Row, nil)
	case Update:
		m, err = t.dump(EventType_UPDATE, v.RelationID, v.Row, v.OldRow)
	case Delete:
		m, err = t.dump(EventType_DELETE, v.RelationID, v.Row, nil)
	case Truncate:
		m, err = t.dump(EventType_TRUNCATE, v.RelationID, nil, nil)
	case Commit:
		if t._flushMsg != nil || len(t._flushMsg) > 0 {
			status := t.dmlHandler(t._flushMsg...)
			t._flushMsg = nil
			if status == DMLHandlerStatusSuccess {
				err = t.sendStatusACK(message.WalStart)
			}
		}
	}
	if err != nil {
		return err
	}
	if m.SchemaName != "" {
		t._flushMsg = append(t._flushMsg, m)
	}
	return nil
}

func (t *ReplicationSyncer) Start(ctx context.Context) (err error) {
	if err = t.option.valid(); err != nil {
		return
	}
	conn, err := t.conn()
	defer conn.Close()
	// create replica identity|publication|replication
	if err = t.CreateReplication(); err != nil {
		return
	}
	// start replication slot
	pluginArguments := t.pluginArgs("1", t.option.SlotName)
	if err = conn.StartReplication(t.option.SlotName, 0, -1, pluginArguments...); err != nil {
		return
	}
	// ready notify
	t.dmlHandler(ReplicationMessage{EventType: EventType_READY})
	// round read
	waitTimeout := 10 * time.Second
	for {
		var message *pgx.ReplicationMessage
		wctx, cancel := context.WithTimeout(ctx, waitTimeout)
		message, err = conn.WaitForReplicationMessage(wctx)
		cancel()
		if err == context.DeadlineExceeded {
			continue
		}
		if err != nil {
			return fmt.Errorf("replication failed: %s", err)
		}
		if message.WalMessage != nil {
			if err = t.handle(message.WalMessage); err != nil {
				return err
			}
		}
		// 服务器心跳验证当前sub是否可用
		// 不向master发送reply可能会导致连接EOF
		if message.ServerHeartbeat != nil {
			if message.ServerHeartbeat.ReplyRequested == 1 {
				if err = t.sendStatusACK(0); err != nil {
					return err
				}
			}
		}
	}
}

// 执行sql忽略exist
func (t *ReplicationSyncer) isErrorWithoutExist(err error) error {
	if err != nil {
		pgErr, ok := err.(pgx.PgError)
		if !ok || pgErr.Code != "42710" {
			return err
		}
	}
	return nil
}

// 执行sql忽略exist
func (t *ReplicationSyncer) execEx(sql string) error {
	conn, err := t.conn()
	if err != nil {
		return err
	}
	t.log("exec:", sql)
	_, err = conn.Exec(sql)
	return t.isErrorWithoutExist(err)
}

// 执行sql并获取结果
func (t *ReplicationSyncer) result(sql string) (res []map[string]interface{}, err error) {
	conn, err := t.conn()
	if err != nil {
		return
	}
	t.log("query:", sql)
	rows, err := conn.Query(sql)
	if err != nil {
		return
	}
	res = make([]map[string]interface{}, 0)
	var values []interface{}
	for rows.Next() {
		values, err = rows.Values()
		fmt.Println(values, rows.FieldDescriptions())
		if err != nil {
			return
		}
		var item = make(map[string]interface{})
		for k, v := range rows.FieldDescriptions() {
			item[v.Name] = values[k]
		}
		res = append(res, item)
	}
	return
}

// 向master发送lsn，即：WAL中使用者已经收到解码数据的最新位置
// 详见：select * from pg_catalog.pg_replication_slots；结果中的confirmed_flush_lsn
func (t *ReplicationSyncer) sendStatusACK(lsn uint64) error {
	conn, err := t.conn()
	if err != nil {
		return err
	}
	k, err := pgx.NewStandbyStatus(lsn)
	if err != nil {
		return fmt.Errorf("error creating standby status: %s", err)
	}
	if err = conn.SendStandbyStatus(k); err != nil {
		return fmt.Errorf("failed to send standy status: %s", err)
	}
	t.log("sendStatus lsn:", lsn, pgx.FormatLSN(lsn))
	return nil
}

func (t *ReplicationSyncer) pluginArgs(version, publication string) []string {
	//} else if outputPlugin == "wal2json" {
	//	pluginArguments = []string{"\"pretty-print\" 'true'"}
	//}
	return []string{fmt.Sprintf(`proto_version '%s'`, version), fmt.Sprintf(`publication_names '%s'`, publication)}
}

// 创建复制槽系列
func (t *ReplicationSyncer) CreateReplication() (err error) {
	if err = t.option.valid(); err != nil {
		return
	}
	// monitor table column update
	if t.option.MonitorUpdateColumn {
		for _, v := range t.option.Tables {
			if err = t.execEx(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL;", v)); err != nil {
				return err
			}
		}
	}
	// create publication
	// 详见：select * from pg_catalog.pg_publication;
	if err = t.execEx(fmt.Sprintf("CREATE PUBLICATION %s FOR %s", t.option.SlotName, t.option.PublicationTables())); err != nil {
		return
	}
	if err = t.execEx(fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL %s NOEXPORT_SNAPSHOT", t.option.SlotName, "pgoutput")); err != nil {
		return
	}
	return
}

// 移除复制槽
func (t *ReplicationSyncer) DropReplication() error {
	if err := t.execEx(fmt.Sprintf("SELECT pg_drop_replication_slot('%s');", t.option.SlotName)); err != nil {
		return err
	}
	if err := t.execEx(fmt.Sprintf("drop publication if exists %s;", t.option.SlotName)); err != nil {
		return err
	}
	return nil
}
