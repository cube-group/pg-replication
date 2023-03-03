package core

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"log"
	"regexp"
	"strings"
	"time"
)

// 表复制标识枚举
type ReplicaIdentity string

const (
	// ReplicaIdentityFull
	// 需要开启replica identity full权限的表
	// 逻辑复制-更改复制标识
	// 默认情况下，复制标识就是主键（如果有主键）。
	// 也可以在复制标识上设置另一个唯一索引（有特定的额外要求）。
	// 如果表没有合适的键，那么可以设置成复制标识“full”，它表示整个行都成为那个键。
	// 不过，这样做效率很低，只有在没有其他方案的情况下才应该使用。
	ReplicaIdentityFull ReplicaIdentity = "FULL"
	// ReplicaIdentityDefault
	// 默认按照主键id为复制标识
	// update时无法得知详细更新column信息
	ReplicaIdentityDefault ReplicaIdentity = "DEFAULT"
)

type ReplicationSyncer struct {
	_debug    bool
	_conn     *pgx.ReplicationConn
	_flushMsg []ReplicationMessage
	_running  bool

	name   string
	config pgx.ConnConfig
	set    *RelationSet
}

func NewReplicationSyncer(name string, config pgx.ConnConfig) *ReplicationSyncer {
	if !regexp.MustCompile(`[a-z0-9_]{3,64}`).MatchString(name) {
		log.Fatal("name invalid")
	}
	return &ReplicationSyncer{name: name, config: config, set: NewRelationSet()}
}

func (t *ReplicationSyncer) Debug() *ReplicationSyncer {
	t._debug = true
	return t
}

func (t *ReplicationSyncer) UnDebug() *ReplicationSyncer {
	t._debug = false
	return t
}

func (t *ReplicationSyncer) log(any ...interface{}) {
	if t._debug {
		log.Println(any...)
	}
}

func (t *ReplicationSyncer) conn() (*pgx.ReplicationConn, error) {
	if t._conn == nil {
		conn, err := pgx.ReplicationConnect(t.config)
		if err != nil {
			return nil, err
		}
		t._conn = conn
	}
	return t._conn, nil
}

func (t *ReplicationSyncer) dump(eventType EventType, relation uint32, row, oldRow []Tuple) (msg ReplicationMessage, err error) {
	msg.RelationID = relation
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

func (t *ReplicationSyncer) handle(message *pgx.WalMessage, dmlHandler ReplicationDMLHandler) error {
	msg, err := Parse(message.WalData)
	if err != nil {
		return fmt.Errorf("invalid pgoutput message: %s", err)
	}
	var m ReplicationMessage
	switch v := msg.(type) {
	case Relation:
		if t._flushMsg == nil {
			t._flushMsg = make([]ReplicationMessage, 0)
		}
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
		if len(t._flushMsg) > 0 {
			status := dmlHandler(t._flushMsg...)
			t._flushMsg = nil
			if status == DMLHandlerStatusSuccess {
				err = t.sendStatusACK(message.WalStart)
			}
		}
	}
	if err != nil {
		return err
	}
	if m.RelationID > 0 {
		t._flushMsg = append(t._flushMsg, m)
	}
	return nil
}

func (t *ReplicationSyncer) Shutdown() {
	t._running = false
}

func (t *ReplicationSyncer) Start(ctx context.Context, dmlHandler ReplicationDMLHandler) (err error) {
	conn, err := t.conn()
	defer conn.Close()
	// create replica identity|publication|replication
	if err = t.CreateReplication(); err != nil {
		return
	}
	// start replication slot
	pluginArguments := t.pluginArgs("1", t.name)
	if err = conn.StartReplication(t.name, 0, -1, pluginArguments...); err != nil {
		return
	}
	t._running = true
	// ready notify
	dmlHandler(ReplicationMessage{EventType: EventType_READY})
	// round read
	waitTimeout := 10 * time.Second
	for {
		if !t._running {
			return errors.New("syncer shutting down.")
		}
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
			if err = t.handle(message.WalMessage, dmlHandler); err != nil {
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
func (t *ReplicationSyncer) execEx(sql string) error {
	conn, err := t.conn()
	if err != nil {
		return err
	}
	if _, err = conn.Exec(sql); err != nil {
		pgErr, ok := err.(pgx.PgError)
		// 42710 already exist
		// 42704 no exist
		if !ok || (pgErr.Code != "42710" && pgErr.Code != "42704") {
			return err
		} else {
			t.log("exec:", sql, "[silent]")
		}
	} else {
		t.log("exec:", sql)
	}
	return nil
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

// CreateReplication 创建逻辑复制槽
// 锁定起始lsn位置
func (t *ReplicationSyncer) CreateReplication() (err error) {
	// create publication
	if err = t.execEx(fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL %s NOEXPORT_SNAPSHOT", t.name, "pgoutput")); err != nil {
		return
	}
	return
}

// DropReplication 移除复制槽
func (t *ReplicationSyncer) DropReplication() error {
	if err := t.execEx(fmt.Sprintf("SELECT pg_drop_replication_slot('%s');", t.name)); err != nil {
		return err
	}
	return nil
}

// CreatePublication 移除复制槽
func (t *ReplicationSyncer) CreatePublication(tables []string) error {
	var tableString string
	if tables == nil || len(tables) == 0 {
		tableString = "ALL TABLES"
	} else {
		tableString = "TABLE " + strings.Join(tables, ",")
	}
	// 详见：select * from pg_catalog.pg_publication;
	return t.execEx(fmt.Sprintf("CREATE PUBLICATION %s FOR %s", t.name, tableString))
}

// DropPublication 移除复制槽
func (t *ReplicationSyncer) DropPublication() error {
	if err := t.execEx(fmt.Sprintf("drop publication if exists %s;", t.name)); err != nil {
		return err
	}
	return nil
}

// SetReplicaIdentity 配置表复制标识
func (t *ReplicationSyncer) SetReplicaIdentity(tables []string, status ReplicaIdentity) (err error) {
	for _, v := range tables {
		if err = t.execEx(fmt.Sprintf("ALTER TABLE %s replica identity %s", v, status)); err != nil {
			return
		}
	}
	return
}
