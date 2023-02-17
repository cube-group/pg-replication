package core

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"log"
	"os"
	"time"
)

type ReplicationSyncer struct {
	_debug bool
	_conn  *pgx.ReplicationConn

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
	msg.SchemaName, msg.TableName = t.set.Assist(relation)
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
	msg.EventType = eventType
	msg.Body = body
	return
}

func (t *ReplicationSyncer) dumpColumns(values, oldValues map[string]pgtype.Value) (res []string) {
	if oldValues == nil || values == nil {
		return
	}
	for k, v := range oldValues {
		if newV, ok := values[k]; !ok || newV.Get() != v.Get() {
			res = append(res, k)
		}
	}
	return
}

func (t *ReplicationSyncer) handle(lsn uint64, m Message) {
	var msg ReplicationMessage
	var err error
	switch v := m.(type) {
	case Relation:
		t.set.Add(v)
	case Insert:
		msg, err = t.dump(EventType_INSERT, v.RelationID, v.Row, nil)
		if err != nil {
			t.log("dump:", err)
		}
	case Update:
		msg, err = t.dump(EventType_UPDATE, v.RelationID, v.Row, v.OldRow)
		if err != nil {
			t.log("dump:", err)
		}
	case Delete:
		msg, err = t.dump(EventType_DELETE, v.RelationID, v.Row, nil)
		if err != nil {
			t.log("dump:", err)
		}
	default:
		msg = ReplicationMessage{}
	}
	if msg.SchemaName != "" {
		if err = t.dmlHandler(msg); err != nil {
			t.log("dmlHandler:", err)
			os.Exit(1)
		}
	}
	//wal set
	if lsn > 0 {
		if err := t.option.Adapter.Set(t.option.SlotName, lsn); err != nil {
			t.log("adapter set err: %v\n", err)
		}
	}
}

func (t *ReplicationSyncer) Start(ctx context.Context) (err error) {
	if err = t.option.valid(); err != nil {
		return
	}
	conn, err := t.conn()
	if err != nil {
		return err
	}
	defer conn.Close()

	var lsn uint64
	if startLSN, _ := t.option.Adapter.Get(t.option.SlotName); startLSN > 0 {
		t.log("startLSN:", startLSN, pgx.FormatLSN(startLSN))
		lsn = startLSN
	}
	defer t.option.Adapter.Close()

	//system
	//if res, err := t.result("IDENTIFY_SYSTEM;");err==nil {
	//	if len(res) > 0 {
	//		var lsnPos = util.MustString(res[0]["xlogpos"])
	//		if outputLSN, err := pgx.ParseLSN(lsnPos); err == nil {
	//			lsn = outputLSN
	//			t.log("startLSN:", lsn, lsnPos)
	//		}
	//	}
	//}
	// monitor table column update
	if t.option.MonitorUpdateColumn {
		for _, v := range t.option.Tables {
			if err := t.exec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL;", v)); err != nil {
				return err
			}
		}
	}

	if err = t.exec(fmt.Sprintf("CREATE PUBLICATION %s FOR %s", t.option.SlotName, t.option.PublicationTables())); err != nil {
		return
	}
	if err = t.exec(fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL %s", t.option.SlotName, "pgoutput")); err != nil {
		return
	}
	err = conn.StartReplication(t.option.SlotName, lsn, -1, t.pluginArgs("1", t.option.SlotName)...)
	if err != nil {
		return fmt.Errorf("failed to start replication2: %s", err)
	}

	statusTimeout := 10 * time.Second
	waitTimeout := 10 * time.Second
	tick := time.NewTicker(statusTimeout).C
	for {
		select {
		case <-tick:
			if err := t.sendStatus(lsn); err != nil {
				return err
			}
		default:
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
				if message.WalMessage.WalStart > lsn {
					lsn = message.WalMessage.WalStart
				}
				logmsg, err := Parse(message.WalMessage.WalData)
				if err != nil {
					return fmt.Errorf("invalid pgoutput message: %s", err)
				}
				t.handle(lsn, logmsg)
			}
			if message.ServerHeartbeat != nil {
				t.handle(message.ServerHeartbeat.ServerWalEnd, nil)
				if message.ServerHeartbeat.ReplyRequested == 1 {
					if err := t.sendStatus(lsn); err != nil {
						return err
					}
				}
			}
		}
	}
}

func (t *ReplicationSyncer) exec(sql string) error {
	conn, err := t.conn()
	if err != nil {
		return err
	}
	t.log("exec:", sql)
	_, err = conn.Exec(sql)
	if err != nil {
		pgErr, ok := err.(pgx.PgError)
		if !ok || pgErr.Code != "42710" {
			return err
		}
	}
	return nil
}

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

func (t *ReplicationSyncer) sendStatus(lsn uint64) error {
	if lsn == 0 {
		return nil
	}
	conn, err := t.conn()
	if err != nil {
		return err
	}
	k, err := pgx.NewStandbyStatus(lsn)
	if err != nil {
		return fmt.Errorf("error creating standby status: %s", err)
	}
	if err := conn.SendStandbyStatus(k); err != nil {
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

func (t *ReplicationSyncer) DropReplication() error {
	if err := t.exec(fmt.Sprintf("SELECT pg_drop_replication_slot('%s');", t.option.SlotName)); err != nil {
		return err
	}
	if err := t.exec(fmt.Sprintf("drop publication if exists %s;", t.option.SlotName)); err != nil {
		return err
	}
	return nil
}
