package core

import "log"

func (t *Replication) debug(name string, args ...interface{}) {
	if !t._debug {
		return
	}
	args = append([]interface{}{name}, args...)
	log.Println(args...)
}
