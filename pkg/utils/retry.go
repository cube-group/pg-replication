package utils

import (
	"fmt"
	"time"
)

//重试函数
func Retry(name string, tryTimes int, sleep time.Duration, callback func() error) (err error) {
	for i := 1; i <= tryTimes; i++ {
		err = callback()
		if err == nil {
			return nil
		}
		time.Sleep(sleep)
	}
	return fmt.Errorf("%v failed retry %d, err: %v", name, tryTimes, err)

}

//重试，限制时间
func RetryDurations(name string, max time.Duration, sleep time.Duration, callback func() error) (err error) {
	t0 := time.Now()
	i := 0
	for {
		err = callback()
		if err == nil {
			return
		}
		delta := time.Now().Sub(t0)
		if delta > max {
			return fmt.Errorf("%v failed timeout, retry %d，err: %v", name, i, err)
		}
		time.Sleep(sleep)
		i++
	}
}
