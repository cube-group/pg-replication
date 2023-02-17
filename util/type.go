package util

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

func GetTypeName(v interface{}) {
	valueOf := reflect.ValueOf(v)
	if valueOf.Type().Kind() == reflect.Ptr {
		fmt.Println(reflect.Indirect(valueOf).Type().Name())
	} else {
		fmt.Println(valueOf.Type().Name())
	}
}

/*
类型转换通用函数
Must开头的为必须返回相应类型的值,第二个参数为默认值，转换失败会传了默认值会返回默认值
非Must开头的会多返回err,不为nil则是转型失败
*/

func Int(data interface{}) (int, error) {
	switch data.(type) {
	case string:
		i, err := strconv.ParseInt(data.(string), 10, 64)
		return int(i), err
	case float32, float64:
		return int(reflect.ValueOf(data).Float()), nil
	case int, int8, int16, int32, int64:
		return int(reflect.ValueOf(data).Int()), nil
	case uint, uint8, uint16, uint32, uint64:
		return int(reflect.ValueOf(data).Uint()), nil
	}
	return 0, errors.New("invalid value convert")
}

func MustInt(data interface{}, defaultValue ...int) int {
	if s, ok := Int(data); ok == nil {
		return s
	}
	if len(defaultValue) == 1 {
		return defaultValue[0]
	}
	return 0
}

func Int8(data interface{}) (int8, error) {
	switch data.(type) {
	case string:
		i, err := strconv.ParseInt(data.(string), 10, 8)
		return int8(i), err
	case float32, float64:
		return int8(reflect.ValueOf(data).Float()), nil
	case int, int8, int16, int32, int64:
		return int8(reflect.ValueOf(data).Int()), nil
	case uint, uint8, uint16, uint32, uint64:
		return int8(reflect.ValueOf(data).Uint()), nil
	}
	return 0, errors.New("invalid value convert")
}

func MustInt8(data interface{}, defaultValue ...int8) int8 {
	if s, ok := Int8(data); ok == nil {
		return s
	}
	if len(defaultValue) == 1 {
		return defaultValue[0]
	}
	return 0
}

func Int16(data interface{}) (int16, error) {
	switch data.(type) {
	case string:
		i, err := strconv.ParseInt(data.(string), 10, 8)
		return int16(i), err
	case float32, float64:
		return int16(reflect.ValueOf(data).Float()), nil
	case int, int8, int16, int32, int64:
		return int16(reflect.ValueOf(data).Int()), nil
	case uint, uint8, uint16, uint32, uint64:
		return int16(reflect.ValueOf(data).Uint()), nil
	}
	return 0, errors.New("invalid value convert")
}

func MustInt16(data interface{}, defaultValue ...int16) int16 {
	if s, ok := Int16(data); ok == nil {
		return s
	}
	if len(defaultValue) == 1 {
		return defaultValue[0]
	}
	return 0
}

func Int32(data interface{}) (int32, error) {
	switch data.(type) {
	case string:
		i, err := strconv.ParseInt(data.(string), 10, 32)
		return int32(i), err
	case float32, float64:
		return int32(reflect.ValueOf(data).Float()), nil
	case int, int8, int16, int32, int64:
		return int32(reflect.ValueOf(data).Int()), nil
	case uint, uint8, uint16, uint32, uint64:
		return int32(reflect.ValueOf(data).Uint()), nil
	}
	return 0, errors.New("invalid value convert")
}

func MustInt32(data interface{}, defaultValue ...int32) int32 {
	if s, ok := Int32(data); ok == nil {
		return s
	}
	if len(defaultValue) == 1 {
		return defaultValue[0]
	}
	return 0
}

func Int64(data interface{}) (int64, error) {
	switch data.(type) {
	case string:
		i, err := strconv.ParseInt(data.(string), 10, 64)
		return int64(i), err
	case float32, float64:
		return int64(reflect.ValueOf(data).Float()), nil
	case int, int8, int16, int32, int64:
		return int64(reflect.ValueOf(data).Int()), nil
	case uint, uint8, uint16, uint32, uint64:
		return int64(reflect.ValueOf(data).Uint()), nil
	}
	return 0, errors.New("invalid value convert")
}

func MustInt64(data interface{}, defaultValue ...int64) int64 {
	if s, ok := Int64(data); ok == nil {
		return s
	}
	if len(defaultValue) == 1 {
		return defaultValue[0]
	}
	return 0
}

func Uint(data interface{}) (uint, error) {
	switch data.(type) {
	case string:
		i, err := strconv.ParseUint(data.(string), 10, 64)
		return uint(i), err
	case float32, float64:
		return uint(reflect.ValueOf(data).Float()), nil
	case int, int8, int16, int32, int64:
		return uint(reflect.ValueOf(data).Int()), nil
	case uint, uint8, uint16, uint32, uint64:
		return uint(reflect.ValueOf(data).Uint()), nil
	}
	return 0, errors.New("invalid value convert")
}

func MustUint(data interface{}, defaultValue ...uint) uint {
	if s, ok := Uint(data); ok == nil {
		return s
	}
	if len(defaultValue) == 1 {
		return defaultValue[0]
	}
	return 0
}

func Uint8(data interface{}) (uint8, error) {
	switch data.(type) {
	case string:
		i, err := strconv.ParseUint(data.(string), 10, 64)
		return uint8(i), err
	case float32, float64:
		return uint8(reflect.ValueOf(data).Float()), nil
	case int, int8, int16, int32, int64:
		return uint8(reflect.ValueOf(data).Int()), nil
	case uint, uint8, uint16, uint32, uint64:
		return uint8(reflect.ValueOf(data).Uint()), nil
	}
	return 0, errors.New("invalid value convert")
}

func MustUint8(data interface{}, defaultValue ...uint8) uint8 {
	if s, ok := Uint8(data); ok == nil {
		return s
	}
	if len(defaultValue) == 1 {
		return defaultValue[0]
	}
	return 0
}

func Uint16(data interface{}) (uint16, error) {
	switch data.(type) {
	case string:
		i, err := strconv.ParseUint(data.(string), 10, 64)
		return uint16(i), err
	case float32, float64:
		return uint16(reflect.ValueOf(data).Float()), nil
	case int, int8, int16, int32, int64:
		return uint16(reflect.ValueOf(data).Int()), nil
	case uint, uint8, uint16, uint32, uint64:
		return uint16(reflect.ValueOf(data).Uint()), nil
	}
	return 0, errors.New("invalid value convert")
}

func MustUint16(data interface{}, defaultValue ...uint16) uint16 {
	if s, ok := Uint16(data); ok == nil {
		return s
	}
	if len(defaultValue) == 1 {
		return defaultValue[0]
	}
	return 0
}

func Uint32(data interface{}) (uint32, error) {
	switch data.(type) {
	case string:
		i, err := strconv.ParseUint(data.(string), 10, 64)
		return uint32(i), err
	case float32, float64:
		return uint32(reflect.ValueOf(data).Float()), nil
	case int, int8, int16, int32, int64:
		return uint32(reflect.ValueOf(data).Int()), nil
	case uint, uint8, uint16, uint32, uint64:
		return uint32(reflect.ValueOf(data).Uint()), nil
	}
	return 0, errors.New("invalid value convert")
}

func MustUint32(data interface{}, defaultValue ...uint32) uint32 {
	if s, ok := Uint32(data); ok == nil {
		return s
	}
	if len(defaultValue) == 1 {
		return defaultValue[0]
	}
	return 0
}

func Uint64(data interface{}) (uint64, error) {
	switch data.(type) {
	case string:
		i, err := strconv.ParseUint(data.(string), 10, 64)
		return uint64(i), err
	case float32, float64:
		return uint64(reflect.ValueOf(data).Float()), nil
	case int, int8, int16, int32, int64:
		return uint64(reflect.ValueOf(data).Int()), nil
	case uint, uint8, uint16, uint32, uint64:
		return uint64(reflect.ValueOf(data).Uint()), nil
	}
	return 0, errors.New("invalid value convert")
}

func MustUint64(data interface{}, defaultValue ...uint64) uint64 {
	if s, ok := Uint64(data); ok == nil {
		return s
	}
	if len(defaultValue) == 1 {
		return defaultValue[0]
	}
	return 0
}

func Float32(data interface{}) (float32, error) {
	switch data.(type) {
	case string:
		res, err := strconv.ParseFloat(data.(string), 64)
		return float32(res), err
	case float32, float64:
		return float32(reflect.ValueOf(data).Float()), nil
	case int, int8, int16, int32, int64:
		return float32(reflect.ValueOf(data).Int()), nil
	case uint, uint8, uint16, uint32, uint64:
		return float32(reflect.ValueOf(data).Uint()), nil
	}
	return 0, errors.New("invalid value convert")
}

func MustFloat32(data interface{}, defaultValue ...float32) float32 {
	if s, ok := Float32(data); ok == nil {
		return s
	}
	if len(defaultValue) == 1 {
		return defaultValue[0]
	}
	return 0
}

func Float64(data interface{}) (float64, error) {
	switch data.(type) {
	case string:
		return strconv.ParseFloat(data.(string), 64)
	case float32, float64:
		return reflect.ValueOf(data).Float(), nil
	case int, int8, int16, int32, int64:
		return float64(reflect.ValueOf(data).Int()), nil
	case uint, uint8, uint16, uint32, uint64:
		return float64(reflect.ValueOf(data).Uint()), nil
	}
	return 0, errors.New("invalid value convert")
}

func MustFloat64(data interface{}, defaultValue ...float64) float64 {
	if s, ok := Float64(data); ok == nil {
		return s
	}
	if len(defaultValue) == 1 {
		return defaultValue[0]
	}
	return 0
}

func Bool(data interface{}) (bool, error) {
	if s, ok := data.(bool); ok {
		return s, nil
	}
	return false, errors.New("invalid value convert")
}

func MustBool(data interface{}, defaultValue ...bool) bool {
	if s, ok := Bool(data); ok == nil {
		return s
	}
	if len(defaultValue) == 1 {
		return defaultValue[0]
	}
	return false
}

func String(data interface{}) (string, error) {
	return fmt.Sprintf("%v", data), nil
	if s, ok := data.(string); ok {
		return s, nil
	}
	return "", errors.New("invalid value convert")
}
func MustString(data interface{}, defaultValue ...string) string {
	if s, ok := String(data); ok == nil {
		return s
	}
	if len(defaultValue) == 1 {
		return defaultValue[0]
	}
	return ""
}

func Bytes(data interface{}) ([]byte, error) {
	if s, ok := data.(string); ok {
		return []byte(s), nil
	}
	return nil, errors.New("invalid value convert")
}

func Slice(data interface{}) ([]interface{}, error) {
	if a, ok := data.([]interface{}); ok {
		return a, nil
	}
	return nil, errors.New("invalid value convert")
}

func StringSlice(data interface{}) ([]string, error) {
	arr, err := Slice(data)
	if err != nil {
		return nil, err
	}
	retArr := make([]string, 0, len(arr))
	for _, a := range arr {
		if a == nil {
			retArr = append(retArr, "")
			continue
		}
		s, ok := a.(string)
		if !ok {
			return nil, errors.New("invalid value convert")
		}
		retArr = append(retArr, s)
	}
	return retArr, nil
}

func MustStringSlice(data interface{}, defaultValue ...[]string) []string {
	if s, ok := StringSlice(data); ok == nil {
		return s
	}
	if len(defaultValue) == 1 {
		return defaultValue[0]
	}
	return nil
}
