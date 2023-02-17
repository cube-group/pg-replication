package adapter

type LsnAdapter interface {
	Close() error
	Set(key string, value uint64) error
	Get(key string) (uint64, error)
}
