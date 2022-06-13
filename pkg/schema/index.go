package schema

type CanPrimaryKey interface {
	PrimaryKey() uint64
	SetPrimaryKey(id uint64)
}

type CanIndexes interface {
	Indexes() map[string]IndexType
}

type IndexType int

const (
	Index IndexType = iota
	UniqueIndex
)

type IndexSchema struct {
	PKey
	Owner     SFID      `msgp:"owner"`
	IndexType IndexType `msgp:"type"`
	Paths     []KeyPath `msgp:"paths"`
}

func (s *IndexSchema) Indexes() map[string]IndexType {
	return map[string]IndexType{
		"owner": Index,
	}
}
