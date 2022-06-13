package schema

var _ CanPrimaryKey = &PKey{}

type PKey struct {
	ID SFID `msgp:"id" json:"id"`
}

func (p PKey) PrimaryKey() uint64 {
	return uint64(p.ID)
}

func (p *PKey) SetPrimaryKey(id uint64) {
	p.ID = SFID(id)
}
