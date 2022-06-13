package schema

import (
	"fmt"
	"reflect"
	"strings"
)

type CanTableName interface {
	TableName() string
}

func TypeOfModel(model any) (reflect.Type, error) {
	t := reflect.TypeOf(model)

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() == reflect.Slice {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("table model must be struct type, but got %T", model)
	}

	return t, nil
}

func TableSchemaFor(model interface{}) (*TableSchema, error) {
	tpe, err := TypeOfModel(model)
	if err != nil {
		return nil, err
	}

	ts := &TableSchema{
		Type: tpe,
	}

	return ts, nil
}

type TableSchema struct {
	PKey
	Name         string                  `msgp:"name"`
	Type         reflect.Type            `msgp:"-"`
	IndexSchemas map[string]*IndexSchema `msgp:"-"`
}

func (s *TableSchema) Init() error {
	m := reflect.New(s.Type).Interface()

	if canTableName, ok := m.(CanTableName); ok {
		s.Name = canTableName.TableName()
	} else {
		s.Name = s.Type.Name()
	}

	if canIndexes, ok := m.(CanIndexes); ok {
		if s.IndexSchemas == nil {
			s.IndexSchemas = map[string]*IndexSchema{}
		}

		for name, indexType := range canIndexes.Indexes() {
			is := &IndexSchema{
				IndexType: indexType,
			}

			parts := strings.Split(name, ",")

			is.Paths = make([]KeyPath, len(parts))
			for i := range is.Paths {
				keyPath, err := ParseKeyPath(parts[i])
				if err != nil {
					return err
				}
				is.Paths[i] = keyPath
			}

			s.IndexSchemas[name] = is
		}
	}

	return nil
}

func (s *TableSchema) String() string {
	return fmt.Sprintf("%s (%s)", s.Name, s.Type)
}

func (s *TableSchema) IsEqual(t *TableSchema) bool {
	if !(s.Type.PkgPath() == t.Type.PkgPath() && s.Type.Name() == t.Type.Name()) {
		return false
	}
	return true
}

func (s *TableSchema) IndexSchema(name string) *IndexSchema {
	if len(s.IndexSchemas) == 0 {
		return nil
	}
	is, _ := s.IndexSchemas[name]
	return is
}

func (TableSchema) Indexes() map[string]IndexType {
	return map[string]IndexType{
		"name": UniqueIndex,
	}
}
