package database

import (
	"context"
	"sync"

	"github.com/octohelm/kiwidb/pkg/dberr"
	"github.com/octohelm/kiwidb/pkg/encoding/msgp"
	"github.com/octohelm/kiwidb/pkg/schema"
)

var (
	tsOfTableSchema *schema.TableSchema
	tsOfIndexSchema *schema.TableSchema
)

func init() {
	tsOfTableSchema, _ = schema.TableSchemaFor(&schema.TableSchema{
		PKey: schema.PKey{
			ID: 0,
		},
	})
	_ = tsOfTableSchema.Init()

	tsOfIndexSchema, _ = schema.TableSchemaFor(&schema.IndexSchema{
		PKey: schema.PKey{
			ID: 1,
		},
	})
	_ = tsOfIndexSchema.Init()
}

type catalog struct {
	tables sync.Map // map[reflect.Type]*schema.TableSchema
	db     *database
}

func (c *catalog) TableSchema(model any) (*schema.TableSchema, error) {
	ts, err := schema.TableSchemaFor(model)
	if err != nil {
		return nil, err
	}

	if stored, ok := c.tables.Load(ts.Type); ok {
		return stored.(*schema.TableSchema), nil
	}

	if err := ts.Init(); err != nil {
		return nil, err
	}

	if err := c.syncTable(context.Background(), ts); err != nil {
		return nil, err
	}

	c.tables.Store(ts.Type, ts)

	return ts, nil
}

func (c *catalog) syncTable(ctx context.Context, ts *schema.TableSchema) (err error) {
	tx := c.db.Begin()

	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	schemaTable, err := NewTable(tx, tsOfTableSchema)
	if err != nil {
		return err
	}

	tableNameIndex := NewIndex(tx, tsOfTableSchema.IndexSchema("name"))
	exists, tableIDKey, err := tableNameIndex.Exists(ctx, []any{ts.Name})
	if err != nil {
		return err
	}

	d := DocumentFrom(ts)

	if exists {
		tableID := tableIDKey.Values()[0].(uint64)
		d.SetPrimaryKey(tableID)
	}

	key, d, err := schemaTable.Insert(ctx, d)
	if err != nil {
		ce, conflict := dberr.IsConflictError(err)
		if !conflict {
			return err
		}
		if err := schemaTable.Replace(ctx, ce.Key, d); err != nil {
			return err
		}
	}

	// sync indexes
	for k := range tsOfTableSchema.IndexSchemas {
		is := tsOfTableSchema.IndexSchemas[k]
		idx := NewIndex(tx, is)

		values := make([]any, len(is.Paths))

		for i := range is.Paths {
			v, err := d.Field(is.Paths[i]...)
			if err != nil {
				return err
			}
			raw, _ := v.Marshal()
			values[i] = msgp.Encoded(raw)
		}

		if err := idx.Set(ctx, values, key); err != nil {
			return err
		}
	}

	return tx.Commit()
}
