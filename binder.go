package orm

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"maps"
	"reflect"
	"unsafe"
)

// makePointers creates a map of field names to pointers for a given struct value.
// It recursively traverses the struct, creating pointers for each field.
func (b *binder) makePointers(v reflect.Value) any {
	m := make(map[string]any)
	actualV := v
	for actualV.Type().Kind() == reflect.Ptr {
		actualV = actualV.Elem()
	}

	if actualV.Type().Kind() == reflect.Struct {
		for i := 0; i < actualV.NumField(); i++ {
			f := actualV.Field(i)
			if (f.Type().Kind() == reflect.Struct || f.Type().Kind() == reflect.Ptr) && !f.Type().Implements(reflect.TypeOf((*driver.Valuer)(nil)).Elem()) {
				f = reflect.NewAt(actualV.Type().Field(i).Type, unsafe.Pointer(actualV.Field(i).UnsafeAddr()))
				fm := b.makePointers(f).(map[string]any)
				maps.Copy(m, fm)
			} else {
				var fm *field
				fm = b.s.getField(actualV.Type().Field(i))
				if fm == nil {
					fm = fieldMetadata(actualV.Type().Field(i), b.s.columnConstraints)[0]
				}

				m[fm.Name] = reflect.NewAt(actualV.Field(i).Type(), unsafe.Pointer(actualV.Field(i).UnsafeAddr())).Interface()
			}
		}
	} else {
		return v.Addr().Interface()
	}

	return m
}

// getScanDestinations returns a slice of pointers to the fields of a struct value,
// ordered according to the columns in the sql.Rows.
func (b *binder) getScanDestinations(v reflect.Value, columns []*sql.ColumnType) []any {
	pointers := b.makePointers(v)
	scanDestinations := make([]any, 0, len(columns))

	if reflect.TypeOf(pointers).Kind() == reflect.Map {
		nameToPtr := pointers.(map[string]any)
		for _, column := range columns {
			if ptr, ok := nameToPtr[column.Name()]; ok {
				scanDestinations = append(scanDestinations, ptr)
			}
		}
	} else {
		scanDestinations = append(scanDestinations, pointers)
	}

	return scanDestinations
}

type binder struct {
	s *schema
}

func newBinder(s *schema) *binder {
	return &binder{s: s}
}

// bind scans the rows from a database query into a Go struct or slice of structs.
func (b *binder) bind(rows *sql.Rows, destination any) error {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	destType := reflect.TypeOf(destination)
	destValue := reflect.ValueOf(destination)

	if destType.Kind() != reflect.Ptr {
		return fmt.Errorf("destination must be a pointer")
	}

	destType = destType.Elem()
	destValue = destValue.Elem()

	if destType.Kind() == reflect.Slice {
		sliceType := destType.Elem()
		for rows.Next() {
			newElem := reflect.New(sliceType).Elem()
			for newElem.Type().Kind() == reflect.Ptr {
				newElem = reflect.New(newElem.Type().Elem()).Elem()
			}

			scanDestinations := b.getScanDestinations(newElem, columnTypes)
			if err := rows.Scan(scanDestinations...); err != nil {
				return err
			}

			for newElem.Type() != sliceType {
				tmp := reflect.New(newElem.Type())
				tmp.Elem().Set(newElem)
				newElem = tmp
			}
			destValue = reflect.Append(destValue, newElem)
		}
	} else {
		if rows.Next() {
			scanDestinations := b.getScanDestinations(destValue, columnTypes)
			if err := rows.Scan(scanDestinations...); err != nil {
				return err
			}
		}
	}

	reflect.ValueOf(destination).Elem().Set(destValue)
	return nil
}
