package helpers

import (
	"fmt"
	"reflect"
)

func RemoveDuplicate(data interface{}, equal func(i, j int) bool) (result interface{}, err error) {
	v := reflect.ValueOf(data)

	switch v.Kind() {
	case reflect.Slice:
	default:
		return nil, fmt.Errorf(`dat参数必须是slice,实际是[%s]`, v.Kind().String())
	}

	DataLen := v.Len()
	if DataLen == 0 {
		return data, nil
	}

	reflectValue := reflect.MakeSlice(reflect.SliceOf(v.Index(0).Type()), DataLen, DataLen)

	j := 0

	for i := 0; i < DataLen; i++ {
		found := removeDuplicate(j, i, equal)

		if !found {
			reflectValue.Index(j).Set(v.Index(i))
			j++
		}
	}

	reflectValue = reflectValue.Slice(0, j)

	return reflectValue.Interface(), nil
}

func removeDuplicate(j, i int, equal func(i int, j int) bool) bool {
	found := false

	for index := 0; index < j; index++ {
		if i == index {
			continue
		}

		found = equal(index, i)

		if found {
			break
		}
	}

	return found
}
