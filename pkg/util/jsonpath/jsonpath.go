package jsonpath

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/yalp/jsonpath"
)

func GetFromRaw(raw []byte, path string) (interface{}, error) {
	obj := map[string]interface{}{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, err
	}
	return jsonpath.Read(obj, path)
}

func Get(data map[string]interface{}, path string) interface{} {
	return getPath(data, strings.Split(path, "."))
}

func getPath(data map[string]interface{}, keys []string) interface{} {
	for i, key := range keys {
		field, idx, all, isArray := parseArrayIndex(key)
		var next interface{}
		if isArray {
			arr, ok := data[field].([]map[string]interface{})
			if !ok {
				return nil
			}
			if all {
				results := []interface{}{}
				for _, item := range arr {
					if result := getPath(item, keys[i+1:]); result != nil {
						results = append(results, result)
					}
				}
				return results
			}
			if idx >= len(arr) {
				return nil
			}
			next = arr[idx]
		} else {
			next = data[field]
		}
		if i == len(keys)-1 {
			return next
		} else {
			if value, ok := next.(map[string]interface{}); ok {
				data = value
			} else {
				return nil
			}
		}
	}
	return nil
}

func Rename(data map[string]interface{}, path, to string) error {
	value := Get(data, path)
	if value != nil {
		if err := Delete(data, path); err != nil {
			return fmt.Errorf("failed to delete path '%s': %w", path, err)
		}

		if err := Set(data, to, value); err != nil {
			return fmt.Errorf("failed to set value at new path '%s': %w", to, err)
		}
	}

	return nil
}

func Delete(data map[string]interface{}, path string) error {
	keys := strings.Split(path, ".")
	return deletePath(data, keys)
}

func deletePath(data interface{}, keys []string) error {
	for i, key := range keys[:len(keys)-1] {
		field, idx, all, isArray := parseArrayIndex(key)
		if isArray {
			arr, ok := data.([]interface{})
			if !ok {
				return errors.New("expected array but found object")
			}
			if all {
				for _, item := range arr {
					if err := deletePath(item, keys[i:]); err != nil {
						return err
					}
				}
			} else if idx < len(arr) {
				data = arr[idx]
			} else {
				return fmt.Errorf("index %d out of bounds at '%s'", idx, key)
			}
		} else {
			obj, ok := data.(map[string]interface{})
			if !ok {
				return errors.New("expected object but found array")
			}
			data = obj[field]
		}
	}

	// Delete final key
	if obj, ok := data.(map[string]interface{}); ok {
		delete(obj, keys[len(keys)-1])
	}
	return nil
}

func Set(data map[string]interface{}, path string, value interface{}) error {
	keys := strings.Split(path, ".")
	return setPath(data, keys, value)
}

func setPath(data interface{}, keys []string, value interface{}) error {
	for i, key := range keys[:len(keys)-1] {
		field, idx, all, isArray := parseArrayIndex(key)
		if isArray {
			arr, ok := data.([]interface{})
			if !ok {
				return errors.New("expected array but found object")
			}
			if all {
				for _, item := range arr {
					if err := setPath(item, keys[i:], value); err != nil {
						return err
					}
				}
				return nil
			}
			if idx >= len(arr) {
				return fmt.Errorf("index %d out of bounds at '%s'", idx, key)
			}
			data = arr[idx]
		} else {
			obj, ok := data.(map[string]interface{})
			if !ok {
				return fmt.Errorf("expected object for key :%s ", key)
			}
			if _, exists := obj[field]; !exists {
				obj[field] = map[string]interface{}{}
			}
			data = obj[field]
		}
	}

	if obj, ok := data.(map[string]interface{}); ok {
		obj[keys[len(keys)-1]] = value
	}
	return nil
}

func InsertField(data map[string]interface{}, fromPaths []string, toPath, join string) error {
	rawValues := make([]string, len(fromPaths))
	arrFromPaths := map[int][]string{}
	count := strings.Count(toPath, "[*]")

	if count > 0 {
		for idx, path := range fromPaths {
			if strings.Count(path, "[*]") != count {
				return fmt.Errorf("模式不匹配")
			}
			arrFromPaths[idx] = strings.Split(path, ".")
		}
	} else {
		for idx, path := range fromPaths {
			val := getValue(data, strings.Split(path, "."))
			rawValues[idx] = fmt.Sprintf("%v", val)
		}
		setValue(data, strings.Split(toPath, "."), strings.Join(rawValues, join))
		return nil
	}

	toKeys := strings.Split(toPath, ".")
	fromValues := make([]interface{}, len(arrFromPaths))
	for idx := range fromValues {
		fromValues[idx] = data
	}
	insert(data, arrFromPaths, fromValues, toKeys, join)
	return nil
}

func insert(data map[string]interface{}, fromKeys map[int][]string, fromValues []interface{}, toKeys []string, join string) {

	rawValues := make([]string, len(fromKeys))

	if len(toKeys) > 1 {
		node, idx, all, remainKeys, ok := nextNode(data, toKeys)
		if !ok {
			return
		}
		switch arrayData := node.(type) {
		case []interface{}:
			processArray(arrayData, idx, all, fromKeys, fromValues, remainKeys, join, rawValues)
		case []map[string]interface{}:
			array := make([]interface{}, len(arrayData))
			for idx, item := range arrayData {
				array[idx] = item
			}
			processArray(array, idx, all, fromKeys, fromValues, remainKeys, join, rawValues)
		case map[string]interface{}:
			insert(arrayData, fromKeys, fromValues, remainKeys, join)
		}
	} else {
		for idx, keys := range fromKeys {
			rawValues[idx] = fmt.Sprintf("%v", getValue(fromValues[idx].(map[string]interface{}), keys))
		}
		data[toKeys[0]] = strings.Join(rawValues, join)
	}
}

func processArray(arrayData []interface{}, idx int, all bool, fromKeys map[int][]string, fromValues []interface{}, remainKeys []string, join string, rawValues []string) {
	start, end := idx, idx+1
	if all {
		start, end = 0, len(arrayData)
	}

	for i := start; i < end; i++ {
		elem, isMap := arrayData[i].(map[string]interface{})
		if !isMap {
			continue
		}

		updatedFrom := make([]interface{}, len(fromValues))
		updatedKeys := make(map[int][]string, len(fromKeys))
		copy(updatedFrom, fromValues)

		for idx, keys := range fromKeys {
			node, _, _, remainingKeys, ok := nextNode(fromValues[idx].(map[string]interface{}), keys)
			if !ok {
				rawValues[idx] = fmt.Sprintf("%v", node)
				continue
			}
			switch nextNode := node.(type) {
			case []interface{}:
				if i < len(nextNode) {
					if nextElem, isMap := nextNode[i].(map[string]interface{}); isMap {
						updatedFrom[idx] = nextElem
						updatedKeys[idx] = remainingKeys
					}
				}
			case []map[string]interface{}:
				if i < len(nextNode) {
					updatedFrom[idx] = nextNode[i]
					updatedKeys[idx] = remainingKeys
				}
			case map[string]interface{}:
				updatedFrom[idx] = nextNode
				updatedKeys[idx] = remainingKeys
			}
		}
		insert(elem, updatedKeys, updatedFrom, remainKeys, join)
	}
}

func getValue(data map[string]interface{}, keys []string) interface{} {
	for _, key := range keys {
		if nestedMap, isMap := data[key].(map[string]interface{}); isMap {
			data = nestedMap
		} else {
			return data[key]
		}
	}
	return data
}

func setValue(data map[string]interface{}, keys []string, value string) {
	for _, key := range keys[:len(keys)-1] {
		if _, exists := data[key]; !exists {
			data[key] = map[string]interface{}{}
		}
		data = data[key].(map[string]interface{})
	}
	data[keys[len(keys)-1]] = value
}

func nextNode(data map[string]interface{}, keys []string) (result interface{}, idx int, all bool, remainKeys []string, ok bool) {
	for i, key := range keys {
		field, idx, all, isArray := parseArrayIndex(key)
		if isArray {
			if next, exists := data[field]; exists {
				return next, idx, all, keys[i+1:], true
			}
			return nil, 0, false, nil, false
		}
		if i == len(keys)-1 {
			return data[key], 0, false, nil, true
		}
		nestedMap, isMap := data[key].(map[string]interface{})
		if !isMap {
			return nestedMap, 0, false, nil, false
		}
		data = nestedMap
	}
	return
}

func parseArrayIndex(key string) (field string, idx int, all, isArray bool) {
	if strings.Contains(key, "[*]") {
		field = strings.TrimSuffix(key, "[*]")
		return field, 0, true, true
	}
	if strings.Contains(key, "[") && strings.Contains(key, "]") {
		fmt.Sscanf(key, "%[^[][%d]", &field, &idx)
		return field, idx, false, true
	}
	return key, 0, false, false
}
