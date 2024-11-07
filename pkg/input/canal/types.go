package canal

import (
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/go-mysql-org/go-mysql/schema"
)

func convertToUint64(value interface{}) (uint64, error) {
	switch v := value.(type) {
	case int:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case int32:
		return uint64(v), nil
	case uint:
		return uint64(v), nil
	case uint64:
		return v, nil
	case uint32:
		return uint64(v), nil
	case []byte:
		if len(v) > 8 {
			return 0, fmt.Errorf("byte slice too large to fit into uint64")
		}
		padded := make([]byte, 8-len(v), 8)
		padded = append(padded, v...)
		return binary.BigEndian.Uint64(padded), nil
	default:
		return 0, fmt.Errorf("unsupported type: %s", reflect.TypeOf(value).String())
	}
}

func convertToInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case []byte:
		if len(v) > 8 {
			return 0, fmt.Errorf("byte slice too large to fit into int64")
		}
		padded := make([]byte, 8-len(v), 8)
		padded = append(padded, v...)
		return int64(binary.BigEndian.Uint64(padded)), nil
	default:
		return 0, fmt.Errorf("unsupported type: %s", reflect.TypeOf(value).String())
	}
}
func convertValueByColumnType(column schema.TableColumn, value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	switch column.Type {
	case schema.TYPE_NUMBER: // tinyint, smallint, mediumint, int, bigint, year
		// 处理无符号数
		if column.IsUnsigned {
			ret, err := convertToUint64(value)
			if err != nil {
				return nil, fmt.Errorf("unsupported type for int conversion for column %s: %T", column.Name, value)
			}
			return ret, err
		} else {
			// 处理有符号数
			ret, err := convertToInt64(value)
			if err != nil {
				return nil, fmt.Errorf("unsupported type for int conversion for column %s: %T", column.Name, value)
			}
			return ret, err
		}

	case schema.TYPE_FLOAT: // float, double
		// 将数据转换为浮点类型
		if v, ok := value.(float64); ok {
			return v, nil
		}
		if v, ok := value.([]uint8); ok {
			return string(v), nil
		}
		return nil, fmt.Errorf("failed to convert value to float for column %s", column.Name)

	case schema.TYPE_ENUM: // enum
		// 枚举类型
		if v, ok := value.(string); ok {
			return v, nil
		}
		if v, ok := value.([]uint8); ok {
			return string(v), nil
		}
		return nil, fmt.Errorf("failed to convert value to enum for column %s", column.Name)

	case schema.TYPE_SET: // set
		// Set类型类似于Enum的处理
		if v, ok := value.(string); ok {
			return v, nil
		}
		if v, ok := value.([]uint8); ok {
			return string(v), nil
		}
		return nil, fmt.Errorf("failed to convert value to set for column %s", column.Name)

	case schema.TYPE_STRING: // varchar, text, blob, etc.
		// MySQL 字符串或 BLOB 会返回 []uint8，需要转换为 string
		if v, ok := value.([]uint8); ok {
			return string(v), nil
		}
		return nil, fmt.Errorf("failed to convert value to string for column %s", column.Name)

	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP, schema.TYPE_DATE, schema.TYPE_TIME: // datetime, timestamp, date, time
		// 时间类型的处理，通常是字符串形式
		if v, ok := value.(string); ok {
			return v, nil
		}
		if v, ok := value.([]uint8); ok {
			return string(v), nil
		}
		return nil, fmt.Errorf("failed to convert value to time format for column %s", column.Name)

	case schema.TYPE_BIT: // bit
		// 处理bit类型，通常返回[]uint8，需要处理成布尔或二进制数据
		if v, ok := value.([]uint8); ok {
			if len(v) == 1 {
				return v[0] != 0, nil // 单个bit处理成布尔值
			}
			return v, nil // 处理为字节数组
		}
		return nil, fmt.Errorf("failed to convert value to bit for column %s", column.Name)

	case schema.TYPE_JSON: // json
		// JSON类型，通常也是返回字符串
		if v, ok := value.([]uint8); ok {
			return string(v), nil
		}
		return nil, fmt.Errorf("failed to convert value to json for column %s", column.Name)

	case schema.TYPE_DECIMAL: // decimal
		// Decimal 数字类型，返回字符串表示
		if v, ok := value.(string); ok {
			return v, nil
		}
		if v, ok := value.([]uint8); ok {
			return string(v), nil
		}
		return nil, fmt.Errorf("failed to convert value to decimal for column %s", column.Name)

	default:
		// 处理未识别的类型，直接返回字符串形式
		return fmt.Sprintf("%v", value), nil
	}
}
