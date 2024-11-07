package jsonpath

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/longbridgeapp/assert"
)

func TestParseJsonArrayIndex(t *testing.T) {
	key, idx, all, isArray := parseArrayIndex("[*]")
	assert.Equal(t, "", key, "Key should be an empty string")
	assert.Equal(t, -1, idx, "Index should be -1")
	assert.True(t, all, "All should be true for wildcard")
	assert.True(t, isArray, "isArray should be true for array syntax")
}

func TestSet(t *testing.T) {
	data := map[string]interface{}{
		"A": map[string]interface{}{
			"B": map[string]interface{}{
				"C": 1,
			},
		},
	}
	path := "A.B.C"
	Set(data, path, 2)
	value := Get(data, "A.B.D")
	assert.Equal(t, 2, value, "result should be 2")
}

func TestGet(t *testing.T) {
	raw := []byte(`
	{
  "A": [
    {
      "B": [
        {
          "C": 1,
          "D": 1,
          "E": "1,1",
          "F": [
            {"G": 10, "H": "H1", "I": true, "Z": "10,H1"},
            {"G": 20, "H": "H2", "I": false, "Z": "20,H2"}
          ]
        },
        {
          "C": 2,
          "D": 2,
          "E": "2,2",
          "F": [
            {"G": 30, "H": "H3", "I": true, "Z": "30,H3"},
            {"G": 40, "H": "H4", "I": false, "Z": "40,H4"}
          ],
          "J": {
            "K": "NestedString",
            "L": [1, 2, 3, {"M": "MValue", "N": [10, 20]}]
          }
        }
      ],
      "B1": [
        {
          "C": 11,
          "D": 11,
          "E": "11,11",
          "F": [
            {"G": 110, "H": "H11", "I": true, "Z": "110,H11"},
            {"G": 120, "H": "H12", "I": false, "Z": "120,H12"}
          ]
        },
        {
          "C": 12,
          "D": 12,
          "E": "12,12",
          "F": [
            {"G": 130, "H": "H13", "I": true, "Z": "130,H13"},
            {"G": 140, "H": "H14", "I": false, "Z": "140,H14"}
          ],
          "J": {
            "K": "NestedB1String",
            "L": [7, 8, {"M": "MB1Value", "N": [110, 120]}]
          }
        }
      ]
    },
    {
      "B": [
        {
          "C": 3,
          "D": 3,
          "E": "3,3",
          "F": [
            {"G": 50, "H": "H5", "I": true, "Z": "50,H5"},
            {"G": 60, "H": "H6", "I": false, "Z": "60,H6"}
          ],
          "J": {
            "K": "AnotherString",
            "L": [4, 5, {"M": "AnotherM", "N": [30, 40]}]
          }
        },
        {
          "C": 4,
          "D": 4,
          "E": "4,4",
          "F": [
            {"G": 70, "H": "H7", "I": true, "Z": "70,H7"},
            {"G": 80, "H": "H8", "I": false, "Z": "80,H8"}
          ],
          "J": {
            "K": "FinalString",
            "L": [6, {"M": "FinalM", "N": [50, 60]}, {"M": "ExtraM", "O": {"P": 100, "Q": "NestedEnd"}}]
          }
        }
      ],
      "B1": [
        {
          "C": 13,
          "D": 13,
          "E": "13,13",
          "F": [
            {"G": 150, "H": "H15", "I": true, "Z": "150,H15"},
            {"G": 160, "H": "H16", "I": false, "Z": "160,H16"}
          ],
          "J": {
            "K": "AnotherB1String",
            "L": [9, 10, {"M": "AnotherMB1", "N": [130, 140]}]
          }
        },
        {
          "C": 14,
          "D": 14,
          "E": "14,14",
          "F": [
            {"G": 170, "H": "H17", "I": true, "Z": "170,H17"},
            {"G": 180, "H": "H18", "I": false, "Z": "180,H18"}
          ],
          "J": {
            "K": "FinalB1String",
            "L": [11, {"M": "FinalMB1", "N": [150, 160]}, {"M": "ExtraMB1", "O": {"P": 200, "Q": "NestedEndB1"}}]
          }
        }
      ]
    }
  ]
}`)
	data := map[string]interface{}{}
	json.Unmarshal(raw, &data)
	InsertField(data, []string{
		"A[*].B[*].F[*].G",
		"A[*].B1[*].F[*].Z",
	}, "A[*].B[*].F[*].T", ",")
	raw, _ = json.Marshal(data)
	fmt.Println(string(raw))
}
