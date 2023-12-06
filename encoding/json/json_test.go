package json

import (
	jsonx "encoding/json"
	"testing"
)

func TestJson(t *testing.T) {
	var arg = `{"code":10000,"message":"success","data":{"total_distance":0,"total_duration":2764800,"stream":null}}`

	m := make(map[string]interface{})

	err := Unmarshal([]byte(arg), &m)

	t.Log(err)
	t.Log(m)
	b, err := Marshal(m)
	t.Log(string(b))

	bb, err := jsonx.Marshal(m)
	t.Log(string(bb))

}
