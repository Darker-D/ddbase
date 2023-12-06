package sign

import "testing"

func TestGenSign(t *testing.T) {
	var arg = make(map[string]string)
	arg["name"] = "张三"
	arg["age"] = "15"
	arg["sex"] = ""
	sign := GenSign(arg, "secretKey")
	t.Log(sign)
}
