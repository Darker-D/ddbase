package middleware

import (
	"bytes"
	"github.com/Darker-D/ddbase/ecode"
	"github.com/Darker-D/ddbase/net/http"
	"github.com/Darker-D/ddbase/net/http/sign"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"

	"github.com/gin-gonic/gin"
)

func Sign(c *sign.Config) gin.HandlerFunc {
	s := sign.New(c)
	return func(c *gin.Context) {
		params := make(map[string]string)
		form := c.Request.Form
		for k, v := range form {
			if len(v) > 0 {
				params[k] = v[0]
			}
		}
		err := parseBody(c, params)
		if err != nil {
			http.JSON(c, nil, ecode.ServerErr)
			c.Abort()
			return
		}

		if s := s.GenSign(params); s != params["sign"] {
			http.JSON(c, nil, ecode.Unauthorized)
			c.Abort()
			return
		}
		c.Next()
	}
}

func parseBody(c *gin.Context, params map[string]string) error {
	bm := map[string]string{}
	bmi := map[string]interface{}{}
	body := c.Request.Body
	if body == nil {
		return nil
	}
	bodyBytes, err := ioutil.ReadAll(body)
	defer body.Close()
	if err != nil {
		return err
	}
	c.Request.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
	if len(bodyBytes) == 0 {
		return nil
	}

	err = json.Unmarshal(bodyBytes, &bmi)
	bm = mapStringify(bmi)

	for k, v := range bm {
		params[k] = v
	}
	return err
}

// map[string]interface{} 转为 map[string]string
func mapStringify(m map[string]interface{}) map[string]string {
	ret := make(map[string]string, len(m))
	for k, v := range m {

		switch reflect.TypeOf(v).Kind() {
		case reflect.Array, reflect.Map, reflect.Slice:
			vBytes, _ := json.Marshal(v)
			ret[k] = string(vBytes)
		default:
			ret[k] = fmt.Sprint(v)
		}
	}
	return ret
}
