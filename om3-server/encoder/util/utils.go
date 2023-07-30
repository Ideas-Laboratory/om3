package util

import "encoding/json"

func MarshalMap( obj map[string]interface{}) string {
	res,_:=json.Marshal(obj)
	return string(res)
}