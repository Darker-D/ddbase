package customtime

import (
	"time"

	ut "github.com/go-playground/universal-translator"
	"gopkg.in/go-playground/validator.v9"

	xtime "github.com/Darker-D/ddbase/time"
)

// ValidateTime 验证是否是时间
func ValidateTime(fl validator.FieldLevel) bool {
	_, err := time.ParseInLocation(xtime.LayoutTime, fl.Field().String(), xtime.TimeLocal)
	if err != nil {
		return false
	}
	return true
}

// ValidateTimeTranslator 翻译
func ValidateTimeTranslator(ut ut.Translator) (err error) {
	return ut.Add("time", "{0} 时间格式错误", true)
}

// ValidateTimeHM 验证是否是时间 eg. 13:09
func ValidateTimeHM(fl validator.FieldLevel) bool {
	_, err := time.ParseInLocation(xtime.LayoutHM, fl.Field().String(), xtime.TimeLocal)
	if err != nil {
		return false
	}
	return true
}

// ValidateTimeHMTranslator 翻译
func ValidateTimeHMTranslator(ut ut.Translator) (err error) {
	return ut.Add("hour", "{0} 时间格式错误", true)
}
