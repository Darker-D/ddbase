package time

import (
	"context"
	"database/sql/driver"
	"errors"
	"strconv"
	xt "time"

	"github.com/Darker-D/ddbase/encoding/json"
)

// 时间相关
const (
	LayoutH    = "15"
	LayoutHM   = "15:04"
	LayoutYMD  = "2006-01-02"
	LayoutTime = "2006-01-02 15:04:05"

	LayoutUNYMD  = "20060102"   // 无分隔符
	LayoutUNYMDH = "2006010215" // 无分隔符
)

// TimeFormat 格式时间
func TimeFormat(t xt.Time, layout string) string {
	if t.IsZero() {
		return ""
	}
	if layout == "" {
		layout = LayoutTime
	}
	return t.Format(layout)
}

// TimeLocal 本地时区
var TimeLocal *xt.Location

func init() {
	TimeLocal, _ = xt.LoadLocation("Local")
}

// Time be used to MySql timestamp converting.
type Time int64

// Scan scan time.
func (t *Time) Scan(src interface{}) (err error) {
	switch sc := src.(type) {
	case xt.Time:
		*t = Time(sc.Unix())
	case string:
		var i int64
		i, err = strconv.ParseInt(sc, 10, 64)
		*t = Time(i)
	}
	return
}

// Value get time.
func (t Time) Value() (driver.Value, error) {
	return xt.Unix(int64(t), 0), nil
}

// Time get time.
func (t Time) Time() xt.Time {
	return xt.Unix(int64(t), 0)
}

// Duration be used toml unmarshal string time, like 1s, 500ms.
type Duration xt.Duration

// UnmarshalText unmarshal text to duration.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		*d = Duration(xt.Duration(value))
		return nil
	case string:
		t, err := xt.ParseDuration(value)
		if err != nil {
			return err
		}
		*d = Duration(t)
		return nil
	default:
		return errors.New("invalid duration")
	}
}

// Shrink will decrease the duration by comparing with context's timeout duration
// and return new timeout\context\CancelFunc.
func (d Duration) Shrink(c context.Context) (Duration, context.Context, context.CancelFunc) {
	if deadline, ok := c.Deadline(); ok {
		if ctimeout := xt.Until(deadline); ctimeout < xt.Duration(d) {
			// deliver small timeout
			return Duration(ctimeout), c, func() {}
		}
	}
	ctx, cancel := context.WithTimeout(c, xt.Duration(d))
	return d, ctx, cancel
}
