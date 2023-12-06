package http

import (
	"github.com/Darker-D/ddbase/encoding/json"
	"encoding/base64"
	"github.com/pkg/errors"
	"strings"
)

type DeviceType string

const (
	AndroidDT   DeviceType = "android"
	IOSDT       DeviceType = "ios"
	WebAppDT    DeviceType = "webapp"
	WeChatAppDT DeviceType = "wechatapp"
	EmptyDT     DeviceType = ""
)

func (dt DeviceType) String() string {
	switch dt {
	case AndroidDT:
		return "android"
	case IOSDT:
		return "ios"
	case WebAppDT:
		return "webapp"
	case WeChatAppDT:
		return "wechatapp"
	default:
		return ""
	}
}

func (ua *UserAgent) SetDeviceType(dt string) {
	switch dt {
	case "android":
		ua.DeviceType = AndroidDT
	case "ios":
		ua.DeviceType = IOSDT
	case "webapp":
		ua.DeviceType = WebAppDT
	case "wechatapp":
		ua.DeviceType = WeChatAppDT
	default:
		ua.DeviceType = EmptyDT
	}
}

// ConvertAgentId
func (dt DeviceType) ConvertAgentId() int {
	switch dt {
	case AndroidDT:
		return 12
	case IOSDT:
		return 11
	case WebAppDT:
		return 14
	case WeChatAppDT:
		return 149
	default:
		return 0
	}
}

//HeaderContent json
type HeaderContent struct {
	LocLat         string `json:"loc_lat"`   // 纬度
	LocLng         string `json:"loc_lng"`   //经度
	LocType        string `json:"loc_type"`  //经纬度类型
	LocSpeed       string `json:"loc_speed"` //速度
	ImeiUUID       string `json:"imei_uuid"` //imei_uuid
	NetType        string `json:"net_type"`  //2G/3G/4G/wifi
	DeviceNo       string `json:"device_no"` // 安卓端通过阿里的库获取的一个安全性更高的 number ,日志使用
	UserID         string `json:"user_id"`
	CityID         string `json:"city_id"`
	TimeStampUnix  string `json:"timestamp_unix"`
	AppTimeoutMs   string `json:"app_timeout_ms"` // key:app_timeout_ms value:30000
	Imsi           string `json:"imsi"`
	Mac            string `json:"mac"`
	MobileMerchant string `json:"mobileMerchant"`
}

const (
	// header key.
	YgHeaderHintContent string = "Yg-Header-Hint-Content"
)

// Key HeaderContent.
func (hc *HeaderContent) Key() string {
	return YgHeaderHintContent
}

//Unmarshal 解析 header-content
func YgHeaderUnmarshal(base64AppHeader string) (*HeaderContent, error) {
	var hc = new(HeaderContent)
	decodedBytes, err := base64.StdEncoding.DecodeString(base64AppHeader)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(decodedBytes, hc)
	return hc, err
}

var (
	SlashErr           = errors.New("slash解析失败")
	UnknownTerminal    = errors.New("terminal解析失败")
	LeftParenthesisErr = errors.New("leftParenthesis解析失败")
	SemicolonErr       = errors.New("semicolon解析失败")
	// Useragent 合法头
	terminal = map[string]bool{"YGPassenger": true, "YGDriver": true, "YGDTaxi": true, "YGGuider": true, "YGSmallProgram": true}
)

//UserAgent 用户代理
type UserAgent struct {
	Client        string
	AppVersion    string
	DeviceType    DeviceType
	SystemVersion string
	DeviceName    string
	Channel       string
	AgentID       int //旧渠道编号
}

//DecodeUA 解析 User-Agent
func YgUAUnmarshal(userAgent string) (*UserAgent, error) {
	var (
		ua                                        = new(UserAgent)
		slash                                     []string // '/' 分割结果
		leftSlash, rightSlash                     string
		leftParenthesis                           []string // （ 分割结果
		leftLeftParenthesis, rightLeftParenthesis string
		semicolon                                 []string // ; 分割结果
	)

	if strings.Count(userAgent, "(") > 1 {
		slash = strings.SplitN(userAgent, "/", 2)
	} else {
		slash = strings.Split(userAgent, "/")
	}

	if len(slash) <= 1 {
		return ua, SlashErr
	}
	leftSlash, rightSlash = slash[0], slash[1]

	if !terminal[leftSlash] {
		return ua, UnknownTerminal
	}

	ua.Client = leftSlash

	if rightSlash[len(rightSlash)-1] == ')' {
		rightSlash = rightSlash[:len(rightSlash)-1]
	}

	if strings.Count(rightSlash, "(") > 1 {
		leftParenthesis = strings.SplitN(rightSlash, "(", 2)
	} else {
		leftParenthesis = strings.Split(rightSlash, "(")
	}

	if len(leftParenthesis) < 1 {
		return ua, LeftParenthesisErr
	}

	leftLeftParenthesis, rightLeftParenthesis = leftParenthesis[0], leftParenthesis[1]

	ua.AppVersion = leftLeftParenthesis

	if strings.Count(rightLeftParenthesis, ";") < 3 {
		return ua, SemicolonErr
	}

	semicolon = strings.SplitN(rightLeftParenthesis, ";", 4)

	ua.SetDeviceType(strings.ToLower(semicolon[0])) // e.g. 终端 Android/iOS/WebApp/WechatApp
	ua.SystemVersion = semicolon[1]                 // e.g. 系统版本号 android7.0/ios11.3.1/chrome12.1/Safari9.1
	ua.DeviceName = semicolon[2]                    // e.g. 设备名称 huaweiP9/oppoR15/IphoneX
	ua.Channel = semicolon[3]                       // 渠道号
	ua.AgentID = ua.DeviceType.ConvertAgentId()
	return ua, nil
}
