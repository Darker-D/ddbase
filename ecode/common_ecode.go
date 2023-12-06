package ecode

// All common ecode
var (
	OK = add(10000) // 正确

	RequestErr            = add(10400) // 请求错误
	Unauthorized          = add(10401) // 未认证
	AccessDenied          = add(10403) // 访问权限不足
	NothingFound          = add(10404) // 啥都木有
	MethodNotAllowed      = add(10405) // 不支持该方法
	Conflict              = add(10409) // 冲突
	ServerErr             = add(10500) // 服务器错误
	ServiceUnavailable    = add(10503) // 过载保护,服务暂不可用
	Deadline              = add(10504) // 服务调用超时
	LimitExceed           = add(10509) // 超出限制
	FileNotExists         = add(10601) // 上传文件不存在
	FileTooLarge          = add(10602) // 上传文件太大
	FailedTooManyTimes    = add(10610) // 登录失败次数太多
	UserNotExist          = add(10611) // 用户不存在
	UsernameOrPasswordErr = add(10612) // 用户名或密码错误
	AccessTokenExpires    = add(10613) // Token 过期
	PasswordHashExpires   = add(10614) // 密码时间戳过期
	YgHeaderErr           = add(10615) // ygHeader解析错误
	YgUserAgentErr        = add(10616) // ygUserAgent解析错误
)

func init() {
	Register(map[int]string{
		10000: "SUCCESS",
		10400: "请求错误",
		10401: "未认证",
		10403: "访问权限不足",
		10404: "啥都木有",
		10405: "不支持该方法",
		10409: "冲突",
		10500: "服务器错误",
		10503: "过载保护,服务暂不可用",
		10504: "服务调用超时",
		10509: "超出限制",
		10601: "上传文件不存在",
		10602: "上传文件太大",
		10610: "登录失败次数太多",
		10611: "用户不存在",
		10612: "用户名或密码错误",
		10613: "Token 过期",
		10614: "密码时间戳过期",
		10615: "无效的请求头",
		10616: "无效的用户代理",
		10501: "内部服务器错误",

		10100: "xxxx",
		10101: "xxxx",
		10102: "内部服务调用错误",
	})
}
