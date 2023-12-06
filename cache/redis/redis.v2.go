package redis

import (
	"bytes"
	"context"
	"encoding/gob"
	"reflect"
	"strings"
	"time"

	"fmt"
	"github.com/cznic/mathutil"
	"github.com/gomodule/redigo/redis"
)

type Config struct {
	Network  string
	Address  string
	Auth     string
	Password string
	DB       int
	// pool
	MaxActive       int           // 0无限制，给定时间内最大分配的连接数
	MaxIdle         int           // 最大空闲连接数
	IdleTimeout     time.Duration // 0不关闭连接，空闲连接时间
	MaxConnLifetime time.Duration // 0不限制，连接最大的生存时间
	// connect
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
}

type Client struct {
	sscanKeyLimit int         // 批量获取数量
	batchLimit    int         // 批量数量限制
	Pool          *redis.Pool // redis connection pool
}

func New(c *Config) *Client {
	pool := &redis.Pool{
		MaxActive:       c.MaxActive,
		MaxIdle:         c.MaxIdle,
		IdleTimeout:     c.IdleTimeout,
		MaxConnLifetime: c.MaxConnLifetime,

		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		Dial: func() (redis.Conn, error) {
			options := make([]redis.DialOption, 0)

			options = append(options, redis.DialDatabase(c.DB))
			options = append(options, redis.DialConnectTimeout(c.ConnectTimeout))
			options = append(options, redis.DialReadTimeout(c.ReadTimeout))
			options = append(options, redis.DialWriteTimeout(c.WriteTimeout))

			if c.Password != "" {
				pwdOptions := redis.DialPassword(c.Password)
				options = append(options, pwdOptions)
			}

			c, err := redis.Dial(c.Network, c.Address, options...)
			if err != nil {
				return nil, err
			}

			return c, err
		},
	}
	return &Client{
		sscanKeyLimit: 1000,
		batchLimit:    5000,
		Pool:          pool,
	}
}

// do base function
func (c *Client) do(ctx context.Context, action string, key string, val ...interface{}) (err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}
	args := []interface{}{key}
	args = append(args, val...)
	if _, err = conn.Do(strings.ToUpper(strings.TrimSpace(action)), args...); err != nil {
		return err
	}
	return nil
}

func (c *Client) Load(ctx context.Context, key string, val interface{}) (found bool, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return false, err
	}
	reply, err := conn.Do("GET", key)
	if err != nil {
		return false, err
	}
	if reply == nil {
		return false, nil // no reply was associated with this key
	}
	switch val.(type) {
	case *int, *uint, *int32, *uint32, *int64, *uint64:
		num, err := redis.Int64(reply, err)
		if err != nil {
			return false, err
		}
		rv := reflect.ValueOf(val)
		p := rv.Elem()
		p.SetInt(num)
	default:
		b, err := redis.Bytes(reply, err)
		if err != nil {
			return false, err
		}

		decoder := gob.NewDecoder(bytes.NewBuffer(b))
		err = decoder.Decode(val)
	}

	return true, err
}

func (c *Client) Store(ctx context.Context, key string, val interface{}) (err error) {
	var storeValue interface{}
	switch val.(type) {
	case int, uint, int32, uint32, int64, uint64:
		storeValue = val
	default:
		buf := new(bytes.Buffer)
		encoder := gob.NewEncoder(buf)
		err = encoder.Encode(val)
		if err != nil {
			return err
		}
		storeValue = buf.Bytes()
	}

	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return err
	}

	_, err = conn.Do("SET", key, storeValue)

	return err
}

func (c *Client) SetEx(ctx context.Context, key string, val interface{}, maxAge int) (err error) {
	var storeValue interface{}
	switch val.(type) {
	case int, uint, int32, uint32, int64, uint64, string:
		storeValue = val
	default:
		buf := new(bytes.Buffer)
		encoder := gob.NewEncoder(buf)
		err = encoder.Encode(val)
		if err != nil {
			return err
		}
		storeValue = buf.Bytes()
	}

	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return err
	}
	if maxAge == 0 {
		_, err = conn.Do("SET", key, storeValue)
	} else {
		_, err = conn.Do("SET", key, storeValue, "EX", maxAge)
	}
	return err
}

func (c *Client) SetNxEx(ctx context.Context, key string, val int, expire int) (success bool, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}

	res, err := redis.String(conn.Do("SET", key, val, "EX", expire, "NX"))
	if err != nil {
		return
	}
	return res == "OK", nil
}

func (c *Client) SetNx(ctx context.Context, key string, val int) (success bool, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}

	res, err := redis.String(conn.Do("SET", key, val, "NX"))
	if err != nil {
		return
	}
	return res == "OK", nil
}

func (c *Client) GetString(ctx context.Context, key string) (content string, err error) { //refactor
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}
	content, err = redis.String(conn.Do("GET", key))
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Client) GetByte(ctx context.Context, key string) (content []byte, err error) { //refactor
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}
	content, err = redis.Bytes(conn.Do("GET", key))
	if redis.ErrNil == err {
		err = nil
	}
	return
}

func (c *Client) GetInt64(ctx context.Context, key string) (content int64, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}
	content, err = redis.Int64(conn.Do("GET", key))
	if redis.ErrNil == err {
		err = nil
	}
	return
}

func (c *Client) GetInt(ctx context.Context, key string) (content int, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}
	content, err = redis.Int(conn.Do("GET", key))
	if redis.ErrNil == err {
		err = nil
	}
	return
}

func (c *Client) GetBool(ctx context.Context, key string) (val bool, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}

	val, err = redis.Bool(conn.Do("GET", key))
	if redis.ErrNil == err {
		err = nil
	}
	return
}

func (c *Client) Incr(ctx context.Context, key string) (counter int64, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	counter, err = redis.Int64(conn.Do("INCR", key))
	if redis.ErrNil == err {
		err = nil
	}
	return
}

func (c *Client) IncrBy(ctx context.Context, key string, counter int64) (result int64, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	result, err = redis.Int64(conn.Do("INCRBY", key, counter))
	if redis.ErrNil == err {
		err = nil
	}
	return
}

func (c *Client) Decrease(ctx context.Context, key string) (counter int, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	counter, err = redis.Int(conn.Do("DECR", key))
	if redis.ErrNil == err {
		err = nil
	}
	return
}

func (c *Client) Delete(ctx context.Context, key string) (err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if _, err = conn.Do("DEL", key); err != nil {
		return err
	}
	return nil
}

// MultiDelete 少量key使用
func (c *Client) MultiDelete(ctx context.Context, keys []string) (err error) {
	if keys == nil {
		return
	}
	var val = make([]interface{}, 0)
	for _, k := range keys {
		val = append(val, k)
	}
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	_, err = conn.Do("DEL", val...)
	return err
}

// Keys 获取少量key
func (c *Client) Keys(ctx context.Context, pattern string) (keys []string, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	values, err := redis.Values(conn.Do("KEYS", pattern))
	if err == redis.ErrNil {
		err = nil
	}
	if err != nil {
		return
	}
	err = redis.ScanSlice(values, &keys)
	return
}

func (c *Client) SAdd(ctx context.Context, key string, member string) (err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if _, err = conn.Do("SADD", key, member); err != nil {
		return err
	}
	return nil
}

func (c *Client) Spop(ctx context.Context, key string) (member string, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	member, err = redis.String(conn.Do("SPOP", key))
	if redis.ErrNil == err {
		err = nil
	}
	return
}

func (c *Client) SRem(ctx context.Context, key string, member string) (err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if _, err = conn.Do("SREM", key, member); err != nil {
		return err
	}
	return nil
}

func (c *Client) SRems(ctx context.Context, key string, members []string) (err error) {
	args := []interface{}{key}
	for _, member := range members {
		args = append(args, member)
	}

	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if _, err = conn.Do("SREM", args...); err != nil {
		return err
	}
	return nil
}

func (c *Client) Expire(ctx context.Context, key string, maxAge int) (err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if _, err = conn.Do("EXPIRE", key, maxAge); err != nil {
		return err
	}
	return nil
}

func (c *Client) SAddMult(ctx context.Context, params ...string) (err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	args := redis.Args{}
	if _, err = conn.Do("SADD", args.AddFlat(params)...); err != nil {
		return err
	}
	return nil
}

func (c *Client) SAddMultValues(ctx context.Context, key string, values []string) (err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	args := []interface{}{key}
	for _, v := range values {
		args = append(args, v)
	}
	if _, err = conn.Do("SADD", args...); err != nil {
		return err
	}
	return nil
}

func (c *Client) SAddLargeMult(ctx context.Context, key string, values []string) (err error) {
	if len(values) <= c.batchLimit {
		return c.SAddMultValues(ctx, key, values)
	}

	valueBatches := splitArrayByCount(values, c.batchLimit)
	for _, batch := range valueBatches {
		if err = c.SAddMultValues(ctx, key, batch); err != nil {
			return
		}
	}

	return
}

func (c *Client) Scard(ctx context.Context, key string) (num int, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	num, err = redis.Int(conn.Do("SCARD", key))
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Client) SMembers(ctx context.Context, key string) (members []string, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	values, err := redis.Values(conn.Do("SMEMBERS", key))
	if err != nil {
		return nil, err
	}
	err = redis.ScanSlice(values, &members)
	return
}

func (c *Client) SisMember(ctx context.Context, key string, member interface{}) (has bool, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	value, err := redis.Int(conn.Do("SISMEMBER", key, member))
	if err != nil {
		return
	}
	has = value == 1
	return
}

func (c *Client) SINTER(ctx context.Context, keys ...string) (members []string, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	args := redis.Args{}
	values, err := redis.Values(conn.Do("SINTER", args.AddFlat(keys)...))
	if err == redis.ErrNil {
		err = nil
	}
	if err != nil {
		return
	}
	err = redis.ScanSlice(values, &members)
	return
}

func (c *Client) SDIFF(ctx context.Context, keys ...string) (members []string, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	args := redis.Args{}
	values, err := redis.Values(conn.Do("SDIFF", args.AddFlat(keys)...))
	if err == redis.ErrNil {
		err = nil
	}
	if err != nil {
		return
	}
	err = redis.ScanSlice(values, &members)

	return
}

func (c *Client) SDIFFSTORE(ctx context.Context, keys ...string) (err error) {
	var val []interface{}
	for _, v := range keys {
		val = append(val, v)
	}
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()

	_, err = conn.Do("SDIFFSTORE", val...)
	return
}

func (c *Client) SRandMember(ctx context.Context, key string, count int) (members []string, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()

	values, err := redis.Values(conn.Do("SRANDMEMBER", key, count))
	if err == redis.ErrNil {
		err = nil
	}
	if err != nil {
		return
	}
	err = redis.ScanSlice(values, &members)
	return
}

func (c *Client) Exists(ctx context.Context, key string) (found bool, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}

	value, err := redis.Int64(conn.Do("EXISTS", key))
	if err != nil {
		return false, err
	}

	if value == 0 {
		return false, nil
	}

	return true, nil
}

func (c *Client) ZAdd(ctx context.Context, key string, score int64, member string) (err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if _, err = conn.Do("ZADD", key, score, member); err != nil {
		return err
	}
	return nil
}

// ZAddMulti first element of val must be key
func (c *Client) ZAddMulti(ctx context.Context, val []interface{}) (err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}

	if _, err = conn.Do("ZADD", val...); err != nil {
		return err
	}
	return nil
}

func (c *Client) ZRem(ctx context.Context, key string, member string) (err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if _, err = conn.Do("ZREM", key, member); err != nil {
		return err
	}
	return nil
}

func (c *Client) ZRemRangeByRank(ctx context.Context, key string, start, stop int) (err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if _, err = conn.Do("ZREMRANGEBYRANK", key, start, stop); err != nil {
		return err
	}
	return nil

}

func (c *Client) ZRemRangeByScore(ctx context.Context, key string, start, stop int) (err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if _, err = conn.Do("ZREMRANGEBYSCORE", key, start, stop); err != nil {
		return err
	}
	return nil

}

func (c *Client) ZRemBatch(ctx context.Context, key string, members []string) (err error) {
	args := []interface{}{key}
	for _, member := range members {
		args = append(args, member)
	}
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if _, err = conn.Do("ZREM", args...); err != nil {
		return err
	}
	return nil
}

func (c *Client) ZRange(ctx context.Context, key string, start, stop int) (members []string, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}

	values, err := redis.Values(conn.Do("ZRANGE", key, start, stop))
	if err == redis.ErrNil {
		err = nil
	}
	if err != nil {
		return
	}
	err = redis.ScanSlice(values, &members)
	return
}

func (c *Client) ZRangeWithScores(ctx context.Context, key string, start, stop int) (members []string, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}

	members, err = redis.Strings(conn.Do("ZRANGE", key, start, stop, "WITHSCORES"))
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Client) ZIncrby(ctx context.Context, key string, inc int, member string) (err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}
	_, err = conn.Do("ZINCRBY", key, inc, member)
	return
}

func (c *Client) ZUnionstore(ctx context.Context, targetKey string, originKey string) (err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}
	_, err = conn.Do("ZUNIONSTORE", targetKey, 1, originKey)
	return
}

func (c *Client) ZRank(ctx context.Context, key, member string) (rank int64, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}
	rank, err = redis.Int64(conn.Do("ZRANK", key, member))
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Client) MGetBytes(ctx context.Context, keys []string) (bs [][]byte, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}
	args := redis.Args{}
	var values []interface{}
	values, err = redis.Values(conn.Do("MGET", args.AddFlat(keys)...))
	var b []byte
	for _, v := range values {
		if v != nil {
			if b, err = redis.Bytes(v, err); err != nil {
				continue
			}
			bs = append(bs, b)
		}
	}
	err = nil
	return
}

func (c *Client) MGetOrigin(ctx context.Context, keys []string) (values []interface{}, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}
	args := redis.Args{}
	values, err = redis.Values(conn.Do("MGET", args.AddFlat(keys)...))
	return values, err
}

func (c *Client) MGetInt(ctx context.Context, keys []string) (values []int, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}
	args := redis.Args{}
	rVals, err := redis.Values(conn.Do("MGET", args.AddFlat(keys)...))
	if err != nil {
		return
	}

	for _, v := range rVals {
		n, _ := redis.Int(v, nil)
		values = append(values, n)
	}
	return values, err
}

func (c *Client) ZRevRange(ctx context.Context, key string, start, stop int64, val interface{}) (err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}
	values, err := redis.Values(conn.Do("ZREVRANGE", key, start, stop, "WITHSCORES"))
	if err == redis.ErrNil {
		err = nil
	}
	if err != nil {
		return
	}
	err = redis.ScanSlice(values, val)
	return
}

func (c *Client) ZRangeByScoreWithScores(ctx context.Context, key string, start, stop int64, val interface{}) (err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}
	values, err := redis.Values(conn.Do("ZRANGEBYSCORE", key, start, stop, "WITHSCORES"))
	if err == redis.ErrNil {
		err = nil
	}
	if err != nil {
		return
	}
	err = redis.ScanSlice(values, val)
	return
}

func (c *Client) ZRangeByScore(ctx context.Context, key string, start, stop int64, val interface{}) (err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}
	values, err := redis.Values(conn.Do("ZRANGEBYSCORE", key, start, stop))
	if err == redis.ErrNil {
		err = nil
	}
	if err != nil {
		return
	}
	err = redis.ScanSlice(values, val)
	return
}

func (c *Client) ZRevRank(ctx context.Context, key string, member string) (found bool, rank int64, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return false, 0, err
	}
	found = true
	rank, err = redis.Int64(conn.Do("ZREVRANK", key, member))
	if err == redis.ErrNil {
		return false, 0, nil
	}
	return
}

func (c *Client) ZRevRanks(ctx context.Context, key string, members []string) (founds []bool, ranks []int64, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return nil, nil, err
	}
	for _, m := range members {
		conn.Send("ZREVRANK", key, m, "WITHSCORES")
	}
	conn.Flush()
	var rank int64
	for i, _ := range members {
		rank, err = redis.Int64(conn.Receive())
		if err == redis.ErrNil {
			founds[i] = false
			ranks[i] = 0
		} else {
			founds[i] = true
			ranks[i] = rank
		}
	}
	err = nil
	return
}

func (c *Client) ZScore(ctx context.Context, key string, member string) (found bool, score float64, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return false, 0, err
	}
	found = true
	score, err = redis.Float64(conn.Do("ZSCORE", key, member))
	if err == redis.ErrNil {
		return false, 0, nil
	}
	return
}

func (c *Client) MExists(ctx context.Context, keys []string) (found []bool, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}
	for _, k := range keys {
		conn.Send("EXISTS", k)
	}
	conn.Flush()
	for _, _ = range keys {
		v, err := conn.Receive()
		if err != nil {
			fmt.Println("redis err:", err.Error())
			v = false
		}
		found = append(found, v == int64(1))
	}
	return found, err
}

func (c *Client) LPushBatch(ctx context.Context, key string, members []string) (err error) {
	args := []interface{}{key}
	for _, member := range members {
		args = append(args, member)
	}
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if _, err = conn.Do("LPUSH", args...); err != nil {
		return err
	}
	return nil
}

func (c *Client) LRange(ctx context.Context, key string, start, stop int64) (members []string, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}

	values, err := redis.Values(conn.Do("LRANGE", key, start, stop))
	if err == redis.ErrNil {
		err = nil
	}
	if err != nil {
		return
	}
	err = redis.ScanSlice(values, &members)
	return
}

func (c *Client) RPush(ctx context.Context, key string, members []string) (err error) {
	args := []interface{}{key}
	for _, member := range members {
		args = append(args, member)
	}
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if _, err = conn.Do("RPUSH", args...); err != nil {
		return err
	}
	return nil
}

func (c *Client) LPop(ctx context.Context, key string) (member string, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()

	reply, err := conn.Do("LPOP", key)
	if redis.ErrNil == err {
		err = nil
		return
	}
	member, err = redis.String(reply, err)
	if redis.ErrNil == err {
		err = nil
		return
	}
	return
}

func (c *Client) LPushLargeMult(ctx context.Context, key string, values []string) (err error) {
	if len(values) <= c.batchLimit {
		return c.LPushBatch(ctx, key, values)
	}

	valueBatches := splitArrayByCount(values, c.batchLimit)
	for _, batch := range valueBatches {
		if err = c.LPushBatch(ctx, key, batch); err != nil {
			return
		}
	}

	return
}

func (c *Client) BRPop(ctx context.Context, key string) (rlt []string, err error) {
	args := []interface{}{key}
	args = append(args, "0")
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	values, err := redis.Values(conn.Do("BRPOP", args...))
	if err == redis.ErrNil {
		err = nil
	}
	if err != nil {
		return
	}
	err = redis.ScanSlice(values, &rlt)
	return
}

func (c *Client) Rpop(ctx context.Context, key string) (member string, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()

	return redis.String(conn.Do("RPOP", key))
}

func (c *Client) ping(ctx context.Context) (bool, error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return false, nil
	}
	defer conn.Close()
	data, err := conn.Do("PING")
	if err != nil || data == nil {
		return false, err
	}
	return (data == "PONG"), nil
}

func (c *Client) HMSet(ctx context.Context, key string, kv ...interface{}) (err error) {
	var val = []interface{}{key}
	val = append(val, kv...)
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}

	if _, err = conn.Do("HMSET", val...); err != nil {
		return err
	}
	return nil
}

func (c *Client) HExists(ctx context.Context, key, field string) (ok bool, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()

	value, err := redis.Int64(conn.Do("HEXISTS", key, field))
	if err != nil {
		return false, err
	}
	if value == 0 {
		return false, nil
	}
	return true, nil
}

// PFAdd <=> PFADD key element [element]
func (c *Client) PFAdd(ctx context.Context, key string, val ...interface{}) (err error) {
	return c.do(ctx, "PFADD", key, val...)
}

// PFCount <=> PFCOUNT key element and others [others]
func (c *Client) PFCount(ctx context.Context, key string, others ...interface{}) (count int64, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}
	args := []interface{}{key}
	args = append(args, others...)
	return redis.Int64(conn.Do("PFCOUNT", args...))
}

// PFMerge <=> PFMERGE destkey sourcekey [sourcekey...]
func (c *Client) PFMerge(ctx context.Context, destkey string, sourcekey string, others ...interface{}) (err error) {
	allkey := []interface{}{sourcekey}
	allkey = append(allkey, others...)
	return c.do(ctx, "PFMERGE", destkey, allkey...)
}

// HashIncrBy hash increase by...
func (c *Client) HashIncrBy(ctx context.Context, key string, field string, by int64) (err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}
	if _, err = conn.Do("HINCRBY", key, field, by); err != nil {
		return err
	}
	return nil
}

// HashGetAllInt hash get all [integer]
func (c *Client) HashGetAllInt(ctx context.Context, key string) (result map[string]int64, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}
	return redis.Int64Map(conn.Do("HGETALL", key))
}

func (c *Client) HashGetAllString(ctx context.Context, key string) (result map[string]string, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}
	return redis.StringMap(conn.Do("HGETALL", key))
}

func (c *Client) HashKeys(ctx context.Context, key string) (keys []string, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()

	values, err := redis.Values(conn.Do("HKEYS", key))
	if err == redis.ErrNil {
		err = nil
	}
	if err != nil {
		return
	}
	err = redis.ScanSlice(values, &keys)
	return
}

func (c *Client) HGet(ctx context.Context, key, field string) (member string, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	member, err = redis.String(conn.Do("HGET", key, field))
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Client) TTL(ctx context.Context, key string) (reply int64, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	reply, err = redis.Int64(conn.Do("TTL", key))

	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Client) HDel(ctx context.Context, key, field string) (err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	_, err = conn.Do("HDEL", key, field)
	return
}

func (c *Client) SScan(ctx context.Context, key string) (val []string, err error) {
	conn, err := c.Pool.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	var cursor int64 = 0
	for {
		ks := make([]string, 0)
		ks, cursor, err = c.sscan(key, cursor, conn)
		if err != nil {
			return
		}
		val = stringSliceMerge(val, ks)
		if cursor == 0 {
			break
		}
	}

	return
}

func (c *Client) sscan(key string, cursor int64, conn redis.Conn) (ks []string, cur int64, err error) {
	sscanReply, err := redis.Values(conn.Do("SSCAN", key, cursor, "count", c.sscanKeyLimit))
	if err != nil {
		return
	}
	cur, err = redis.Int64(sscanReply[0], nil)
	if err != nil {
		return
	}
	err = redis.ScanSlice(sscanReply[1].([]interface{}), &ks)
	return
}

func stringSliceMerge(slice []string, otherSlice []string) (ret []string) {
	mergeMap := make(map[string]string, 0)
	for _, v := range slice {
		mergeMap[v] = v
	}
	for _, v := range otherSlice {
		mergeMap[v] = v
	}
	ret = make([]string, len(mergeMap))
	i := 0
	for _, s := range mergeMap {
		ret[i] = s
		i++
	}
	return
}

func splitArrayByCount(arr []string, count int) (result [][]string) {
	for i := 0; i < len(arr); i += count {
		result = append(result, arr[i:mathutil.Min(i+count, len(arr))])
	}
	return
}
