package redis

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
)

type Config struct {
	Type      bool     //是否集群
	Hosts     []string //IP
	Password  string   //密码
	Database  int      //数据库
	PoolSize  int      //连接池大小
	KeyPrefix string
}

type Configs struct {
	cfg         map[string]*Config
	connections map[string]*Client
	mu          sync.RWMutex
}

//Default ..
func Default() *Configs {
	return &Configs{
		cfg:         make(map[string]*Config),
		connections: make(map[string]*Client),
	}
}

//SetConfig 设置配置文件
func (configs *Configs) SetConfig(name string, cf *Config) *Configs {
	configs.cfg[name] = cf
	return configs
}

//Get  获取 redis 实列
func (configs *Configs) GetRedis(name string) *Client {

	conn, ok := configs.connections[name]
	if ok {
		return conn
	}

	config, ok := configs.cfg[name]
	if !ok {
		Log.Panic("Redis配置:" + name + "找不到！")
	}

	db := connect(config)
	configs.mu.Lock()
	configs.connections[name] = db
	configs.mu.Unlock()
	configs.mu.RLock()
	v := configs.connections[name]
	configs.mu.RUnlock()
	return v
}

func connect(config *Config) *Client {
	opts := Options{}
	if config.Type {
		opts.Type = ClientCluster
	} else {
		opts.Type = ClientNormal
	}
	opts.Hosts = config.Hosts
	opts.KeyPrefix = config.KeyPrefix

	if config.PoolSize > 0 {
		opts.PoolSize = config.PoolSize
	} else {
		opts.PoolSize = 64
	}
	if config.Database > 0 {
		opts.Database = config.Database
	} else {
		opts.Database = 0
	}
	if config.Password != "" {
		opts.Password = config.Password
	}
	client := NewClient(opts)
	if err := client.Ping().Err(); err != nil {
		Log.Panic(err.Error())
	}
	return client
}

// RedisNil means nil reply, .e.g. when key does not exist.
const RedisNil = redis.Nil

// Client a struct representing the redis client
type Client struct {
	opts      Options
	client    Commander
	fmtString string
}

// NewClient 新客户端
func NewClient(opts Options) *Client {
	r := &Client{opts: opts}
	switch opts.Type {
	// 群集客户端
	case ClientCluster:
		r.client = redis.NewClusterClient(opts.GetClusterConfig())
	// 标准客户端也是默认值
	case ClientNormal:
		fallthrough
	default:
		r.client = redis.NewClient(opts.GetNormalConfig())
	}
	r.fmtString = opts.KeyPrefix + "%s"
	return r
}

// IsCluster
func (r *Client) IsCluster() bool {
	if r.opts.Type == ClientCluster {
		return true
	}
	return false
}

//Prefix 返回前缀+键
func (r *Client) Prefix(key string) string {
	return fmt.Sprintf(r.fmtString, key)
}

// k 格式化并返回带前缀的密钥
func (r *Client) k(key string) string {
	return fmt.Sprintf(r.fmtString, key)
}

// ks 使用前缀格式化并返回一组键
func (r *Client) ks(key ...string) []string {
	keys := make([]string, len(key))
	for i, k := range key {
		keys[i] = r.k(k)
	}
	return keys
}

// GetClient 返回客户端
func (r *Client) GetClient() Commander {
	return r.client
}

// Ping 发送ping命令
func (r *Client) Ping() *redis.StatusCmd {
	return r.client.Ping()
}

// Incr 对key递增+1
func (r *Client) Incr(key string) *redis.IntCmd {
	return r.client.Incr(r.k(key))
}

// IncrBy 使用增量值递增
func (r *Client) IncrBy(key string, value int64) *redis.IntCmd {
	return r.client.IncrBy(r.k(key), value)
}

// Decr key递减1
func (r *Client) Decr(key string) *redis.IntCmd {
	return r.client.Decr(r.k(key))
}

// DecrBy 使用增量值递减
func (r *Client) DecrBy(key string, value int64) *redis.IntCmd {
	return r.client.DecrBy(r.k(key), value)
}

// Expire 过期方法
func (r *Client) Expire(key string, expiration time.Duration) *redis.BoolCmd {
	return r.client.Expire(r.k(key), expiration)
}

// ExpireAt  命令用于以 UNIX 时间戳(unix timestamp)格式设置 key 的过期时间
func (r *Client) ExpireAt(key string, tm time.Time) *redis.BoolCmd {
	return r.client.ExpireAt(r.k(key), tm)
}

// Persist 移除 key 的过期时间
func (r *Client) Persist(key string) *redis.BoolCmd {
	return r.client.Persist(r.k(key))
}

// PExpire 毫秒为单位设置 key 的生存时间
func (r *Client) PExpire(key string, expiration time.Duration) *redis.BoolCmd {
	return r.client.PExpire(r.k(key), expiration)
}
func (r *Client) PExpireAt(key string, tm time.Time) *redis.BoolCmd {
	return r.client.PExpireAt(r.k(key), tm)
}
func (r *Client) PTTL(key string) *redis.DurationCmd {
	return r.client.PTTL(r.k(key))
}
func (r *Client) TTL(key string) *redis.DurationCmd {
	return r.client.TTL(r.k(key))
}

// Exists exists command
func (r *Client) Exists(key ...string) *redis.IntCmd {
	return r.client.Exists(r.ks(key...)...)
}

// Get get key value
func (r *Client) Get(key string) *redis.StringCmd {
	return r.client.Get(r.k(key))
}

// GetBit getbit key value
func (r *Client) GetBit(key string, offset int64) *redis.IntCmd {
	return r.client.GetBit(r.k(key), offset)
}

// GetRange GetRange key value
func (r *Client) GetRange(key string, start, end int64) *redis.StringCmd {
	return r.client.GetRange(r.k(key), start, end)
}

// GetSet getset command
func (r *Client) GetSet(key string, value interface{}) *redis.StringCmd {
	return r.client.GetSet(r.k(key), value)
}

// MGetByPipeline gets multiple values from keys,Pipeline is used when
// redis is a cluster,This means higher IO performance
// params: keys ...string
// return: []string, error
func (r *Client) MGetByPipeline(keys ...string) ([]string, error) {

	var res []string

	if r.IsCluster() {
		start := time.Now()
		pipeLineLen := 100
		pipeCount := len(keys)/pipeLineLen + 1
		pipes := make([]redis.Pipeliner, pipeCount)
		for i := 0; i < pipeCount; i++ {
			pipes[i] = r.client.Pipeline()
		}
		for i, k := range keys {
			p := pipes[i%pipeCount]
			p.Get(r.k(k))
		}
		Log.Debug("process cost: %v", time.Since(start))
		start = time.Now()
		var wg sync.WaitGroup
		var lock sync.Mutex
		errors := make(chan error, pipeCount)
		for _, p := range pipes {
			p := p
			wg.Add(1)
			go func() {
				defer wg.Done()
				cmders, err := p.Exec()
				if err != nil {
					select {
					case errors <- err:
					default:
					}
					return
				}
				lock.Lock()
				defer lock.Unlock()
				for _, cmder := range cmders {
					result, _ := cmder.(*redis.StringCmd).Result()
					res = append(res, result)
				}
			}()
		}
		wg.Wait()
		Log.Debug("exec cost: %v", time.Since(start))

		if len(errors) > 0 {
			return nil, <-errors
		}

		return res, nil
	}

	vals, err := r.client.MGet(keys...).Result()

	if redis.Nil != err && nil != err {
		return nil, err
	}

	for _, item := range vals {
		res = append(res, fmt.Sprintf("%s", item))
	}

	return res, err
}

// MGet Multiple get command
func (r *Client) MGet(keys ...string) *redis.SliceCmd {
	return r.client.MGet(r.ks(keys...)...)
}

// Dump dump command
func (r *Client) Dump(key string) *redis.StringCmd {
	return r.client.Dump(r.k(key))
}

// -------------- Hasher

func (r *Client) HExists(key, field string) *redis.BoolCmd {
	return r.client.HExists(r.k(key), field)
}
func (r *Client) HGet(key, field string) *redis.StringCmd {
	return r.client.HGet(r.k(key), field)
}
func (r *Client) HGetAll(key string) *redis.StringStringMapCmd {
	return r.client.HGetAll(r.k(key))
}
func (r *Client) HIncrBy(key, field string, incr int64) *redis.IntCmd {
	return r.client.HIncrBy(r.k(key), field, incr)
}
func (r *Client) HIncrByFloat(key, field string, incr float64) *redis.FloatCmd {
	return r.client.HIncrByFloat(r.k(key), field, incr)
}
func (r *Client) HKeys(key string) *redis.StringSliceCmd {
	return r.client.HKeys(r.k(key))
}
func (r *Client) HLen(key string) *redis.IntCmd {
	return r.client.HLen(r.k(key))
}
func (r *Client) HMGet(key string, fields ...string) *redis.SliceCmd {
	return r.client.HMGet(r.k(key), fields...)
}

func (r *Client) HMSet(key string, value ...interface{}) *redis.BoolCmd {
	return r.client.HMSet(r.k(key), value...)
}

func (r *Client) HSet(key string, value ...interface{}) *redis.IntCmd {
	return r.client.HSet(r.k(key), value...)
}
func (r *Client) HSetNX(key, field string, value interface{}) *redis.BoolCmd {
	return r.client.HSetNX(r.k(key), field, value)
}
func (r *Client) HVals(key string) *redis.StringSliceCmd {
	return r.client.HVals(r.k(key))
}
func (r *Client) HDel(key string, fields ...string) *redis.IntCmd {
	return r.client.HDel(r.k(key), fields...)
}

// -------------- Lister

func (r *Client) LIndex(key string, index int64) *redis.StringCmd {
	return r.client.LIndex(r.k(key), index)
}
func (r *Client) LInsert(key, op string, pivot, value interface{}) *redis.IntCmd {
	return r.client.LInsert(r.k(key), op, pivot, value)
}
func (r *Client) LInsertAfter(key string, pivot, value interface{}) *redis.IntCmd {
	return r.client.LInsertAfter(r.k(key), pivot, value)
}
func (r *Client) LInsertBefore(key string, pivot, value interface{}) *redis.IntCmd {
	return r.client.LInsertBefore(r.k(key), pivot, value)
}
func (r *Client) LLen(key string) *redis.IntCmd {
	return r.client.LLen(r.k(key))
}
func (r *Client) LPop(key string) *redis.StringCmd {
	return r.client.LPop(r.k(key))
}
func (r *Client) LPush(key string, values ...interface{}) *redis.IntCmd {
	return r.client.LPush(r.k(key), values...)
}
func (r *Client) LPushX(key string, value interface{}) *redis.IntCmd {
	return r.client.LPushX(r.k(key), value)
}
func (r *Client) LRange(key string, start, stop int64) *redis.StringSliceCmd {
	return r.client.LRange(r.k(key), start, stop)
}
func (r *Client) LRem(key string, count int64, value interface{}) *redis.IntCmd {
	return r.client.LRem(r.k(key), count, value)
}
func (r *Client) LSet(key string, index int64, value interface{}) *redis.StatusCmd {
	return r.client.LSet(r.k(key), index, value)
}
func (r *Client) LTrim(key string, start, stop int64) *redis.StatusCmd {
	return r.client.LTrim(r.k(key), start, stop)
}
func (r *Client) RPop(key string) *redis.StringCmd {
	return r.client.RPop(r.k(key))
}
func (r *Client) RPopLPush(source, destination string) *redis.StringCmd {
	return r.client.RPopLPush(r.k(source), r.k(destination))
}
func (r *Client) RPush(key string, values ...interface{}) *redis.IntCmd {
	return r.client.RPush(r.k(key), values...)
}
func (r *Client) RPushX(key string, value interface{}) *redis.IntCmd {
	return r.client.RPushX(r.k(key), value)
}

// -------------- Setter

// Set function
func (r *Client) Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return r.client.Set(r.k(key), value, expiration)
}
func (r *Client) Append(key, value string) *redis.IntCmd {
	return r.client.Append(r.k(key), value)
}
func (r *Client) Del(keys ...string) *redis.IntCmd {
	return r.client.Del(r.ks(keys...)...)
}
func (r *Client) Unlink(keys ...string) *redis.IntCmd {
	return r.client.Unlink(r.ks(keys...)...)
}

func (r *Client) MSet(values ...interface{}) *redis.StatusCmd {
	return r.client.MSet(values...)
}

func (r *Client) MSetNX(values ...interface{}) *redis.BoolCmd {
	return r.client.MSetNX(values...)
}

// -------------- Settable

func (r *Client) SAdd(key string, members ...interface{}) *redis.IntCmd {
	return r.client.SAdd(r.k(key), members...)
}
func (r *Client) SCard(key string) *redis.IntCmd {
	return r.client.SCard(r.k(key))
}
func (r *Client) SDiff(keys ...string) *redis.StringSliceCmd {
	return r.client.SDiff(r.ks(keys...)...)
}
func (r *Client) SDiffStore(destination string, keys ...string) *redis.IntCmd {
	return r.client.SDiffStore(r.k(destination), r.ks(keys...)...)
}
func (r *Client) SInter(keys ...string) *redis.StringSliceCmd {
	return r.client.SInter(r.ks(keys...)...)
}
func (r *Client) SInterStore(destination string, keys ...string) *redis.IntCmd {
	return r.client.SInterStore(r.k(destination), r.ks(keys...)...)
}
func (r *Client) SIsMember(key string, member interface{}) *redis.BoolCmd {
	return r.client.SIsMember(r.k(key), member)
}
func (r *Client) SMembers(key string) *redis.StringSliceCmd {
	return r.client.SMembers(r.k(key))
}
func (r *Client) SMove(source, destination string, member interface{}) *redis.BoolCmd {
	return r.client.SMove(r.k(source), r.k(destination), member)
}
func (r *Client) SPop(key string) *redis.StringCmd {
	return r.client.SPop(r.k(key))
}
func (r *Client) SPopN(key string, count int64) *redis.StringSliceCmd {
	return r.client.SPopN(r.k(key), count)
}
func (r *Client) SRandMember(key string) *redis.StringCmd {
	return r.client.SRandMember(r.k(key))
}
func (r *Client) SRandMemberN(key string, count int64) *redis.StringSliceCmd {
	return r.client.SRandMemberN(r.k(key), count)
}
func (r *Client) SRem(key string, members ...interface{}) *redis.IntCmd {
	return r.client.SRem(r.k(key), members...)
}
func (r *Client) SUnion(keys ...string) *redis.StringSliceCmd {
	return r.client.SUnion(r.ks(keys...)...)
}
func (r *Client) SUnionStore(destination string, keys ...string) *redis.IntCmd {
	return r.client.SUnionStore(r.k(destination), r.ks(keys...)...)
}

// -------------- SortedSettable

func (r *Client) ZAdd(key string, members ...*redis.Z) *redis.IntCmd {
	return r.client.ZAdd(r.k(key), members...)
}
func (r *Client) ZAddNX(key string, members ...*redis.Z) *redis.IntCmd {
	return r.client.ZAddNX(r.k(key), members...)
}
func (r *Client) ZAddXX(key string, members ...*redis.Z) *redis.IntCmd {
	return r.client.ZAddXX(r.k(key), members...)
}
func (r *Client) ZAddCh(key string, members ...*redis.Z) *redis.IntCmd {
	return r.client.ZAddCh(r.k(key), members...)
}
func (r *Client) ZAddNXCh(key string, members ...*redis.Z) *redis.IntCmd {
	return r.client.ZAddNXCh(r.k(key), members...)
}
func (r *Client) ZAddXXCh(key string, members ...*redis.Z) *redis.IntCmd {
	return r.client.ZAddXXCh(r.k(key), members...)
}
func (r *Client) ZIncr(key string, member *redis.Z) *redis.FloatCmd {
	return r.client.ZIncr(r.k(key), member)
}
func (r *Client) ZIncrNX(key string, member *redis.Z) *redis.FloatCmd {
	return r.client.ZIncrNX(r.k(key), member)
}
func (r *Client) ZIncrXX(key string, member *redis.Z) *redis.FloatCmd {
	return r.client.ZIncrXX(r.k(key), member)
}
func (r *Client) ZCard(key string) *redis.IntCmd {
	return r.client.ZCard(r.k(key))
}
func (r *Client) ZCount(key, min, max string) *redis.IntCmd {
	return r.client.ZCount(r.k(key), min, max)
}
func (r *Client) ZIncrBy(key string, increment float64, member string) *redis.FloatCmd {
	return r.client.ZIncrBy(r.k(key), increment, member)
}

func (r *Client) ZInterStore(key string, store *redis.ZStore) *redis.IntCmd {
	return r.client.ZInterStore(r.k(key), store)
}
func (r *Client) ZRange(key string, start, stop int64) *redis.StringSliceCmd {
	return r.client.ZRange(r.k(key), start, stop)
}
func (r *Client) ZRangeWithScores(key string, start, stop int64) *redis.ZSliceCmd {
	return r.client.ZRangeWithScores(r.k(key), start, stop)
}
func (r *Client) ZRangeByScore(key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return r.client.ZRangeByScore(r.k(key), opt)
}
func (r *Client) ZRangeByLex(key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return r.client.ZRangeByLex(r.k(key), opt)
}
func (r *Client) ZRangeByScoreWithScores(key string, opt *redis.ZRangeBy) *redis.ZSliceCmd {
	return r.client.ZRangeByScoreWithScores(r.k(key), opt)
}
func (r *Client) ZRank(key, member string) *redis.IntCmd {
	return r.client.ZRank(r.k(key), member)
}
func (r *Client) ZRem(key string, members ...interface{}) *redis.IntCmd {
	return r.client.ZRem(r.k(key), members...)
}
func (r *Client) ZRemRangeByRank(key string, start, stop int64) *redis.IntCmd {
	return r.client.ZRemRangeByRank(r.k(key), start, stop)
}
func (r *Client) ZRemRangeByScore(key, min, max string) *redis.IntCmd {
	return r.client.ZRemRangeByScore(r.k(key), min, max)
}
func (r *Client) ZRemRangeByLex(key, min, max string) *redis.IntCmd {
	return r.client.ZRemRangeByLex(r.k(key), min, max)
}
func (r *Client) ZRevRange(key string, start, stop int64) *redis.StringSliceCmd {
	return r.client.ZRevRange(r.k(key), start, stop)
}
func (r *Client) ZRevRangeWithScores(key string, start, stop int64) *redis.ZSliceCmd {
	return r.client.ZRevRangeWithScores(r.k(key), start, stop)
}
func (r *Client) ZRevRangeByScore(key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return r.client.ZRevRangeByScore(r.k(key), opt)
}
func (r *Client) ZRevRangeByLex(key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return r.client.ZRevRangeByLex(r.k(key), opt)
}
func (r *Client) ZRevRangeByScoreWithScores(key string, opt *redis.ZRangeBy) *redis.ZSliceCmd {
	return r.client.ZRevRangeByScoreWithScores(r.k(key), opt)
}
func (r *Client) ZRevRank(key, member string) *redis.IntCmd {
	return r.client.ZRevRank(r.k(key), member)
}
func (r *Client) ZScore(key, member string) *redis.FloatCmd {
	return r.client.ZScore(r.k(key), member)
}

func (r *Client) ZUnionStore(dest string, store *redis.ZStore) *redis.IntCmd {
	return r.client.ZUnionStore(r.k(dest), store)
}

// -------------- BlockedSettable

func (r *Client) BLPop(timeout time.Duration, keys ...string) *redis.StringSliceCmd {
	return r.client.BLPop(timeout, r.ks(keys...)...)
}
func (r *Client) BRPop(timeout time.Duration, keys ...string) *redis.StringSliceCmd {
	return r.client.BRPop(timeout, r.ks(keys...)...)
}
func (r *Client) BRPopLPush(source, destination string, timeout time.Duration) *redis.StringCmd {
	return r.client.BRPopLPush(r.k(source), r.k(destination), timeout)
}

// -------------- Scanner

func (r *Client) Type(key string) *redis.StatusCmd {
	return r.client.Type(r.k(key))
}
func (r *Client) Scan(cursor uint64, match string, count int64) *redis.ScanCmd {
	return r.client.Scan(cursor, r.k(match), count)
}
func (r *Client) SScan(key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	return r.client.SScan(r.k(key), cursor, match, count)
}
func (r *Client) ZScan(key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	return r.client.ZScan(r.k(key), cursor, match, count)
}
func (r *Client) HScan(key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	return r.client.HScan(r.k(key), cursor, match, count)
}

// -------------- Publisher

func (r *Client) Publish(channel string, message interface{}) *redis.IntCmd {
	return r.client.Publish(r.k(channel), message)
}
func (r *Client) Subscribe(channels ...string) *redis.PubSub {
	return r.client.Subscribe(r.ks(channels...)...)
}

// Pipeline get Pipeliner of r.client
func (r *Client) Pipeline() redis.Pipeliner {
	return r.client.Pipeline()
}

// ErrNotImplemented not implemented error
var ErrNotImplemented = errors.New("Not implemented")
