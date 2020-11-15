package redis

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
)

//Config 配置
type Config struct {
	Type      bool     //是否集群
	Hosts     []string //IP
	Password  string   //密码
	Database  int      //数据库
	PoolSize  int      //连接池大小
	KeyPrefix string
}

//Configs 配置组
type Configs struct {
	cfg         map[string]*Config
	connections map[string]*Client
	mu          sync.RWMutex
}

//Default 构造
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

//GetRedis  获取 redis 实列
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
		tc := redis.NewClusterClient(opts.GetClusterConfig())
		// redisClient.AddHook()
		r.client = tc
	// 标准客户端也是默认值
	case ClientNormal:
		fallthrough
	default:
		tc := redis.NewClient(opts.GetNormalConfig())
		// redisClient.AddHook()
		r.client = tc

	}
	r.fmtString = opts.KeyPrefix + "%s"
	return r
}

// IsCluster 判断是否集群
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

// Pipeline 获取管道
func (r *Client) Pipeline() redis.Pipeliner {
	return r.client.Pipeline()
}

//Pipelined 管道
func (r *Client) Pipelined(fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	return r.Pipelined(fn)
}

//TxPipeline 获取管道
func (r *Client) TxPipeline() redis.Pipeliner {
	return r.client.TxPipeline()
}

//TxPipelined 管道
func (r *Client) TxPipelined(fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	return r.TxPipelined(fn)
}

//Command 返回有关所有Redis命令的详细信息的Array回复
func (r *Client) Command() *redis.CommandsInfoCmd {
	return r.client.Command()
}

// ClientGetName returns the name of the connection.
func (r *Client) ClientGetName() *redis.StringCmd {
	return r.client.ClientGetName()
}

// Echo  批量字符串回复
func (r *Client) Echo(message interface{}) *redis.StringCmd {
	return r.client.Echo(message)
}

// Ping 使用客户端向 Redis 服务器发送一个 PING ，如果服务器运作正常的话，会返回一个 PONG 。
// 通常用于测试与服务器的连接是否仍然生效，或者用于测量延迟值。
// 如果连接正常就返回一个 PONG ，否则返回一个连接错误。
func (r *Client) Ping() *redis.StatusCmd {
	return r.client.Ping()
}

//Quit 关闭连接
func (r *Client) Quit() *redis.StatusCmd {
	return r.client.Quit()
}

// Del 删除给定的一个或多个 key 。
// 不存在的 key 会被忽略。
func (r *Client) Del(keys ...string) *redis.IntCmd {
	return r.client.Del(r.ks(keys...)...)
}

// Unlink 这个命令非常类似于DEL：它删除指定的键。就像DEL键一样，如果它不存在，它将被忽略。但是，该命令在不同的线程中执行实际的内存回收，所以它不会阻塞，而DEL是。这是命令名称的来源：命令只是将键与键空间断开连接。实际删除将在以后异步发生。
func (r *Client) Unlink(keys ...string) *redis.IntCmd {
	return r.client.Unlink(r.ks(keys...)...)
}

// Dump 序列化给定 key ，并返回被序列化的值，使用 RESTORE 命令可以将这个值反序列化为 Redis 键。
// 如果 key 不存在，那么返回 nil 。
// 否则，返回序列化之后的值。
func (r *Client) Dump(key string) *redis.StringCmd {
	return r.client.Dump(r.k(key))
}

// Exists 检查给定 key 是否存在。
// 若 key 存在，返回 1 ，否则返回 0 。
func (r *Client) Exists(key ...string) *redis.IntCmd {
	return r.client.Exists(r.ks(key...)...)
}

// Expire 为给定 key 设置生存时间，当 key 过期时(生存时间为 0 )，它会被自动删除。
// 设置成功返回 1 。
// 当 key 不存在或者不能为 key 设置生存时间时(比如在低于 2.1.3 版本的 Redis 中你尝试更新 key 的生存时间)，返回 0 。
func (r *Client) Expire(key string, expiration time.Duration) *redis.BoolCmd {
	return r.client.Expire(r.k(key), expiration)
}

// ExpireAt  EXPIREAT 的作用和 EXPIRE 类似，都用于为 key 设置生存时间。
// 命令用于以 UNIX 时间戳(unix timestamp)格式设置 key 的过期时间
func (r *Client) ExpireAt(key string, tm time.Time) *redis.BoolCmd {
	return r.client.ExpireAt(r.k(key), tm)
}

// Keys 查找所有符合给定模式 pattern 的 key 。
func (r *Client) Keys(pattern string) *redis.StringSliceCmd {
	return r.client.Keys(r.k(pattern))
}

//Migrate 将 key 原子性地从当前实例传送到目标实例的指定数据库上，一旦传送成功， key 保证会出现在目标实例上，而当前实例上的 key 会被删除。
func (r *Client) Migrate(host, port, key string, db int, timeout time.Duration) *redis.StatusCmd {
	return r.client.Migrate(host, port, r.k(key), db, timeout)
}

// Move 将当前数据库的 key 移动到给定的数据库 db 当中。
// 如果当前数据库(源数据库)和给定数据库(目标数据库)有相同名字的给定 key ，或者 key 不存在于当前数据库，那么 MOVE 没有任何效果。
func (r *Client) Move(key string, db int) *redis.BoolCmd {
	return r.client.Move(r.k(key), db)
}

//ObjectRefCount 返回给定 key 引用所储存的值的次数。此命令主要用于除错。
func (r *Client) ObjectRefCount(key string) *redis.IntCmd {
	return r.client.ObjectRefCount(r.k(key))
}

//ObjectEncoding 返回给定 key 锁储存的值所使用的内部表示(representation)。
func (r *Client) ObjectEncoding(key string) *redis.StringCmd {
	return r.client.ObjectEncoding(r.k(key))
}

//ObjectIdleTime 返回给定 key 自储存以来的空转时间(idle， 没有被读取也没有被写入)，以秒为单位。
func (r *Client) ObjectIdleTime(key string) *redis.DurationCmd {
	return r.client.ObjectIdleTime(r.k(key))
}

// Persist 移除给定 key 的生存时间，将这个 key 从『易失的』(带生存时间 key )转换成『持久的』(一个不带生存时间、永不过期的 key )。
// 当生存时间移除成功时，返回 1 .
// 如果 key 不存在或 key 没有设置生存时间，返回 0 。
func (r *Client) Persist(key string) *redis.BoolCmd {
	return r.client.Persist(r.k(key))
}

// PExpire 毫秒为单位设置 key 的生存时间
func (r *Client) PExpire(key string, expiration time.Duration) *redis.BoolCmd {
	return r.client.PExpire(r.k(key), expiration)
}

// PExpireAt 这个命令和 expireat 命令类似，但它以毫秒为单位设置 key 的过期 unix 时间戳，而不是像 expireat 那样，以秒为单位。
// 如果生存时间设置成功，返回 1 。 当 key 不存在或没办法设置生存时间时，返回 0
func (r *Client) PExpireAt(key string, tm time.Time) *redis.BoolCmd {
	return r.client.PExpireAt(r.k(key), tm)
}

// PTTL 这个命令类似于 TTL 命令，但它以毫秒为单位返回 key 的剩余生存时间，而不是像 TTL 命令那样，以秒为单位。
// 当 key 不存在时，返回 -2 。
// 当 key 存在但没有设置剩余生存时间时，返回 -1 。
// 否则，以毫秒为单位，返回 key 的剩余生存时间。
func (r *Client) PTTL(key string) *redis.DurationCmd {
	return r.client.PTTL(r.k(key))
}

// RandomKey 从当前数据库中随机返回(不删除)一个 key 。
func (r *Client) RandomKey() *redis.StringCmd {
	return r.client.RandomKey()
}

// Rename 将 key 改名为 newkey 。
// 当 key 和 newkey 相同，或者 key 不存在时，返回一个错误。
// 当 newkey 已经存在时， RENAME 命令将覆盖旧值。
func (r *Client) Rename(key, newkey string) *redis.StatusCmd {
	return r.client.Rename(r.k(key), r.k(newkey))
}

// RenameNX 当且仅当 newkey 不存在时，将 key 改名为 newkey 。
// 当 key 不存在时，返回一个错误。
func (r *Client) RenameNX(key, newkey string) *redis.BoolCmd {
	return r.client.RenameNX(r.k(key), r.k(newkey))
}

// Restore 反序列化给定的序列化值，并将它和给定的 key 关联。
// 参数 ttl 以毫秒为单位为 key 设置生存时间；如果 ttl 为 0 ，那么不设置生存时间。
// RESTORE 在执行反序列化之前会先对序列化值的 RDB 版本和数据校验和进行检查，如果 RDB 版本不相同或者数据不完整的话，那么 RESTORE 会拒绝进行反序列化，并返回一个错误。
func (r *Client) Restore(key string, ttl time.Duration, value string) *redis.StatusCmd {
	return r.client.Restore(r.k(key), ttl, value)
}

// RestoreReplace -> Restore
func (r *Client) RestoreReplace(key string, ttl time.Duration, value string) *redis.StatusCmd {
	return r.client.RestoreReplace(r.k(key), ttl, value)
}

// Sort 返回或保存给定列表、集合、有序集合 key 中经过排序的元素。
// 排序默认以数字作为对象，值被解释为双精度浮点数，然后进行比较。
func (r *Client) Sort(key string, sort *redis.Sort) *redis.StringSliceCmd {
	return r.client.Sort(r.k(key), sort)
}

//SortStore -> Sort
func (r *Client) SortStore(key, store string, sort *redis.Sort) *redis.IntCmd {
	return r.client.SortStore(r.k(key), store, sort)
}

//SortInterfaces -> Sort
func (r *Client) SortInterfaces(key string, sort *redis.Sort) *redis.SliceCmd {
	return r.client.SortInterfaces(r.k(key), sort)
}

// Touch 更改键的上次访问时间。返回指定的现有键的数量。
func (r *Client) Touch(keys ...string) *redis.IntCmd {
	return r.client.Touch(r.ks(keys...)...)
}

// TTL 以秒为单位，返回给定 key 的剩余生存时间(TTL, time to live)。
// 当 key 不存在时，返回 -2 。
// 当 key 存在但没有设置剩余生存时间时，返回 -1 。
// 否则，以秒为单位，返回 key 的剩余生存时间。
func (r *Client) TTL(key string) *redis.DurationCmd {
	return r.client.TTL(r.k(key))
}

// Type 返回 key 所储存的值的类型。
func (r *Client) Type(key string) *redis.StatusCmd {
	return r.client.Type(r.k(key))
}

// Scan 命令及其相关的 SSCAN 命令、 HSCAN 命令和 ZSCAN 命令都用于增量地迭代（incrementally iterate）一集元素
func (r *Client) Scan(cursor uint64, match string, count int64) *redis.ScanCmd {
	return r.client.Scan(cursor, r.k(match), count)
}

// SScan 详细信息请参考 SCAN 命令。
func (r *Client) SScan(key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	return r.client.SScan(r.k(key), cursor, match, count)
}

// HScan 详细信息请参考 SCAN 命令。
func (r *Client) HScan(key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	return r.client.HScan(r.k(key), cursor, match, count)
}

// ZScan 详细信息请参考 SCAN 命令。
func (r *Client) ZScan(key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	return r.client.ZScan(r.k(key), cursor, match, count)
}

// Append 如果 key 已经存在并且是一个字符串， APPEND 命令将 value 追加到 key 原来的值的末尾。
// 如果 key 不存在， APPEND 就简单地将给定 key 设为 value ，就像执行 SET key value 一样。
func (r *Client) Append(key, value string) *redis.IntCmd {
	return r.client.Append(r.k(key), value)
}

// BitCount 计算给定字符串中，被设置为 1 的比特位的数量。
// 一般情况下，给定的整个字符串都会被进行计数，通过指定额外的 start 或 end 参数，可以让计数只在特定的位上进行。
// start 和 end 参数的设置和 GETRANGE 命令类似，都可以使用负数值：比如 -1 表示最后一个位，而 -2 表示倒数第二个位，以此类推。
// 不存在的 key 被当成是空字符串来处理，因此对一个不存在的 key 进行 BITCOUNT 操作，结果为 0 。
func (r *Client) BitCount(key string, bitCount *redis.BitCount) *redis.IntCmd {
	return r.client.BitCount(r.k(key), bitCount)
}

// BitOpAnd -> BitCount
func (r *Client) BitOpAnd(destKey string, keys ...string) *redis.IntCmd {
	return r.client.BitOpAnd(r.k(destKey), r.ks(keys...)...)
}

// BitOpOr -> BitCount
func (r *Client) BitOpOr(destKey string, keys ...string) *redis.IntCmd {
	return r.client.BitOpOr(r.k(destKey), r.ks(keys...)...)
}

// BitOpXor -> BitCount
func (r *Client) BitOpXor(destKey string, keys ...string) *redis.IntCmd {
	return r.client.BitOpXor(r.k(destKey), r.ks(keys...)...)
}

// BitOpNot -> BitCount
func (r *Client) BitOpNot(destKey string, key string) *redis.IntCmd {
	return r.client.BitOpXor(r.k(destKey), r.k(key))
}

// BitPos -> BitCount
func (r *Client) BitPos(key string, bit int64, pos ...int64) *redis.IntCmd {
	return r.client.BitPos(r.k(key), bit, pos...)
}

// BitField -> BitCount
func (r *Client) BitField(key string, args ...interface{}) *redis.IntSliceCmd {
	return r.client.BitField(r.k(key), args...)
}

// Decr 将 key 中储存的数字值减一。
// 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 DECR 操作。
// 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。
// 本操作的值限制在 64 位(bit)有符号数字表示之内。
// 关于递增(increment) / 递减(decrement)操作的更多信息，请参见 INCR 命令。
// 执行 DECR 命令之后 key 的值。
func (r *Client) Decr(key string) *redis.IntCmd {
	return r.client.Decr(r.k(key))
}

// DecrBy 将 key 所储存的值减去减量 decrement 。
// 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 DECRBY 操作。
// 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。
// 本操作的值限制在 64 位(bit)有符号数字表示之内。
// 关于更多递增(increment) / 递减(decrement)操作的更多信息，请参见 INCR 命令。
// 减去 decrement 之后， key 的值。
func (r *Client) DecrBy(key string, value int64) *redis.IntCmd {
	return r.client.DecrBy(r.k(key), value)
}

// Get 返回 key 所关联的字符串值。
// 如果 key 不存在那么返回特殊值 nil 。
// 假如 key 储存的值不是字符串类型，返回一个错误，因为 GET 只能用于处理字符串值。
// 当 key 不存在时，返回 nil ，否则，返回 key 的值。
// 如果 key 不是字符串类型，那么返回一个错误。
func (r *Client) Get(key string) *redis.StringCmd {
	return r.client.Get(r.k(key))
}

// GetBit 对 key 所储存的字符串值，获取指定偏移量上的位(bit)。
// 当 offset 比字符串值的长度大，或者 key 不存在时，返回 0 。
// 字符串值指定偏移量上的位(bit)。
func (r *Client) GetBit(key string, offset int64) *redis.IntCmd {
	return r.client.GetBit(r.k(key), offset)
}

// GetRange 返回 key 中字符串值的子字符串，字符串的截取范围由 start 和 end 两个偏移量决定(包括 start 和 end 在内)。
// 负数偏移量表示从字符串最后开始计数， -1 表示最后一个字符， -2 表示倒数第二个，以此类推。
// GETRANGE 通过保证子字符串的值域(range)不超过实际字符串的值域来处理超出范围的值域请求。
// 返回截取得出的子字符串。
func (r *Client) GetRange(key string, start, end int64) *redis.StringCmd {
	return r.client.GetRange(r.k(key), start, end)
}

// GetSet 将给定 key 的值设为 value ，并返回 key 的旧值(old value)。
// 当 key 存在但不是字符串类型时，返回一个错误。
// 返回给定 key 的旧值。
// 当 key 没有旧值时，也即是， key 不存在时，返回 nil 。
func (r *Client) GetSet(key string, value interface{}) *redis.StringCmd {
	return r.client.GetSet(r.k(key), value)
}

// Incr 将 key 中储存的数字值增一。
// 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCR 操作。
// 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。
// 本操作的值限制在 64 位(bit)有符号数字表示之内。
// 执行 INCR 命令之后 key 的值。
func (r *Client) Incr(key string) *redis.IntCmd {
	return r.client.Incr(r.k(key))
}

// IncrBy 将 key 所储存的值加上增量 increment 。
// 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCRBY 命令。
// 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。
// 本操作的值限制在 64 位(bit)有符号数字表示之内。
// 关于递增(increment) / 递减(decrement)操作的更多信息，参见 INCR 命令。
// 加上 increment 之后， key 的值。
func (r *Client) IncrBy(key string, value int64) *redis.IntCmd {
	return r.client.IncrBy(r.k(key), value)
}

// IncrByFloat 为 key 中所储存的值加上浮点数增量 increment 。
// 如果 key 不存在，那么 INCRBYFLOAT 会先将 key 的值设为 0 ，再执行加法操作。
// 如果命令执行成功，那么 key 的值会被更新为（执行加法之后的）新值，并且新值会以字符串的形式返回给调用者。
func (r *Client) IncrByFloat(key string, value float64) *redis.FloatCmd {
	return r.client.IncrByFloat(r.k(key), value)
}

// MGet 返回所有(一个或多个)给定 key 的值。
// 如果给定的 key 里面，有某个 key 不存在，那么这个 key 返回特殊值 nil 。因此，该命令永不失败。
// 一个包含所有给定 key 的值的列表。
func (r *Client) MGet(keys ...string) *redis.SliceCmd {
	return r.client.MGet(r.ks(keys...)...)
}

// MSet 同时设置一个或多个 key-value 对。
func (r *Client) MSet(values ...interface{}) *redis.StatusCmd {
	return r.client.MSet(values...)
}

// MSetNX 同时设置一个或多个 key-value 对，当且仅当所有给定 key 都不存在。
// 即使只有一个给定 key 已存在， MSETNX 也会拒绝执行所有给定 key 的设置操作。
// MSETNX 是原子性的，因此它可以用作设置多个不同 key 表示不同字段(field)的唯一性逻辑对象(unique logic object)，所有字段要么全被设置，要么全不被设置。
func (r *Client) MSetNX(values ...interface{}) *redis.BoolCmd {
	return r.client.MSetNX(values...)
}

// Set 将字符串值 value 关联到 key 。
// 如果 key 已经持有其他值， SET 就覆写旧值，无视类型。
// 对于某个原本带有生存时间（TTL）的键来说， 当 SET 命令成功在这个键上执行时， 这个键原有的 TTL 将被清除。
func (r *Client) Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return r.client.Set(r.k(key), value, expiration)
}

// SetBit 对 key 所储存的字符串值，设置或清除指定偏移量上的位(bit)。
// 位的设置或清除取决于 value 参数，可以是 0 也可以是 1 。
// 当 key 不存在时，自动生成一个新的字符串值。
// 字符串会进行伸展(grown)以确保它可以将 value 保存在指定的偏移量上。当字符串值进行伸展时，空白位置以 0 填充。
// offset 参数必须大于或等于 0 ，小于 2^32 (bit 映射被限制在 512 MB 之内)。
func (r *Client) SetBit(key string, offset int64, value int) *redis.IntCmd {
	return r.client.SetBit(r.k(key), offset, value)
}

// SetNX 将 key 的值设为 value ，当且仅当 key 不存在。
// 若给定的 key 已经存在，则 SETNX 不做任何动作。
// SETNX 是『SET if Not eXists』(如果不存在，则 SET)的简写。
func (r *Client) SetNX(key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	return r.client.SetNX(r.k(key), value, expiration)
}

// SetXX -> SetNX
func (r *Client) SetXX(key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	return r.client.SetXX(r.k(key), value, expiration)
}

// SetRange 用 value 参数覆写(overwrite)给定 key 所储存的字符串值，从偏移量 offset 开始。
// 不存在的 key 当作空白字符串处理。
func (r *Client) SetRange(key string, offset int64, value string) *redis.IntCmd {
	return r.client.SetRange(r.k(key), offset, value)
}

// StrLen 返回 key 所储存的字符串值的长度。
// 当 key 储存的不是字符串值时，返回一个错误。
func (r *Client) StrLen(key string) *redis.IntCmd {
	return r.client.StrLen(r.k(key))
}

// HDel 删除哈希表 key 中的一个或多个指定域，不存在的域将被忽略。
func (r *Client) HDel(key string, fields ...string) *redis.IntCmd {
	return r.client.HDel(r.k(key), fields...)
}

//HExists 查看哈希表 key 中，给定域 field 是否存在。
func (r *Client) HExists(key, field string) *redis.BoolCmd {
	return r.client.HExists(r.k(key), field)
}

// HGet 返回哈希表 key 中给定域 field 的值。
func (r *Client) HGet(key, field string) *redis.StringCmd {
	return r.client.HGet(r.k(key), field)
}

// HGetAll 返回哈希表 key 中，所有的域和值。
// 在返回值里，紧跟每个域名(field name)之后是域的值(value)，所以返回值的长度是哈希表大小的两倍。
func (r *Client) HGetAll(key string) *redis.StringStringMapCmd {
	return r.client.HGetAll(r.k(key))
}

// HIncrBy 为哈希表 key 中的域 field 的值加上增量 increment 。
// 增量也可以为负数，相当于对给定域进行减法操作。
// 如果 key 不存在，一个新的哈希表被创建并执行 HINCRBY 命令。
// 如果域 field 不存在，那么在执行命令前，域的值被初始化为 0 。
// 对一个储存字符串值的域 field 执行 HINCRBY 命令将造成一个错误。
// 本操作的值被限制在 64 位(bit)有符号数字表示之内。
func (r *Client) HIncrBy(key, field string, incr int64) *redis.IntCmd {
	return r.client.HIncrBy(r.k(key), field, incr)
}

// HIncrByFloat 为哈希表 key 中的域 field 加上浮点数增量 increment 。
// 如果哈希表中没有域 field ，那么 HINCRBYFLOAT 会先将域 field 的值设为 0 ，然后再执行加法操作。
// 如果键 key 不存在，那么 HINCRBYFLOAT 会先创建一个哈希表，再创建域 field ，最后再执行加法操作。
func (r *Client) HIncrByFloat(key, field string, incr float64) *redis.FloatCmd {
	return r.client.HIncrByFloat(r.k(key), field, incr)
}

// HKeys 返回哈希表 key 中的所有域。
func (r *Client) HKeys(key string) *redis.StringSliceCmd {
	return r.client.HKeys(r.k(key))
}

//HLen 返回哈希表 key 中域的数量。
func (r *Client) HLen(key string) *redis.IntCmd {
	return r.client.HLen(r.k(key))
}

// HMGet 返回哈希表 key 中，一个或多个给定域的值。
// 如果给定的域不存在于哈希表，那么返回一个 nil 值。
// 因为不存在的 key 被当作一个空哈希表来处理，所以对一个不存在的 key 进行 HMGET 操作将返回一个只带有 nil 值的表。
func (r *Client) HMGet(key string, fields ...string) *redis.SliceCmd {
	return r.client.HMGet(r.k(key), fields...)
}

// HSet 将哈希表 key 中的域 field 的值设为 value 。
// 如果 key 不存在，一个新的哈希表被创建并进行 HSET 操作。
// 如果域 field 已经存在于哈希表中，旧值将被覆盖。
func (r *Client) HSet(key string, value ...interface{}) *redis.IntCmd {
	return r.client.HSet(r.k(key), value...)
}

// HMSet 同时将多个 field-value (域-值)对设置到哈希表 key 中。
// 此命令会覆盖哈希表中已存在的域。
// 如果 key 不存在，一个空哈希表被创建并执行 HMSET 操作。
func (r *Client) HMSet(key string, value ...interface{}) *redis.BoolCmd {
	return r.client.HMSet(r.k(key), value...)
}

// HSetNX 将哈希表 key 中的域 field 的值设置为 value ，当且仅当域 field 不存在。
// 若域 field 已经存在，该操作无效。
// 如果 key 不存在，一个新哈希表被创建并执行 HSETNX 命令。
func (r *Client) HSetNX(key, field string, value interface{}) *redis.BoolCmd {
	return r.client.HSetNX(r.k(key), field, value)
}

// HVals 返回哈希表 key 中所有域的值。
func (r *Client) HVals(key string) *redis.StringSliceCmd {
	return r.client.HVals(r.k(key))
}

// BLPop 是列表的阻塞式(blocking)弹出原语。
// 它是 LPop 命令的阻塞版本，当给定列表内没有任何元素可供弹出的时候，连接将被 BLPop 命令阻塞，直到等待超时或发现可弹出元素为止。
// 当给定多个 key 参数时，按参数 key 的先后顺序依次检查各个列表，弹出第一个非空列表的头元素。
func (r *Client) BLPop(timeout time.Duration, keys ...string) *redis.StringSliceCmd {
	return r.client.BLPop(timeout, r.ks(keys...)...)
}

// BRPopLPush 是 RPOPLPUSH 的阻塞版本，当给定列表 source 不为空时， BRPOPLPUSH 的表现和 RPOPLPUSH 一样。
// 当列表 source 为空时， BRPOPLPUSH 命令将阻塞连接，直到等待超时，或有另一个客户端对 source 执行 LPUSH 或 RPUSH 命令为止。
func (r *Client) BRPopLPush(source, destination string, timeout time.Duration) *redis.StringCmd {
	return r.client.BRPopLPush(r.k(source), r.k(destination), timeout)
}

// LIndex 返回列表 key 中，下标为 index 的元素。
// 下标(index)参数 start 和 stop 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。
// 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
// 如果 key 不是列表类型，返回一个错误。
func (r *Client) LIndex(key string, index int64) *redis.StringCmd {
	return r.client.LIndex(r.k(key), index)
}

// LInsert 将值 value 插入到列表 key 当中，位于值 pivot 之前或之后。
// 当 pivot 不存在于列表 key 时，不执行任何操作。
// 当 key 不存在时， key 被视为空列表，不执行任何操作。
// 如果 key 不是列表类型，返回一个错误。
func (r *Client) LInsert(key, op string, pivot, value interface{}) *redis.IntCmd {
	return r.client.LInsert(r.k(key), op, pivot, value)
}

// LInsertAfter 同 LInsert
func (r *Client) LInsertAfter(key string, pivot, value interface{}) *redis.IntCmd {
	return r.client.LInsertAfter(r.k(key), pivot, value)
}

// LInsertBefore 同 LInsert
func (r *Client) LInsertBefore(key string, pivot, value interface{}) *redis.IntCmd {
	return r.client.LInsertBefore(r.k(key), pivot, value)
}

// LLen 返回列表 key 的长度。
// 如果 key 不存在，则 key 被解释为一个空列表，返回 0 .
// 如果 key 不是列表类型，返回一个错误。
func (r *Client) LLen(key string) *redis.IntCmd {
	return r.client.LLen(r.k(key))
}

// LPop 移除并返回列表 key 的头元素。
func (r *Client) LPop(key string) *redis.StringCmd {
	return r.client.LPop(r.k(key))
}

// LPush 将一个或多个值 value 插入到列表 key 的表头
// 如果有多个 value 值，那么各个 value 值按从左到右的顺序依次插入到表头
// 如果 key 不存在，一个空列表会被创建并执行 LPush 操作。
// 当 key 存在但不是列表类型时，返回一个错误。
func (r *Client) LPush(key string, values ...interface{}) *redis.IntCmd {
	return r.client.LPush(r.k(key), values...)
}

// LPushX 将值 value 插入到列表 key 的表头，当且仅当 key 存在并且是一个列表。
// 和 LPUSH 命令相反，当 key 不存在时， LPUSHX 命令什么也不做。
func (r *Client) LPushX(key string, value interface{}) *redis.IntCmd {
	return r.client.LPushX(r.k(key), value)
}

// LRange 返回列表 key 中指定区间内的元素，区间以偏移量 start 和 stop 指定。
// 下标(index)参数 start 和 stop 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。
// 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
func (r *Client) LRange(key string, start, stop int64) *redis.StringSliceCmd {
	return r.client.LRange(r.k(key), start, stop)
}

// LRem 根据参数 count 的值，移除列表中与参数 value 相等的元素。
func (r *Client) LRem(key string, count int64, value interface{}) *redis.IntCmd {
	return r.client.LRem(r.k(key), count, value)
}

// LSet 将列表 key 下标为 index 的元素的值设置为 value 。
// 当 index 参数超出范围，或对一个空列表( key 不存在)进行 LSET 时，返回一个错误。
// 关于列表下标的更多信息，请参考 LINDEX 命令。
func (r *Client) LSet(key string, index int64, value interface{}) *redis.StatusCmd {
	return r.client.LSet(r.k(key), index, value)
}

// LTrim 对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。
// 举个例子，执行命令 LTRIM list 0 2 ，表示只保留列表 list 的前三个元素，其余元素全部删除。
// 下标(index)参数 start 和 stop 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。
// 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
// 当 key 不是列表类型时，返回一个错误。
func (r *Client) LTrim(key string, start, stop int64) *redis.StatusCmd {
	return r.client.LTrim(r.k(key), start, stop)
}

// BRPop 是列表的阻塞式(blocking)弹出原语。
// 它是 RPOP 命令的阻塞版本，当给定列表内没有任何元素可供弹出的时候，连接将被 BRPOP 命令阻塞，直到等待超时或发现可弹出元素为止。
// 当给定多个 key 参数时，按参数 key 的先后顺序依次检查各个列表，弹出第一个非空列表的尾部元素。
// 关于阻塞操作的更多信息，请查看 BLPOP 命令， BRPOP 除了弹出元素的位置和 BLPOP 不同之外，其他表现一致。
func (r *Client) BRPop(timeout time.Duration, keys ...string) *redis.StringSliceCmd {
	return r.client.BRPop(timeout, r.ks(keys...)...)
}

// RPopLPush 命令 RPOPLPUSH 在一个原子时间内，执行以下两个动作：
// 将列表 source 中的最后一个元素(尾元素)弹出，并返回给客户端。
// 将 source 弹出的元素插入到列表 destination ，作为 destination 列表的的头元素。
// 举个例子，你有两个列表 source 和 destination ， source 列表有元素 a, b, c ， destination 列表有元素 x, y, z ，执行 RPOPLPUSH source destination 之后， source 列表包含元素 a, b ， destination 列表包含元素 c, x, y, z ，并且元素 c 会被返回给客户端。
// 如果 source 不存在，值 nil 被返回，并且不执行其他动作。
// 如果 source 和 destination 相同，则列表中的表尾元素被移动到表头，并返回该元素，可以把这种特殊情况视作列表的旋转(rotation)操作。
func (r *Client) RPopLPush(source, destination string) *redis.StringCmd {
	return r.client.RPopLPush(r.k(source), r.k(destination))
}

// RPush 将一个或多个值 value 插入到列表 key 的表尾(最右边)。
// 如果有多个 value 值，那么各个 value 值按从左到右的顺序依次插入到表尾：比如对一个空列表 mylist 执行 RPUSH mylist a b c ，得出的结果列表为 a b c ，等同于执行命令 RPUSH mylist a 、 RPUSH mylist b 、 RPUSH mylist c 。
// 如果 key 不存在，一个空列表会被创建并执行 RPUSH 操作。
// 当 key 存在但不是列表类型时，返回一个错误。
func (r *Client) RPush(key string, values ...interface{}) *redis.IntCmd {
	return r.client.RPush(r.k(key), values...)
}

// RPushX 将值 value 插入到列表 key 的表尾，当且仅当 key 存在并且是一个列表。
// 和 RPUSH 命令相反，当 key 不存在时， RPUSHX 命令什么也不做。
func (r *Client) RPushX(key string, value interface{}) *redis.IntCmd {
	return r.client.RPushX(r.k(key), value)
}

// RPop 移除并返回列表 key 的尾元素。
func (r *Client) RPop(key string) *redis.StringCmd {
	return r.client.RPop(r.k(key))
}

// SAdd 将一个或多个 member 元素加入到集合 key 当中，已经存在于集合的 member 元素将被忽略。
// 假如 key 不存在，则创建一个只包含 member 元素作成员的集合。
// 当 key 不是集合类型时，返回一个错误。
func (r *Client) SAdd(key string, members ...interface{}) *redis.IntCmd {
	return r.client.SAdd(r.k(key), members...)
}

// SCard 返回集合 key 的基数(集合中元素的数量)。
func (r *Client) SCard(key string) *redis.IntCmd {
	return r.client.SCard(r.k(key))
}

// SDiff 返回一个集合的全部成员，该集合是所有给定集合之间的差集。
// 不存在的 key 被视为空集。
func (r *Client) SDiff(keys ...string) *redis.StringSliceCmd {
	return r.client.SDiff(r.ks(keys...)...)
}

// SDiffStore 这个命令的作用和 SDIFF 类似，但它将结果保存到 destination 集合，而不是简单地返回结果集。
// 如果 destination 集合已经存在，则将其覆盖。
// destination 可以是 key 本身。
func (r *Client) SDiffStore(destination string, keys ...string) *redis.IntCmd {
	return r.client.SDiffStore(r.k(destination), r.ks(keys...)...)
}

// SInter 返回一个集合的全部成员，该集合是所有给定集合的交集。
// 不存在的 key 被视为空集。
// 当给定集合当中有一个空集时，结果也为空集(根据集合运算定律)。
func (r *Client) SInter(keys ...string) *redis.StringSliceCmd {
	return r.client.SInter(r.ks(keys...)...)
}

// SInterStore 这个命令类似于 SINTER 命令，但它将结果保存到 destination 集合，而不是简单地返回结果集。
// 如果 destination 集合已经存在，则将其覆盖。
// destination 可以是 key 本身。
func (r *Client) SInterStore(destination string, keys ...string) *redis.IntCmd {
	return r.client.SInterStore(r.k(destination), r.ks(keys...)...)
}

// SIsMember 判断 member 元素是否集合 key 的成员。
func (r *Client) SIsMember(key string, member interface{}) *redis.BoolCmd {
	return r.client.SIsMember(r.k(key), member)
}

// SMembers 返回集合 key 中的所有成员。
// 不存在的 key 被视为空集合。
func (r *Client) SMembers(key string) *redis.StringSliceCmd {
	return r.client.SMembers(r.k(key))
}

// SMembersMap -> SMembers
func (r *Client) SMembersMap(key string) *redis.StringStructMapCmd {
	return r.client.SMembersMap(r.k(key))
}

// SMove 将 member 元素从 source 集合移动到 destination 集合。
// SMOVE 是原子性操作。
// 如果 source 集合不存在或不包含指定的 member 元素，则 SMOVE 命令不执行任何操作，仅返回 0 。否则， member 元素从 source 集合中被移除，并添加到 destination 集合中去。
// 当 destination 集合已经包含 member 元素时， SMOVE 命令只是简单地将 source 集合中的 member 元素删除。
// 当 source 或 destination 不是集合类型时，返回一个错误。
func (r *Client) SMove(source, destination string, member interface{}) *redis.BoolCmd {
	return r.client.SMove(r.k(source), r.k(destination), member)
}

// SPop 移除并返回集合中的一个随机元素。
// 如果只想获取一个随机元素，但不想该元素从集合中被移除的话，可以使用 SRANDMEMBER 命令。
func (r *Client) SPop(key string) *redis.StringCmd {
	return r.client.SPop(r.k(key))
}

// SPopN -> SPop
func (r *Client) SPopN(key string, count int64) *redis.StringSliceCmd {
	return r.client.SPopN(r.k(key), count)
}

// SRandMember 如果命令执行时，只提供了 key 参数，那么返回集合中的一个随机元素。
// 从 Redis 2.6 版本开始， SRANDMEMBER 命令接受可选的 count 参数：
// 如果 count 为正数，且小于集合基数，那么命令返回一个包含 count 个元素的数组，数组中的元素各不相同。如果 count 大于等于集合基数，那么返回整个集合。
// 如果 count 为负数，那么命令返回一个数组，数组中的元素可能会重复出现多次，而数组的长度为 count 的绝对值。
// 该操作和 SPOP 相似，但 SPOP 将随机元素从集合中移除并返回，而 SRANDMEMBER 则仅仅返回随机元素，而不对集合进行任何改动。
func (r *Client) SRandMember(key string) *redis.StringCmd {
	return r.client.SRandMember(r.k(key))
}

// SRandMemberN -> SRandMember
func (r *Client) SRandMemberN(key string, count int64) *redis.StringSliceCmd {
	return r.client.SRandMemberN(r.k(key), count)
}

// SRem 移除集合 key 中的一个或多个 member 元素，不存在的 member 元素会被忽略。
// 当 key 不是集合类型，返回一个错误。
func (r *Client) SRem(key string, members ...interface{}) *redis.IntCmd {
	return r.client.SRem(r.k(key), members...)
}

// SUnion 返回一个集合的全部成员，该集合是所有给定集合的并集。
// 不存在的 key 被视为空集。
func (r *Client) SUnion(keys ...string) *redis.StringSliceCmd {
	return r.client.SUnion(r.ks(keys...)...)
}

// SUnionStore 这个命令类似于 SUNION 命令，但它将结果保存到 destination 集合，而不是简单地返回结果集。
// 如果 destination 已经存在，则将其覆盖。
// destination 可以是 key 本身。
func (r *Client) SUnionStore(destination string, keys ...string) *redis.IntCmd {
	return r.client.SUnionStore(r.k(destination), r.ks(keys...)...)
}

// XAdd 将指定的流条目追加到指定key的流中。 如果key不存在，作为运行这个命令的副作用，将使用流的条目自动创建key。
func (r *Client) XAdd(a *redis.XAddArgs) *redis.StringCmd {
	return r.client.XAdd(a)
}

// XDel 从指定流中移除指定的条目，并返回成功删除的条目的数量，在传递的ID不存在的情况下， 返回的数量可能与传递的ID数量不同。
func (r *Client) XDel(stream string, ids ...string) *redis.IntCmd {
	return r.client.XDel(stream, ids...)
}

// XLen 返回流中的条目数。如果指定的key不存在，则此命令返回0，就好像该流为空。 但是请注意，与其他的Redis类型不同，零长度流是可能的，所以你应该调用TYPE 或者 EXISTS 来检查一个key是否存在。
// 一旦内部没有任何的条目（例如调用XDEL后），流不会被自动删除，因为可能还存在与其相关联的消费者组。
func (r *Client) XLen(stream string) *redis.IntCmd {
	return r.client.XLen(stream)
}

// XRange 此命令返回流中满足给定ID范围的条目。范围由最小和最大ID指定。所有ID在指定的两个ID之间或与其中一个ID相等（闭合区间）的条目将会被返回。
func (r *Client) XRange(stream, start, stop string) *redis.XMessageSliceCmd {
	return r.client.XRange(stream, start, stop)
}

// XRangeN -> XRange
func (r *Client) XRangeN(stream, start, stop string, count int64) *redis.XMessageSliceCmd {
	return r.client.XRangeN(stream, start, stop, count)
}

// XRevRange 此命令与XRANGE完全相同，但显著的区别是以相反的顺序返回条目，并以相反的顺序获取开始-结束参数：在XREVRANGE中，你需要先指定结束ID，再指定开始ID，该命令就会从结束ID侧开始生成两个ID之间（或完全相同）的所有元素。
func (r *Client) XRevRange(stream string, start, stop string) *redis.XMessageSliceCmd {
	return r.client.XRevRange(stream, start, stop)
}

// XRevRangeN -> XRevRange
func (r *Client) XRevRangeN(stream string, start, stop string, count int64) *redis.XMessageSliceCmd {
	return r.client.XRevRangeN(stream, start, stop, count)
}

// XRead 从一个或者多个流中读取数据，仅返回ID大于调用者报告的最后接收ID的条目。此命令有一个阻塞选项，用于等待可用的项目，类似于BRPOP或者BZPOPMIN等等。
func (r *Client) XRead(a *redis.XReadArgs) *redis.XStreamSliceCmd {
	return r.client.XRead(a)
}

//XReadStreams -> XRead
func (r *Client) XReadStreams(streams ...string) *redis.XStreamSliceCmd {
	return r.client.XReadStreams(streams...)
}

// XGroupCreate command
func (r *Client) XGroupCreate(stream, group, start string) *redis.StatusCmd {
	return r.client.XGroupCreate(stream, group, start)
}

// XGroupCreateMkStream command
func (r *Client) XGroupCreateMkStream(stream, group, start string) *redis.StatusCmd {
	return r.client.XGroupCreateMkStream(stream, group, start)
}

// XGroupSetID command
func (r *Client) XGroupSetID(stream, group, start string) *redis.StatusCmd {
	return r.client.XGroupSetID(stream, group, start)
}

// XGroupDestroy command
func (r *Client) XGroupDestroy(stream, group string) *redis.IntCmd {
	return r.client.XGroupDestroy(stream, group)
}

// XGroupDelConsumer command
func (r *Client) XGroupDelConsumer(stream, group, consumer string) *redis.IntCmd {
	return r.client.XGroupDelConsumer(stream, group, consumer)
}

// XReadGroup command
func (r *Client) XReadGroup(a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	return r.client.XReadGroup(a)
}

// XAck command
func (r *Client) XAck(stream, group string, ids ...string) *redis.IntCmd {
	return r.client.XAck(stream, group, ids...)
}

// XPending command
func (r *Client) XPending(stream, group string) *redis.XPendingCmd {
	return r.client.XPending(stream, group)
}

// XPendingExt command
func (r *Client) XPendingExt(a *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	return r.client.XPendingExt(a)
}

// XClaim command
func (r *Client) XClaim(a *redis.XClaimArgs) *redis.XMessageSliceCmd {
	return r.client.XClaim(a)
}

// XClaimJustID command
func (r *Client) XClaimJustID(a *redis.XClaimArgs) *redis.StringSliceCmd {
	return r.client.XClaimJustID(a)
}

// XTrim command
func (r *Client) XTrim(key string, maxLen int64) *redis.IntCmd {
	return r.client.XTrim(key, maxLen)
}

// XTrimApprox command
func (r *Client) XTrimApprox(key string, maxLen int64) *redis.IntCmd {
	return r.client.XTrimApprox(key, maxLen)
}

// XInfoGroups command
func (r *Client) XInfoGroups(key string) *redis.XInfoGroupsCmd {
	return r.client.XInfoGroups(key)
}

// BZPopMax 是有序集合命令 ZPOPMAX带有阻塞功能的版本。
func (r *Client) BZPopMax(timeout time.Duration, keys ...string) *redis.ZWithKeyCmd {
	return r.client.BZPopMax(timeout, r.ks(keys...)...)
}

// BZPopMin 是有序集合命令 ZPOPMIN带有阻塞功能的版本。
func (r *Client) BZPopMin(timeout time.Duration, keys ...string) *redis.ZWithKeyCmd {
	return r.client.BZPopMin(timeout, r.ks(keys...)...)
}

// ZAdd 将一个或多个 member 元素及其 score 值加入到有序集 key 当中。
// 如果某个 member 已经是有序集的成员，那么更新这个 member 的 score 值，并通过重新插入这个 member 元素，来保证该 member 在正确的位置上。
// score 值可以是整数值或双精度浮点数。
// 如果 key 不存在，则创建一个空的有序集并执行 ZADD 操作。
// 当 key 存在但不是有序集类型时，返回一个错误。
func (r *Client) ZAdd(key string, members ...*redis.Z) *redis.IntCmd {
	return r.client.ZAdd(r.k(key), members...)
}

// ZAddNX -> ZAdd
func (r *Client) ZAddNX(key string, members ...*redis.Z) *redis.IntCmd {
	return r.client.ZAddNX(r.k(key), members...)
}

// ZAddXX -> ZAdd
func (r *Client) ZAddXX(key string, members ...*redis.Z) *redis.IntCmd {
	return r.client.ZAddXX(r.k(key), members...)
}

// ZAddCh -> ZAdd
func (r *Client) ZAddCh(key string, members ...*redis.Z) *redis.IntCmd {
	return r.client.ZAddCh(r.k(key), members...)
}

// ZAddNXCh -> ZAdd
func (r *Client) ZAddNXCh(key string, members ...*redis.Z) *redis.IntCmd {
	return r.client.ZAddNXCh(r.k(key), members...)
}

// ZAddXXCh -> ZAdd
func (r *Client) ZAddXXCh(key string, members ...*redis.Z) *redis.IntCmd {
	return r.client.ZAddXXCh(r.k(key), members...)
}

// ZIncr Redis `ZADD key INCR score member` command.
func (r *Client) ZIncr(key string, member *redis.Z) *redis.FloatCmd {
	return r.client.ZIncr(r.k(key), member)
}

// ZIncrNX Redis `ZADD key NX INCR score member` command.
func (r *Client) ZIncrNX(key string, member *redis.Z) *redis.FloatCmd {
	return r.client.ZIncrNX(r.k(key), member)
}

// ZIncrXX Redis `ZADD key XX INCR score member` command.
func (r *Client) ZIncrXX(key string, member *redis.Z) *redis.FloatCmd {
	return r.client.ZIncrXX(r.k(key), member)
}

// ZCard 返回有序集 key 的基数。
func (r *Client) ZCard(key string) *redis.IntCmd {
	return r.client.ZCard(r.k(key))
}

// ZCount 返回有序集 key 中， score 值在 min 和 max 之间(默认包括 score 值等于 min 或 max )的成员的数量。
// 关于参数 min 和 max 的详细使用方法，请参考 ZRANGEBYSCORE 命令。
func (r *Client) ZCount(key, min, max string) *redis.IntCmd {
	return r.client.ZCount(r.k(key), min, max)
}

//ZLexCount -> ZCount
func (r *Client) ZLexCount(key, min, max string) *redis.IntCmd {
	return r.client.ZLexCount(r.k(key), min, max)
}

// ZIncrBy 为有序集 key 的成员 member 的 score 值加上增量 increment 。
// 可以通过传递一个负数值 increment ，让 score 减去相应的值，比如 ZINCRBY key -5 member ，就是让 member 的 score 值减去 5 。
// 当 key 不存在，或 member 不是 key 的成员时， ZINCRBY key increment member 等同于 ZADD key increment member 。
// 当 key 不是有序集类型时，返回一个错误。
// score 值可以是整数值或双精度浮点数。
func (r *Client) ZIncrBy(key string, increment float64, member string) *redis.FloatCmd {
	return r.client.ZIncrBy(r.k(key), increment, member)
}

// ZInterStore 计算给定的一个或多个有序集的交集，其中给定 key 的数量必须以 numkeys 参数指定，并将该交集(结果集)储存到 destination 。
// 默认情况下，结果集中某个成员的 score 值是所有给定集下该成员 score 值之和.
// 关于 WEIGHTS 和 AGGREGATE 选项的描述，参见 ZUNIONSTORE 命令。
func (r *Client) ZInterStore(key string, store *redis.ZStore) *redis.IntCmd {
	return r.client.ZInterStore(r.k(key), store)
}

// ZPopMax 删除并返回有序集合key中的最多count个具有最高得分的成员。
func (r *Client) ZPopMax(key string, count ...int64) *redis.ZSliceCmd {
	return r.client.ZPopMax(r.k(key), count...)
}

// ZPopMin 删除并返回有序集合key中的最多count个具有最低得分的成员。
func (r *Client) ZPopMin(key string, count ...int64) *redis.ZSliceCmd {
	return r.client.ZPopMin(r.k(key), count...)
}

// ZRange 返回有序集 key 中，指定区间内的成员。
// 其中成员的位置按 score 值递增(从小到大)来排序。
func (r *Client) ZRange(key string, start, stop int64) *redis.StringSliceCmd {
	return r.client.ZRange(r.k(key), start, stop)
}

// ZRangeWithScores -> ZRange
func (r *Client) ZRangeWithScores(key string, start, stop int64) *redis.ZSliceCmd {
	return r.client.ZRangeWithScores(r.k(key), start, stop)
}

// ZRangeByScore 返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。有序集成员按 score 值递增(从小到大)次序排列。
func (r *Client) ZRangeByScore(key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return r.client.ZRangeByScore(r.k(key), opt)
}

// ZRangeByLex -> ZRangeByScore
func (r *Client) ZRangeByLex(key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return r.client.ZRangeByLex(r.k(key), opt)
}

// ZRangeByScoreWithScores -> ZRangeByScore
func (r *Client) ZRangeByScoreWithScores(key string, opt *redis.ZRangeBy) *redis.ZSliceCmd {
	return r.client.ZRangeByScoreWithScores(r.k(key), opt)
}

// ZRank 返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递增(从小到大)顺序排列。
// 排名以 0 为底，也就是说， score 值最小的成员排名为 0 。
// 使用 ZREVRANK 命令可以获得成员按 score 值递减(从大到小)排列的排名。
func (r *Client) ZRank(key, member string) *redis.IntCmd {
	return r.client.ZRank(r.k(key), member)
}

// ZRem 移除有序集 key 中的一个或多个成员，不存在的成员将被忽略。
// 当 key 存在但不是有序集类型时，返回一个错误。
func (r *Client) ZRem(key string, members ...interface{}) *redis.IntCmd {
	return r.client.ZRem(r.k(key), members...)
}

// ZRemRangeByRank 移除有序集 key 中，指定排名(rank)区间内的所有成员。
// 区间分别以下标参数 start 和 stop 指出，包含 start 和 stop 在内。
// 下标参数 start 和 stop 都以 0 为底，也就是说，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。
// 你也可以使用负数下标，以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推
func (r *Client) ZRemRangeByRank(key string, start, stop int64) *redis.IntCmd {
	return r.client.ZRemRangeByRank(r.k(key), start, stop)
}

// ZRemRangeByScore 移除有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。
// 自版本2.1.6开始， score 值等于 min 或 max 的成员也可以不包括在内，详情请参见 ZRANGEBYSCORE 命令。
func (r *Client) ZRemRangeByScore(key, min, max string) *redis.IntCmd {
	return r.client.ZRemRangeByScore(r.k(key), min, max)
}

//ZRemRangeByLex -> ZRemRangeByScore
func (r *Client) ZRemRangeByLex(key, min, max string) *redis.IntCmd {
	return r.client.ZRemRangeByLex(r.k(key), min, max)
}

// ZRevRange 返回有序集 key 中，指定区间内的成员。
// 其中成员的位置按 score 值递减(从大到小)来排列。
// 具有相同 score 值的成员按字典序的逆序(reverse lexicographical order)排列。
// 除了成员按 score 值递减的次序排列这一点外， ZREVRANGE 命令的其他方面和 ZRANGE 命令一样。
func (r *Client) ZRevRange(key string, start, stop int64) *redis.StringSliceCmd {
	return r.client.ZRevRange(r.k(key), start, stop)
}

//ZRevRangeWithScores -> ZRevRange
func (r *Client) ZRevRangeWithScores(key string, start, stop int64) *redis.ZSliceCmd {
	return r.client.ZRevRangeWithScores(r.k(key), start, stop)
}

// ZRevRangeByScore 返回有序集 key 中， score 值介于 max 和 min 之间(默认包括等于 max 或 min )的所有的成员。有序集成员按 score 值递减(从大到小)的次序排列。
// 具有相同 score 值的成员按字典序的逆序(reverse lexicographical order )排列。
// 除了成员按 score 值递减的次序排列这一点外， ZREVRANGEBYSCORE 命令的其他方面和 ZRANGEBYSCORE 命令一样。
func (r *Client) ZRevRangeByScore(key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return r.client.ZRevRangeByScore(r.k(key), opt)
}

// ZRevRangeByLex -> ZRevRangeByScore
func (r *Client) ZRevRangeByLex(key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return r.client.ZRevRangeByLex(r.k(key), opt)
}

// ZRevRangeByScoreWithScores -> ZRevRangeByScore
func (r *Client) ZRevRangeByScoreWithScores(key string, opt *redis.ZRangeBy) *redis.ZSliceCmd {
	return r.client.ZRevRangeByScoreWithScores(r.k(key), opt)
}

// ZRevRank 返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递减(从大到小)排序。
// 排名以 0 为底，也就是说， score 值最大的成员排名为 0 。
// 使用 ZRANK 命令可以获得成员按 score 值递增(从小到大)排列的排名。
func (r *Client) ZRevRank(key, member string) *redis.IntCmd {
	return r.client.ZRevRank(r.k(key), member)
}

// ZScore 返回有序集 key 中，成员 member 的 score 值。
// 如果 member 元素不是有序集 key 的成员，或 key 不存在，返回 nil 。
func (r *Client) ZScore(key, member string) *redis.FloatCmd {
	return r.client.ZScore(r.k(key), member)
}

// ZUnionStore 计算给定的一个或多个有序集的并集，其中给定 key 的数量必须以 numkeys 参数指定，并将该并集(结果集)储存到 destination 。
// 默认情况下，结果集中某个成员的 score 值是所有给定集下该成员 score 值之 和 。
func (r *Client) ZUnionStore(dest string, store *redis.ZStore) *redis.IntCmd {
	return r.client.ZUnionStore(r.k(dest), store)
}

// PFAdd将指定元素添加到HyperLogLog
func (r *Client) PFAdd(key string, els ...interface{}) *redis.IntCmd {
	return r.client.PFAdd(r.k(key), els...)
}

// PFCount 返回HyperlogLog观察到的集合的近似基数。
func (r *Client) PFCount(keys ...string) *redis.IntCmd {
	return r.client.PFCount(r.ks(keys...)...)
}

// PFMerge N个不同的HyperLogLog合并为一个。
func (r *Client) PFMerge(dest string, keys ...string) *redis.StatusCmd {
	return r.client.PFMerge(r.k(dest), r.ks(keys...)...)
}

// BgRewriteAOF 异步重写附加文件
func (r *Client) BgRewriteAOF() *redis.StatusCmd {
	return r.client.BgRewriteAOF()
}

// BgSave 将数据集异步保存到磁盘
func (r *Client) BgSave() *redis.StatusCmd {
	return r.client.BgSave()
}

// ClientKill 杀掉客户端的连接
func (r *Client) ClientKill(ipPort string) *redis.StatusCmd {
	return r.client.ClientKill(ipPort)
}

// ClientKillByFilter is new style synx, while the ClientKill is old
// CLIENT KILL <option> [value] ... <option> [value]
func (r *Client) ClientKillByFilter(keys ...string) *redis.IntCmd {
	return r.client.ClientKillByFilter(r.ks(keys...)...)
}

// ClientList 获取客户端连接列表
func (r *Client) ClientList() *redis.StringCmd {
	return r.client.ClientList()
}

// ClientPause 停止处理来自客户端的命令一段时间
func (r *Client) ClientPause(dur time.Duration) *redis.BoolCmd {
	return r.client.ClientPause(dur)
}

// ClientID Returns the client ID for the current connection
func (r *Client) ClientID() *redis.IntCmd {
	return r.client.ClientID()
}

// ConfigGet 获取指定配置参数的值
func (r *Client) ConfigGet(parameter string) *redis.SliceCmd {
	return r.client.ConfigGet(parameter)
}

// ConfigResetStat 重置 INFO 命令中的某些统计数据
func (r *Client) ConfigResetStat() *redis.StatusCmd {
	return r.client.ConfigResetStat()
}

// ConfigSet 修改 redis 配置参数，无需重启
func (r *Client) ConfigSet(parameter, value string) *redis.StatusCmd {
	return r.client.ConfigSet(parameter, value)
}

// ConfigRewrite 对启动 Redis 服务器时所指定的 redis.conf 配置文件进行改写
func (r *Client) ConfigRewrite() *redis.StatusCmd {
	return r.client.ConfigRewrite()
}

// DBSize 返回当前数据库的 key 的数量
func (r *Client) DBSize() *redis.IntCmd {
	return r.client.DBSize()
}

// FlushAll 删除所有数据库的所有key
func (r *Client) FlushAll() *redis.StatusCmd {
	return r.client.FlushAll()
}

// FlushAllAsync 异步删除所有数据库的所有key
func (r *Client) FlushAllAsync() *redis.StatusCmd {
	return r.client.FlushAllAsync()
}

// FlushDB 删除当前数据库的所有key
func (r *Client) FlushDB() *redis.StatusCmd {
	return r.client.FlushDB()
}

// FlushDBAsync 异步删除当前数据库的所有key
func (r *Client) FlushDBAsync() *redis.StatusCmd {
	return r.client.FlushDBAsync()
}

// Info 获取 Redis 服务器的各种信息和统计数值
func (r *Client) Info(section ...string) *redis.StringCmd {
	return r.client.Info(section...)
}

// LastSave 返回最近一次 Redis 成功将数据保存到磁盘上的时间，以 UNIX 时间戳格式表示
func (r *Client) LastSave() *redis.IntCmd {
	return r.client.LastSave()
}

//Save 异步保存数据到硬盘
func (r *Client) Save() *redis.StatusCmd {
	return r.client.Save()
}

// Shutdown 关闭服务器
func (r *Client) Shutdown() *redis.StatusCmd {
	return r.client.Shutdown()
}

// ShutdownSave 异步保存数据到硬盘，并关闭服务器
func (r *Client) ShutdownSave() *redis.StatusCmd {
	return r.client.ShutdownSave()
}

// ShutdownNoSave 不保存数据到硬盘，并关闭服务器
func (r *Client) ShutdownNoSave() *redis.StatusCmd {
	return r.client.ShutdownNoSave()
}

// SlaveOf 将当前服务器转变为指定服务器的从属服务器(slave server)
func (r *Client) SlaveOf(host, port string) *redis.StatusCmd {
	return r.client.SlaveOf(host, port)
}

// Time 返回当前服务器时间
func (r *Client) Time() *redis.TimeCmd {
	return r.client.Time()
}

//Eval 执行 Lua 脚本。
func (r *Client) Eval(script string, keys []string, args ...interface{}) *redis.Cmd {
	return r.client.Eval(script, r.ks(keys...), args...)
}

//EvalSha 执行 Lua 脚本。
func (r *Client) EvalSha(sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return r.client.EvalSha(sha1, r.ks(keys...), args...)
}

// ScriptExists 查看指定的脚本是否已经被保存在缓存当中。
func (r *Client) ScriptExists(hashes ...string) *redis.BoolSliceCmd {
	return r.client.ScriptExists(hashes...)
}

// ScriptFlush 从脚本缓存中移除所有脚本。
func (r *Client) ScriptFlush() *redis.StatusCmd {
	return r.client.ScriptFlush()
}

// ScriptKill 杀死当前正在运行的 Lua 脚本。
func (r *Client) ScriptKill() *redis.StatusCmd {
	return r.client.ScriptKill()

}

// ScriptLoad 将脚本 script 添加到脚本缓存中，但并不立即执行这个脚本。
func (r *Client) ScriptLoad(script string) *redis.StringCmd {
	return r.client.ScriptLoad(script)
}

// DebugObject 获取 key 的调试信息
func (r *Client) DebugObject(key string) *redis.StringCmd {
	return r.client.DebugObject(r.k(key))
}

//Publish 将信息发送到指定的频道。
func (r *Client) Publish(channel string, message interface{}) *redis.IntCmd {
	return r.client.Publish(r.k(channel), message)
}

//PubSubChannels 订阅一个或多个符合给定模式的频道。
func (r *Client) PubSubChannels(pattern string) *redis.StringSliceCmd {
	return r.client.PubSubChannels(r.k(pattern))
}

// PubSubNumSub 查看订阅与发布系统状态。
func (r *Client) PubSubNumSub(channels ...string) *redis.StringIntMapCmd {
	return r.client.PubSubNumSub(r.ks(channels...)...)
}

// PubSubNumPat 用于获取redis订阅或者发布信息的状态
func (r *Client) PubSubNumPat() *redis.IntCmd {
	return r.client.PubSubNumPat()
}

//ClusterSlots 获取集群节点的映射数组
func (r *Client) ClusterSlots() *redis.ClusterSlotsCmd {
	return r.client.ClusterSlots()
}

// ClusterNodes Get Cluster config for the node
func (r *Client) ClusterNodes() *redis.StringCmd {
	return r.client.ClusterNodes()
}

// ClusterMeet Force a node cluster to handshake with another node
func (r *Client) ClusterMeet(host, port string) *redis.StatusCmd {
	return r.client.ClusterMeet(host, port)
}

// ClusterForget Remove a node from the nodes table
func (r *Client) ClusterForget(nodeID string) *redis.StatusCmd {
	return r.client.ClusterForget(nodeID)
}

// ClusterReplicate Reconfigure a node as a replica of the specified master node
func (r *Client) ClusterReplicate(nodeID string) *redis.StatusCmd {
	return r.client.ClusterReplicate(nodeID)

}

// ClusterResetSoft Reset a Redis Cluster node
func (r *Client) ClusterResetSoft() *redis.StatusCmd {
	return r.client.ClusterResetSoft()
}

// ClusterResetHard Reset a Redis Cluster node
func (r *Client) ClusterResetHard() *redis.StatusCmd {
	return r.client.ClusterResetHard()
}

// ClusterInfo Provides info about Redis Cluster node state
func (r *Client) ClusterInfo() *redis.StringCmd {
	return r.client.ClusterInfo()
}

// ClusterKeySlot Returns the hash slot of the specified key
func (r *Client) ClusterKeySlot(key string) *redis.IntCmd {
	return r.client.ClusterKeySlot(r.k(key))
}

// ClusterGetKeysInSlot Return local key names in the specified hash slot
func (r *Client) ClusterGetKeysInSlot(slot int, count int) *redis.StringSliceCmd {
	return r.client.ClusterGetKeysInSlot(slot, count)
}

// ClusterCountFailureReports Return the number of failure reports active for a given node
func (r *Client) ClusterCountFailureReports(nodeID string) *redis.IntCmd {
	return r.client.ClusterCountFailureReports(nodeID)
}

// ClusterCountKeysInSlot Return the number of local keys in the specified hash slot
func (r *Client) ClusterCountKeysInSlot(slot int) *redis.IntCmd {
	return r.client.ClusterCountKeysInSlot(slot)
}

// ClusterDelSlots Set hash slots as unbound in receiving node
func (r *Client) ClusterDelSlots(slots ...int) *redis.StatusCmd {
	return r.client.ClusterDelSlots(slots...)
}

// ClusterDelSlotsRange ->  ClusterDelSlots
func (r *Client) ClusterDelSlotsRange(min, max int) *redis.StatusCmd {
	return r.client.ClusterDelSlotsRange(min, max)
}

// ClusterSaveConfig Forces the node to save cluster state on disk
func (r *Client) ClusterSaveConfig() *redis.StatusCmd {
	return r.client.ClusterSaveConfig()
}

// ClusterSlaves List replica nodes of the specified master node
func (r *Client) ClusterSlaves(nodeID string) *redis.StringSliceCmd {
	return r.client.ClusterSlaves(nodeID)
}

// ClusterFailover Forces a replica to perform a manual failover of its master.
func (r *Client) ClusterFailover() *redis.StatusCmd {
	return r.client.ClusterFailover()
}

// ClusterAddSlots Assign new hash slots to receiving node
func (r *Client) ClusterAddSlots(slots ...int) *redis.StatusCmd {
	return r.client.ClusterAddSlots(slots...)
}

// ClusterAddSlotsRange -> ClusterAddSlots
func (r *Client) ClusterAddSlotsRange(min, max int) *redis.StatusCmd {
	return r.client.ClusterAddSlotsRange(min, max)
}

//GeoAdd 将指定的地理空间位置（纬度、经度、名称）添加到指定的key中
func (r *Client) GeoAdd(key string, geoLocation ...*redis.GeoLocation) *redis.IntCmd {
	return r.client.GeoAdd(r.k(key), geoLocation...)
}

// GeoPos 从key里返回所有给定位置元素的位置（经度和纬度）
func (r *Client) GeoPos(key string, members ...string) *redis.GeoPosCmd {
	return r.client.GeoPos(r.k(key), members...)
}

// GeoRadius 以给定的经纬度为中心， 找出某一半径内的元素
func (r *Client) GeoRadius(key string, longitude, latitude float64, query *redis.GeoRadiusQuery) *redis.GeoLocationCmd {
	return r.client.GeoRadius(r.k(key), longitude, latitude, query)
}

// GeoRadiusStore -> GeoRadius
func (r *Client) GeoRadiusStore(key string, longitude, latitude float64, query *redis.GeoRadiusQuery) *redis.IntCmd {
	return r.client.GeoRadiusStore(r.k(key), longitude, latitude, query)
}

// GeoRadiusByMember -> GeoRadius
func (r *Client) GeoRadiusByMember(key, member string, query *redis.GeoRadiusQuery) *redis.GeoLocationCmd {
	return r.client.GeoRadiusByMember(r.k(key), member, query)
}

//GeoRadiusByMemberStore 找出位于指定范围内的元素，中心点是由给定的位置元素决定
func (r *Client) GeoRadiusByMemberStore(key, member string, query *redis.GeoRadiusQuery) *redis.IntCmd {
	return r.client.GeoRadiusByMemberStore(r.k(key), member, query)
}

// GeoDist 返回两个给定位置之间的距离
func (r *Client) GeoDist(key string, member1, member2, unit string) *redis.FloatCmd {
	return r.client.GeoDist(r.k(key), member1, member2, unit)
}

// GeoHash 返回一个或多个位置元素的 Geohash 表示
func (r *Client) GeoHash(key string, members ...string) *redis.StringSliceCmd {
	return r.client.GeoHash(r.k(key), members...)
}

// ReadOnly Enables read queries for a connection to a cluster replica node
func (r *Client) ReadOnly() *redis.StatusCmd {
	return r.client.ReadOnly()
}

// ReadWrite Disables read queries for a connection to a cluster replica node
func (r *Client) ReadWrite() *redis.StatusCmd {
	return r.client.ReadWrite()
}

// MemoryUsage Estimate the memory usage of a key
func (r *Client) MemoryUsage(key string, samples ...int) *redis.IntCmd {
	return r.client.MemoryUsage(r.k(key), samples...)
}

// Subscribe 订阅给定的一个或多个频道的信息。
func (r *Client) Subscribe(channels ...string) *redis.PubSub {
	return r.client.Subscribe(r.ks(channels...)...)
}

// ErrNotImplemented not implemented error
var ErrNotImplemented = errors.New("Not implemented")
