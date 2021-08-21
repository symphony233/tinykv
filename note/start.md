# Start learning tinyKV

Wish me luckly...

What I learned before about Golang: Just [A tour of go](https://tour.golang.org/welcome/1) and something system call I used in [tiny-container](https://github.com/symphony233/tiny-container).

## Test case - make project1

Absolutely **panic**.

```
meng@ali-ecs:~/projects/tinykv$ make project1
GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./kv/server -run 1
=== RUN   TestRawGet1
--- FAIL: TestRawGet1 (0.00s)
panic: runtime error: invalid memory address or nil pointer dereference [recovered]
        panic: runtime error: invalid memory address or nil pointer dereference
[signal SIGSEGV: segmentation violation code=0x1 addr=0x18 pc=0xbc7db4]
```

I have no idea about that...

Just go to test code, the first one triggered the panic is `TestRawGet1`.

**Get to know the logic of test cases is a good way for me to learn the framework of the part of "project"**(I think...), because it will literally run the project in a 100 percent correct way.

### Golang test mechanism

See `/kv/server/server_test`.

the suffix `_test` is used to tell Golang's build-in test mechanism, the command `go test` contains test functions.

Functions implemented in `*_test` files will be called by command `go test`(`go test -v` for verbose output), each function has user-defined name for specific function needed to test. User can use `t.Fatalf()` to print error message, in this case, use `assert`.(assert is a comprehensive testing tools so I will only learn it when some methods occurred in the Talent Plan... orz)

### TestRawGet1()

What did `TestRawGet1()` do?
Let's read the code with proper comments I added into it.

- `conf := config.NewTestConfig()`
  package `config` consists of some struct which indicates many setting parameters in tinyKV including `StoreAddr`, `DBPath`(where to store the data in), etc. And some functions to initialize parameters.
  `NewTestConfig()` is used in here

```go
func NewTestConfig() *Config {
	return &Config{
		LogLevel:                 "info", //log
		Raft:                     true,   //Emmm... I know raft is a protocol for distribute system, but in project1 why it is set as true?(Maybe I will know later...)
        //raft parameters as following, it's not the time to figure it out.
		RaftBaseTickInterval:     50 * time.Millisecond,
		RaftHeartbeatTicks:       2,
		RaftElectionTimeoutTicks: 10,
		RaftLogGCTickInterval:    50 * time.Millisecond,
		// Assume the average size of entries is 1k.
		RaftLogGcCountLimit:                 128000,
		SplitRegionCheckTickInterval:        100 * time.Millisecond,
		SchedulerHeartbeatTickInterval:      100 * time.Millisecond,
		SchedulerStoreHeartbeatTickInterval: 500 * time.Millisecond,
		RegionMaxSize:                       144 * MB,
		RegionSplitSize:                     96 * MB,
        //Directory to store data
		DBPath:                              "/tmp/badger",
	}
}
```

- `s := standalone_storage.NewStandAloneStorage(conf)`

Instantiate a standalone storage.

Let's see `kv/standalone_storage/standalone_storage.go`...

Interesting, nearly empty at all. Okay, that's what we need to implement in project1.

**All** functions in this file are needed to implement.

- `s.Start()`
- `server := NewServer(s)`
- `defer cleanUpTestData(conf)`
- `defer s.Stop()`

I think they do what you see as their names...

- `cf := engine_util.CfDefault`

`engine_util` is mentioned in official Project1 doc.

> Badger doesn’t give support for column families. `engine_util` package (`kv/util/engine_util`) simulates column families by adding a prefix to keys. For example, a key key that belongs to a specific column family cf is stored as \${cf}\_\${key}. It wraps badger to provide operations with CFs, and also offers many useful helper functions. So you should do all read/write operations through engine_util provided methods. Please read util/engine_util/doc.go to learn more.

So we can infer that `engine_util` contains a lot of method for operating storage engine. `doc.go` describes the storage engine.

- `engines`: a data structure for keeping engines required by unistore.
- `write_batch`: code to batch writes into a single, atomic 'transaction'.
- `cf_iterator`: code to iterate over a whole column family in badger.

In `weite_batch.go` CfDefault is assigned as "**default**" with type `string`.

- `Set(s, cf, []byte{99}, []byte{42})`

Function `Set()` was defined in test file, we check for the args then we can see parameter `cf` with type **string** which prove the statement above. It set 99 as **key** and 42 as **value** into the storage.

**Return value** : error
In function, it returns `s.Write()`.
And the argument `s` is the standalone storage we need to instantiate before.

the interfaces of `Write` and `Read` are defined in `storage.go`.

> **The standalone storage is just a wrapper of badger kv API which is provided by these to methods.**

- `req := &kvrpcpb.RawGetRequest{`
  `Key: []byte{99},`
  `Cf: cf,`
  `}`

It's calling grpc's generated code.
Message structure

```proto
message RawGetRequest {
    Context context = 1;
    bytes key = 2;
    string cf = 3;
}
```

Construct a struct and assigned it to variable `req`.

- `resp, err := server.RawGet(nil, req)`

We need to implement `RawGet()` in standalone server.

- assert.Nil(t, err)
- assert.Equal(t, []byte{42}, resp.Value)

Just assert.

Okay now we get to know a approximate running logic.

### Coding

Key words in official markdown documents: **The standalone storage is just a warpper of badger key/value API**.

As it is said in [三篇文章了解 TiDB 技术内幕 - 说存储](https://pingcap.com/zh/blog/tidb-internal-1), TiKV uses RockDB as its storage engine, so TiKV do not write data into disk directly. Similar to tinyKV, I think we should write data into `Badger`, then a transparent procedure about writing data into disk will finish by `Badger` instead of on my own.

Recall to `engine_util/doc.go`, `engines.go` contains data struct and methods of badger endgine.

#### Strategy

It's a truely comprehensive framework, just I describe above, I began with the **test case** to understand how these API would be called during the application. Then I happened to know the `engine_util` package warpped various methods about `badger` which is used for tinyKV as an standalone storage engine.

Following the instruction given by official markdown.

**Firstly**, implement the interfaces in `standalone_storage.go`, mainly the action of engine. The most important thing is realize APIs provided in `engine_util` package, and distinguish which function we should use.

For example, function `GetCF()` in `Reader` has receiver `StandAloneReader`(I named it by myself), so `GetCf()` in `engine_util` cannot be used in `Reader` due to the receiver, we should we `GetCFFromTxn()`.

Omit details...

**Secondly**, implement the server. Luckily I have some basic knowledge about `gRPC`. When we implement the server it's a little like the server in C/S framework. We just understand what the request what to do and implement the corresponding funtion.

For example, `RawDelete()` function with reciver server is used to accept the detetion request. What we need to do is to call the storage engine's write function and pass the corresponding parameters, then return the response.

All we need to focus on is the detail logic and remember `Discard()` and `Close()`.

Like in `RawScan()` we need to read `cf_iterator.go` to know the wrapped `badge` APIs...

Omit details...


Then Project1 done!

**Note:** though I passed all the test case of project1, there may be some unreasonable code I had written and need to review as progressing.