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


