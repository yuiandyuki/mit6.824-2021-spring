package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type Done chan GetReply

type Config struct {
	Conf      shardctrler.Config
	Committer int
}

func (c Config) String() string {
	return fmt.Sprintf("{%d %v}", c.Committer, c.Conf)
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck         *shardctrler.Clerk
	config      Config
	lastConfig  Config
	groups      map[int][]string
	cid         int32
	serversLen  int
	shardStates [shardctrler.NShards]ShardState
	kv          map[string]string
	dedup       map[int32]int64
	num         int64
	handoffCh   chan Handoff
	done        map[int]Done
	doneMu      sync.Mutex
	lastApplied int
}

type ShardState int

const (
	Serving ShardState = iota
	Pulling
	Pushing
)

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *ShardKV) copyDedup() map[int32]int64 {
	dedup := make(map[int32]int64)
	for cid, dup := range kv.dedup {
		dedup[cid] = dup
	}
	return dedup
}

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	var dedup map[int32]int64
	var kvmap map[string]string
	var shardStates [shardctrler.NShards]ShardState
	var lastConf Config
	var conf Config
	var gs map[int][]string
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if e := d.Decode(&dedup); e == nil {
		kv.dedup = dedup
	}
	if e := d.Decode(&kvmap); e == nil {
		kv.kv = kvmap
	}
	if e := d.Decode(&shardStates); e == nil {
		kv.shardStates = shardStates
	}
	if e := d.Decode(&lastConf); e == nil {
		kv.lastConfig = lastConf
	}
	if e := d.Decode(&conf); e == nil {
		kv.config = conf
	}
	if e := d.Decode(&gs); e == nil {
		kv.groups = gs
	}
}

func (kv *ShardKV) DoApply() {
	for v := range kv.applyCh {
		if v.CommandValid {
			if latest, ok := v.Command.(Config); ok {
				kv.applyConfig(latest, v.CommandIndex)
			} else {
				val, err := kv.applyMsg(v)
				if kv.isLeader() {
					kv.doneMu.Lock()
					ch := kv.done[v.CommandIndex]
					kv.doneMu.Unlock()
					if ch != nil {
						ch <- GetReply{err, val}
					}
				}
			}

			if kv.maxraftstate != -1 && kv.rf.GetStateSize() >= kv.maxraftstate {
				kv.Debug("checkSnapshot: kv.rf.GetStateSize(%d) >= kv.maxraftstate(%d)", kv.rf.GetStateSize(), kv.maxraftstate)
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				kv.mu.Lock()
				if err := e.Encode(kv.dedup); err != nil {
					panic(err)
				}
				if err := e.Encode(kv.kv); err != nil {
					panic(err)
				}
				if err := e.Encode(kv.shardStates); err != nil {
					panic(err)
				}
				if err := e.Encode(kv.lastConfig); err != nil {
					panic(err)
				}
				if err := e.Encode(kv.config); err != nil {
					panic(err)
				}
				if err := e.Encode(kv.groups); err != nil {
					panic(err)
				}
				kv.mu.Unlock()
				kv.rf.Snapshot(v.CommandIndex, w.Bytes())
			}
		} else if v.SnapshotValid {
			b := kv.rf.CondInstallSnapshot(v.SnapshotTerm, v.SnapshotIndex, v.SnapshotSeq, v.Snapshot)
			kv.Debug("CondInstallSnapshot %t SnapshotTerm=%d SnapshotIndex=%d SnapshotSeq=%d len(Snapshot)=%d", b, v.SnapshotTerm, v.SnapshotIndex, v.SnapshotSeq, len(v.Snapshot))
			if b {
				kv.lastApplied = v.SnapshotSeq
				kv.readSnapshot(v.Snapshot)
			}
		}
	}
}

func (kv *ShardKV) applyConfig(latest Config, commandIndex int) {
	if commandIndex <= kv.lastApplied {
		kv.Debug("reject Config due to <= Index. lastApplied=%d latest=%+v", kv.lastApplied, latest)
		return
	}
	kv.lastApplied = commandIndex
	kv.mu.Lock()
	kv.lastConfig = kv.config
	kv.config = latest
	for gid, servers := range latest.Conf.Groups {
		kv.groups[gid] = servers
	}
	kv.updateShardStatesLW()
}

func (kv *ShardKV) updateShardStatesLW() {
	latest := kv.config
	kv.Debug("applying Config  states=%v", kv.shardStates)
	handoff := make(map[int][]int)
	for shard, gid := range kv.lastConfig.Conf.Shards {
		target := kv.config.Conf.Shards[shard]
		// TODO
		if gid == kv.gid && target != kv.gid { // move from self to others
			handoff[target] = append(handoff[target], shard)
			kv.shardStates[shard] = Pushing
		} else if gid != 0 && gid != kv.gid && target == kv.gid { // move from others to self
			kv.shardStates[shard] = Pulling
		}
	}
	kv.Debug("applied Config  lastConfig=%+v latest=%+v updatedStates=%v", kv.lastConfig, latest, kv.shardStates)

	kv.mu.Unlock()
	if kv.isLeader() || kv.me == latest.Committer {
		kv.handoff(handoff, latest.Conf, kv.copyDedup())
	}
}

func (kv *ShardKV) handoff(handoff map[int][]int, latest shardctrler.Config, dedup map[int32]int64) {
	for gid, shards := range handoff {
		slice := make(map[string]string)
		for key := range kv.kv {
			for _, shard := range shards {
				if key2shard(key) == shard {
					slice[key] = kv.kv[key]
				}
			}
		}
		kv.Debug("handoff shards %v to gid %d: %v", shards, gid, slice)
		if servers, ok := latest.Groups[gid]; ok {
			kv.handoffCh <- Handoff{HandoffArgs{Num: latest.Num, Origin: kv.gid, Shards: shards, Kv: slice, Dedup: dedup}, gid, servers}
		} else {
			panic("no group to handoff")
		}
	}
}

// Todo
func (kv *ShardKV) applyMsg(v raft.ApplyMsg) (string, Err) {
	if v.CommandIndex <= kv.lastApplied {
		kv.Debug("reject ApplyMsg due to <= Index. lastApplied=%d v=%+v", kv.lastApplied, v)
		return "", ErrTimeout
	}
	kv.lastApplied = v.CommandIndex
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var key string
	switch args := v.Command.(type) {
	case GetArgs:
		key = args.Key
		if err := kv.checkKeyL(key); err != OK {
			kv.Debug("reject ApplyMsg due to failed checkKeyL=%s  v=%+v", err, v)
			return "", err
		}
		kv.Debug("applied Get => %s  args: {%d %v} config: %+v", kv.kv[key], v.CommandIndex, args, kv.config)
		return kv.kv[key], OK
	case PutAppendArgs:
		key = args.Key
		if err := kv.checkKeyL(key); err != OK {
			kv.Debug("reject ApplyMsg due to failed checkKeyL=%s  v=%+v", err, v)
			return "", err
		}
		if dup, ok := kv.dedup[args.ClientId]; ok && args.SequenceNum <= dup {
			kv.Debug("PutAppend duplicate found for %d  args=%+v", dup, args)
			return "", OK
		}
		if args.Type == PutOp {
			kv.kv[key] = args.Value
		} else {
			kv.kv[key] += args.Value
		}
		kv.dedup[args.ClientId] = args.SequenceNum
		kv.Debug("applied PutAppend => %s  args: {%d %+v} config: %+v", kv.kv[key], v.CommandIndex, args, kv.config)
		return "", OK
	case HandoffArgs:
		if dup, ok := kv.dedup[args.ClientId]; ok && args.SequenceNum <= dup {
			kv.Debug("HandoffArgs duplicate found for %d  args=%+v", dup, args)
			return "", OK
		}
		if args.Num > kv.config.Conf.Num {
			kv.Debug("reject Handoff due to args.Num %d > kv.config.Num %d. current=%+v args=%+v", args.Num, kv.config.Conf.Num, kv.config, args)
			return "", ErrTimeout
		} else if args.Num < kv.config.Conf.Num {
			kv.Debug("ignore Handoff due to args.Num %d < kv.config.Num %d. current=%+v args=%+v", args.Num, kv.config.Conf.Num, kv.config, args)
			return "", OK
		}
		allServing := true
		for _, shard := range args.Shards {
			if kv.shardStates[shard] != Serving {
				allServing = false
			}
		}
		if allServing {
			kv.Debug("ignore Handoff due to allServing. current=%+v args=%+v", kv.config, args)
			return "", OK
		}
		for k, v := range args.Kv {
			kv.kv[k] = v
		}
		for _, shard := range args.Shards {
			kv.shardStates[shard] = Serving
		}
		for cid, dup := range args.Dedup {
			if dup > kv.dedup[cid] {
				kv.dedup[cid] = dup
			}
		}
		kv.dedup[args.ClientId] = args.SequenceNum
		kv.Debug("applied HandoffArgs from gid %d => %v states: %v  args: %v", args.Origin, kv.config, kv.shardStates, args)
		return "", OK
	case HandoffDoneArgs:
		if args.Num > kv.config.Conf.Num {
			kv.Debug("reject HandoffDoneArgs due to args.Num %d > kv.config.Num %d. current=%+v args=%+v", args.Num, kv.config.Conf.Num, kv.config, args)
			return "", ErrTimeout
		} else if args.Num < kv.config.Conf.Num {
			kv.Debug("ignore HandoffDoneArgs due to args.Num %d < kv.config.Num %d. current=%+v args=%+v", args.Num, kv.config.Conf.Num, kv.config, args)
			return "", OK
		}
		for _, k := range args.Keys {
			delete(kv.kv, k)
		}
		for _, shard := range args.Shards {
			kv.shardStates[shard] = Serving
		}
		kv.Debug("handoff %v to %d done  states=%v", args.Shards, args.Receiver, kv.shardStates)
		return "", OK
	default:
		panic("uncovered ApplyMsg")
	}
}

const (
	UpdateConfigInterval     = 100 * time.Millisecond
	UpdateConfigPollInterval = 200 * time.Millisecond
	TimeoutInterval          = 500 * time.Millisecond
)

func (kv *ShardKV) DoUpdateConfig() {
updateConfig:
	for {
		time.Sleep(UpdateConfigInterval)
		if !kv.isLeader() {
			continue
		}
		kv.mu.Lock()
		kv.Debug("DoUpdateConfig states=%v currentConf=%v", kv.shardStates, kv.config)
		for _, state := range kv.shardStates {
			if state != Serving {
				kv.mu.Unlock()
				continue updateConfig
			}
		}
		num := kv.config.Conf.Num + 1
		kv.mu.Unlock()
		kv.rf.Start(Config{kv.mck.Query(num), kv.me})
	}
}

type Handoff struct {
	args    HandoffArgs
	target  int
	servers []string
}

type HandoffArgs struct {
	Args
	Num    int
	Origin int
	Shards []int
	Kv     map[string]string
	Dedup  map[int32]int64
}

type HandoffReply struct {
	Err Err
}

func (kv *ShardKV) Handoff(args *HandoffArgs, reply *HandoffReply) {
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	_, reply.Err = kv.startAndWait("Handoff", *args)
	if reply.Err == OK {
		var doneArgs HandoffDoneArgs
		doneArgs.Num, doneArgs.Receiver, doneArgs.Shards = args.Num, kv.gid, args.Shards
		for s := range args.Kv {
			doneArgs.Keys = append(doneArgs.Keys, s)
		}
		go kv.pollHandoffDone(doneArgs, args.Origin)
	}
}

func (kv *ShardKV) HandoffDone(args *HandoffDoneArgs, reply *HandoffDoneReply) {
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	_, reply.Err = kv.startAndWait("HandoffDone", *args)
}

func (kv *ShardKV) pollHandoffDone(args HandoffDoneArgs, origin int) {
	for {
		kv.mu.Lock()
		servers := kv.groups[origin]
		kv.mu.Unlock()
		if len(servers) <= 0 {
			panic("no servers to HandoffDone")
		}
		for _, si := range servers {
			var reply HandoffDoneReply
			ok := kv.sendHandoffDone(si, &args, &reply)
			kv.Debug("HandoffDone to origin %d %s ok=%t reply.Err=%s  args=%v+\n", origin, si, ok, reply.Err, args)
			if ok && reply.Err == OK {
				return
			}
			if ok && reply.Err == ErrWrongGroup {
				panic("handoff reply.Err == ErrWrongGroup")
			}
		}
		time.Sleep(UpdateConfigPollInterval)
	}
}

func (kv *ShardKV) DoPollHandoff() {
	for handoff := range kv.handoffCh {
		handoff.args.Args = Args{ClientId: kv.cid, SequenceNum: kv.num}
	nextHandoff:
		for {
			for _, si := range handoff.servers {
				var reply HandoffReply
				ok := kv.sendHandoff(si, &handoff.args, &reply)
				kv.Debug("handoff to target %d %s ok=%t reply.Err=%s  args=%v+\n", handoff.target, si, ok, reply.Err, handoff.args)
				if ok && reply.Err == OK {
					kv.num++
					break nextHandoff
				}
				if ok && reply.Err == ErrWrongGroup {
					panic("handoff reply.Err == ErrWrongGroup")
				}
			}
			time.Sleep(UpdateConfigPollInterval)
		}
	}
}

func (kv *ShardKV) sendHandoff(si string, args *HandoffArgs, reply *HandoffReply) bool {
	return kv.make_end(si).Call("ShardKV.Handoff", args, reply)
}

func (kv *ShardKV) sendHandoffDone(si string, args *HandoffDoneArgs, reply *HandoffDoneReply) bool {
	return kv.make_end(si).Call("ShardKV.HandoffDone", args, reply)
}

type HandoffDoneArgs struct {
	Err      Err
	Num      int
	Receiver int
	Keys     []string
	Shards   []int
}

type HandoffDoneReply struct {
	Err Err
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	v, err := kv.Command("Get", args.Key, *args)
	reply.Err = err
	if err == OK {
		reply.Value = v
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, reply.Err = kv.Command("PutAppend", args.Key, *args)
}

// Command args needs to be raw type (not pointer)
func (kv *ShardKV) Command(ty string, key string, args interface{}) (val string, err Err) {
	if !kv.isLeader() {
		return "", ErrWrongLeader
	}
	if err := kv.checkKey(key); err != OK {
		return "", err
	}
	return kv.startAndWait(ty, args)
}

// startAndWait args needs to be raw type (not pointer)
func (kv *ShardKV) startAndWait(ty string, cmd interface{}) (val string, err Err) {
	i, _, isLeader := kv.rf.Start(cmd)
	kv.mu.Lock()
	kv.Debug("raft start %s i=%d %+v  config: %+v states=%v", ty, i, cmd, kv.config, kv.shardStates)
	kv.mu.Unlock()
	if !isLeader {
		return "", ErrWrongLeader
	}
	ch := make(Done, 1)
	kv.doneMu.Lock()
	kv.done[i] = ch
	kv.doneMu.Unlock()
	select {
	case reply := <-ch:
		kv.Debug("raft %s done => %v  cmd: %+v ", ty, reply, cmd)
		return reply.Value, reply.Err
	case <-time.After(TimeoutInterval):
		return "", ErrTimeout
	}
}

func (kv *ShardKV) checkKey(key string) Err {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.checkKeyL(key)
}

func (kv *ShardKV) checkKeyL(key string) Err {
	shard := key2shard(key)
	if kv.config.Conf.Shards[shard] == kv.gid {
		if kv.shardStates[shard] == Serving {
			return OK
		}
		return ErrTimeout
	}
	return ErrWrongGroup
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) resumeHandoff() {
	handoff := make(map[int][]int) // gid -> shards
	for shard, gid := range kv.lastConfig.Conf.Shards {
		if kv.shardStates[shard] == Pushing {
			target := kv.config.Conf.Shards[shard]
			if gid == kv.gid && target != kv.gid { // move from self to others
				handoff[target] = append(handoff[target], shard)
			}
		}
	}
	kv.Debug("resumed handoff: %v", handoff)
	kv.handoff(handoff, kv.config.Conf, kv.copyDedup())
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(Config{})
	labgob.Register(HandoffArgs{})
	labgob.Register(HandoffDoneArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.serversLen = len(servers)
	rand.Seed(time.Now().UnixNano())
	kv.cid = rand.Int31()
	kv.num = 100

	kv.mck = shardctrler.MakeClerk(ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = &raft.Raft{}
	kv.rf.SdebugPrefix = fmt.Sprintf("%d ", gid)
	kv.rf.Initialize(servers, me, persister, kv.applyCh)
	kv.groups = make(map[int][]string)
	kv.kv = make(map[string]string)
	kv.dedup = make(map[int32]int64)
	kv.done = make(map[int]Done)
	kv.handoffCh = make(chan Handoff, shardctrler.NShards)
	kv.readSnapshot(persister.ReadSnapshot())
	kv.resumeHandoff()
	kv.Debug("StartServer cid=%d config=%v shardStates=%v  dedup=%v kv=%v", kv.cid, kv.config, kv.shardStates, kv.dedup, kv.kv)
	go kv.DoApply()
	go kv.DoUpdateConfig()
	go kv.DoPollHandoff()

	return kv
}

const Padding = "    "

func (kv *ShardKV) Debug(format string, a ...interface{}) {
	preamble := strings.Repeat(Padding, kv.me)
	epilogue := strings.Repeat(Padding, kv.serversLen-kv.me-1)
	l := ""
	if kv.isLeader() {
		l = "L "
	}
	prefix := fmt.Sprintf("%s%s S%d %s[SHARDKV] %s%d ", preamble, raft.Microseconds(time.Now()), kv.me, epilogue, l, kv.gid)
	format = prefix + format
	log.Print(fmt.Sprintf(format, a...))
}
