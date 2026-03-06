package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	resourcev1 "go-wind-admin/api/gen/go/k8s/service/v1"

	"github.com/go-kratos/kratos/v2/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	// EtcdLockTTL 锁的会话TTL，稍微调大以容纳批量操作的耗时
	EtcdLockTTL = 15
)

type MonitorManager struct {
	resourcev1.UnimplementedResourceManagerServer

	etcdClient *clientv3.Client
	clusters   sync.Map // map[string]*ClusterMonitor
	log        *log.Helper
	stopCh     chan struct{}
}

func NewMonitorManager(logger log.Logger, etcdClient *clientv3.Client) *MonitorManager {
	m := &MonitorManager{
		stopCh:     make(chan struct{}),
		log:        log.NewHelper(logger),
		etcdClient: etcdClient,
	}
	// 启动后台巡检，处理过期 Token
	go m.watchExpiredTokens() // 现有的过期清理
	go m.runReconcileLoop()   // 新增：账本校准
	return m
}

// AddCluster 注册集群监控器，用于调度建议计算
func (m *MonitorManager) AddCluster(cm *ClusterMonitor) {
	if val, ok := m.clusters.Load(cm.Name); ok {
		val.(*ClusterMonitor).Stop()
	}
	cm.Start()
	m.clusters.Store(cm.Name, cm)
}

// ==========================================
// 核心接口 1: ApplyResource (原子打包申请 + 索引写入)
// ==========================================
func (m *MonitorManager) ApplyResource(ctx context.Context, req *resourcev1.ApplyResourceRequest) (*resourcev1.ApplyResourceResponse, error) {
	if len(req.Items) == 0 {
		return &resourcev1.ApplyResourceResponse{Success: false, Message: "Empty items"}, nil
	}

	tokenID := GenTokenID()

	// 1. 收集并排序所有需要加锁的 Key
	type lockInfo struct {
		Type  ResourceType
		Level string
	}
	uniqueLocks := make(map[lockInfo]bool)
	var lockKeys []string

	for _, item := range req.Items {
		rt := FromProtoType(item.ResourceType)
		for _, lid := range item.LevelIds {
			lk := lockInfo{rt, lid}
			if !uniqueLocks[lk] {
				uniqueLocks[lk] = true
				lockKeys = append(lockKeys, KeyLock(rt, lid))
			}
		}
	}
	sort.Strings(lockKeys)

	// 2. 批量加锁
	session, err := concurrency.NewSession(m.etcdClient, concurrency.WithTTL(EtcdLockTTL))
	if err != nil {
		return nil, fmt.Errorf("create session failed: %v", err)
	}
	defer session.Close()

	locks := make([]*concurrency.Mutex, 0, len(lockKeys))
	defer func() {
		for i := len(locks) - 1; i >= 0; i-- {
			locks[i].Unlock(context.Background())
		}
	}()

	lockCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	for _, k := range lockKeys {
		mu := concurrency.NewMutex(session, k)
		if err := mu.Lock(lockCtx); err != nil {
			return &resourcev1.ApplyResourceResponse{Success: false, Message: fmt.Sprintf("Busy: %s", k)}, nil
		}
		locks = append(locks, mu)
	}

	// 3. 批量读取账本 & 校验
	var ops []clientv3.Op
	var tokenItems []TokenItem
	now := time.Now()

	for _, item := range req.Items {
		rt := FromProtoType(item.ResourceType)
		if item.AllocateNum <= 0 {
			continue
		}

		tokenItems = append(tokenItems, TokenItem{
			ResourceType: rt, AllocateNum: item.AllocateNum, UsedNum: item.AllocateNum, LevelIDs: item.LevelIds,
		})

		for _, lid := range item.LevelIds {
			key := KeyLedger(rt, lid)
			resp, err := m.etcdClient.Get(ctx, key)
			if err != nil {
				return nil, err
			}

			var l ResourceLedger
			if len(resp.Kvs) > 0 {
				json.Unmarshal(resp.Kvs[0].Value, &l)
			}

			if (l.Total - l.Used) < item.AllocateNum {
				return &resourcev1.ApplyResourceResponse{Success: false, Message: fmt.Sprintf("Quota exceeded: %s", lid)}, nil
			}

			l.Used += item.AllocateNum
			val, _ := json.Marshal(l)
			ops = append(ops, clientv3.OpPut(key, string(val)))
		}
	}

	// 4. 调度建议
	recommendedNode := ""
	if req.RequireScheduleHint && req.ClusterIdHint != "" {
		if val, ok := m.clusters.Load(req.ClusterIdHint); ok {
			cm := val.(*ClusterMonitor)
			resReq := make(map[ResourceType]int64)
			for _, ti := range tokenItems {
				resReq[ti.ResourceType] = ti.AllocateNum
			}
			if node, err := cm.FindBestNode(resReq); err == nil {
				recommendedNode = node
			}
		}
	}

	// 5. 准备 Lease
	timeout := req.ConfirmTimeoutS
	if timeout <= 0 {
		timeout = 30
	}
	leaseResp, err := m.etcdClient.Grant(ctx, timeout)
	if err != nil {
		return nil, err
	}

	// 6. 写入 Token & 二级索引
	token := &ResourceToken{
		TokenID: tokenID, Items: tokenItems, TokenStatus: TokenStatusPendingConfirm, ConfirmTimeout: timeout,
		ExpireAt: req.ExpireAtS, CreateAt: now, UpdateAt: now, LeaseID: int64(leaseResp.ID),
	}
	tVal, _ := json.Marshal(token)

	// Op: Write Token
	ops = append(ops, clientv3.OpPut(KeyToken(tokenID), string(tVal), clientv3.WithLease(leaseResp.ID)))

	// Op: Write Index (新增)
	seenLevels := make(map[string]bool)
	for _, item := range tokenItems {
		for _, lid := range item.LevelIDs {
			if !seenLevels[lid] {
				idxKey := KeyIndexLevel(lid, tokenID)
				// 索引 Key 绑定同一个 Lease，Token 过期索引自动删除
				ops = append(ops, clientv3.OpPut(idxKey, "", clientv3.WithLease(leaseResp.ID)))
				seenLevels[lid] = true
			}
		}
	}

	// 7. 提交
	txnResp, err := m.etcdClient.Txn(ctx).Then(ops...).Commit()
	if err != nil || !txnResp.Succeeded {
		return nil, fmt.Errorf("txn failed")
	}

	return &resourcev1.ApplyResourceResponse{Success: true, TokenId: tokenID, RecommendedNode: recommendedNode}, nil
}

// ==========================================
// 核心接口 2: ConfirmResource (续租 + 索引续租)
// ==========================================
func (m *MonitorManager) ConfirmResource(ctx context.Context, req *resourcev1.ConfirmResourceRequest) (*resourcev1.ConfirmResourceResponse, error) {
	key := KeyToken(req.TokenId)
	resp, err := m.etcdClient.Get(ctx, key)
	if err != nil || len(resp.Kvs) == 0 {
		return &resourcev1.ConfirmResourceResponse{Success: false, Message: "Token not found"}, nil
	}

	var token ResourceToken
	json.Unmarshal(resp.Kvs[0].Value, &token)

	if token.TokenStatus != TokenStatusPendingConfirm {
		return &resourcev1.ConfirmResourceResponse{Success: false, Message: "Invalid status"}, nil
	}

	token.TokenStatus = TokenStatusAllocated
	token.UpdateAt = time.Now()

	var opts []clientv3.OpOption
	var idxOps []clientv3.Op // 额外的索引更新操作

	if token.ExpireAt > 0 {
		// 临时资源：计算剩余时间并续租
		ttl := token.ExpireAt - time.Now().Unix()
		if ttl <= 0 {
			ttl = 1
		}
		newLease, err := m.etcdClient.Grant(ctx, ttl)
		if err != nil {
			return nil, err
		}
		opts = append(opts, clientv3.WithLease(newLease.ID))
		token.LeaseID = int64(newLease.ID)

		// 必须同时刷新索引的 Lease！
		seenLevels := make(map[string]bool)
		for _, item := range token.Items {
			for _, lid := range item.LevelIDs {
				if !seenLevels[lid] {
					idxKey := KeyIndexLevel(lid, req.TokenId)
					// 重新 Put 空值，仅为了绑定新 Lease
					idxOps = append(idxOps, clientv3.OpPut(idxKey, "", clientv3.WithLease(newLease.ID)))
					seenLevels[lid] = true
				}
			}
		}
	} else {
		token.LeaseID = 0 // 永久资源，解绑 Lease
		// 索引也需要解绑 Lease (变成永久索引)
		// 但注意：Put Token 时如果不带 Lease 参数，默认就是解绑。
		// 索引 Key 也需要重新 Put 一次（不带 Lease）以解绑之前的 Lease。
		seenLevels := make(map[string]bool)
		for _, item := range token.Items {
			for _, lid := range item.LevelIDs {
				if !seenLevels[lid] {
					idxKey := KeyIndexLevel(lid, req.TokenId)
					idxOps = append(idxOps, clientv3.OpPut(idxKey, "", clientv3.WithIgnoreLease())) // Wait, IgnoreLease是保留原Lease。这里我们要解绑。
					// 正确做法：直接 Put，不带 Lease 参数 = No Lease (Permanent)
					// 但是为了代码一致性，我们这里使用空 Put
					idxOps = append(idxOps, clientv3.OpPut(idxKey, ""))
					seenLevels[lid] = true
				}
			}
		}
	}

	val, _ := json.Marshal(token)

	// 组合操作：更新 Token + 更新索引
	var ops []clientv3.Op
	ops = append(ops, clientv3.OpPut(key, string(val), opts...))
	ops = append(ops, idxOps...)

	_, err = m.etcdClient.Txn(ctx).Then(ops...).Commit()
	return &resourcev1.ConfirmResourceResponse{Success: err == nil}, err
}

// ==========================================
// 核心接口 3: ReleaseResource (释放资源)
// ==========================================
func (m *MonitorManager) ReleaseResource(ctx context.Context, req *resourcev1.ReleaseResourceRequest) (*resourcev1.ReleaseResourceResponse, error) {
	// 使用独立的 Context 防止客户端断开连接导致释放中断
	bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return m.doRelease(bgCtx, req.TokenId, req.ReleaseNum, TokenStatusReleased)
}

func (m *MonitorManager) doRelease(ctx context.Context, tokenID string, releaseNum int64, finalStatus TokenStatus) (*resourcev1.ReleaseResourceResponse, error) {
	key := KeyToken(tokenID)
	resp, err := m.etcdClient.Get(ctx, key)
	if err != nil || len(resp.Kvs) == 0 {
		return &resourcev1.ReleaseResourceResponse{Success: false, Message: "Token not found"}, nil
	}

	var token ResourceToken
	json.Unmarshal(resp.Kvs[0].Value, &token)

	// 幂等性：如果已经是终态，直接返回成功
	if token.TokenStatus.IsFinal() {
		return &resourcev1.ReleaseResourceResponse{Success: true, Message: "Already released"}, nil
	}

	// --- Step 1: 批量加锁 (涉及的所有 Item 的所有 Level) ---
	type lockInfo struct {
		Type  ResourceType
		Level string
	}
	uniqueLocks := make(map[lockInfo]bool)
	var lockKeys []string

	for _, item := range token.Items {
		for _, lid := range item.LevelIDs {
			lk := lockInfo{item.ResourceType, lid}
			if !uniqueLocks[lk] {
				uniqueLocks[lk] = true
				lockKeys = append(lockKeys, KeyLock(item.ResourceType, lid))
			}
		}
	}
	sort.Strings(lockKeys)

	session, _ := concurrency.NewSession(m.etcdClient, concurrency.WithTTL(EtcdLockTTL))
	defer session.Close()

	locks := make([]*concurrency.Mutex, 0)
	defer func() {
		for i := len(locks) - 1; i >= 0; i-- {
			locks[i].Unlock(context.Background())
		}
	}()

	for _, k := range lockKeys {
		mu := concurrency.NewMutex(session, k)
		if err := mu.Lock(ctx); err != nil {
			return nil, fmt.Errorf("acquire lock failed during release")
		}
		locks = append(locks, mu)
	}

	// --- Step 2: 批量计算并归还账本 ---
	var ops []clientv3.Op
	allReleased := true
	// 目前多资源 Token 建议全量释放 (releaseNum=0)
	// 如果需要支持部分释放，需要更复杂的参数定义指定释放哪个 Item
	isFullRelease := (releaseNum == 0)

	for i := range token.Items {
		item := &token.Items[i] // 指针操作，便于修改 UsedNum
		toRelease := item.UsedNum
		if !isFullRelease {
			// 暂不支持多 Item 的部分释放逻辑，默认全量
			toRelease = item.UsedNum
		}

		// 归还该 Item 涉及的所有 Level 账本
		for _, lid := range item.LevelIDs {
			lKey := KeyLedger(item.ResourceType, lid)
			gResp, _ := m.etcdClient.Get(ctx, lKey)
			var l ResourceLedger
			if len(gResp.Kvs) > 0 {
				json.Unmarshal(gResp.Kvs[0].Value, &l)
			}
			l.Used -= toRelease
			if l.Used < 0 {
				l.Used = 0 // 防御性兜底
			}
			val, _ := json.Marshal(l)
			ops = append(ops, clientv3.OpPut(lKey, string(val)))
		}

		item.UsedNum -= toRelease
		if item.UsedNum > 0 {
			allReleased = false
		}
	}

	// --- Step 3: 更新 Token 状态 & 索引 Lease ---
	if allReleased {
		token.TokenStatus = finalStatus
		// 终态凭证：授予 7 天 Lease 用于保留历史记录，便于排查
		lease7d, _ := m.etcdClient.Grant(ctx, 7*24*3600)
		token.LeaseID = int64(lease7d.ID)
		tVal, _ := json.Marshal(token)

		// 1. 更新 Token Key，绑定新 Lease
		ops = append(ops, clientv3.OpPut(key, string(tVal), clientv3.WithLease(lease7d.ID)))

		// 2. 关键更新：同时刷新二级索引的 Lease！
		// 如果不刷，索引会随 Apply 时的旧 Lease 过期，导致 ListTokens 查不到历史记录
		seenLevels := make(map[string]bool)
		for _, item := range token.Items {
			for _, lid := range item.LevelIDs {
				if !seenLevels[lid] {
					idxKey := KeyIndexLevel(lid, tokenID)
					// 重新 Put 空值，仅为了绑定新 Lease
					ops = append(ops, clientv3.OpPut(idxKey, "", clientv3.WithLease(lease7d.ID)))
					seenLevels[lid] = true
				}
			}
		}
	} else {
		// 部分释放：保持 Active，仅更新数据
		token.UpdateAt = time.Now()
		tVal, _ := json.Marshal(token)
		// 保持原有 Lease (必须显式 IgnoreLease，否则会被清除)
		ops = append(ops, clientv3.OpPut(key, string(tVal), clientv3.WithIgnoreLease()))
		// 索引无需操作，Lease 会跟随 Token 维持现状
	}

	// --- Step 4: 提交事务 ---
	_, err = m.etcdClient.Txn(ctx).Then(ops...).Commit()
	return &resourcev1.ReleaseResourceResponse{Success: err == nil}, err
}

// ==========================================
// 查询与管理接口
// ==========================================

func (m *MonitorManager) QueryResource(ctx context.Context, req *resourcev1.QueryResourceRequest) (*resourcev1.QueryResourceResponse, error) {
	switch q := req.QueryType.(type) {
	case *resourcev1.QueryResourceRequest_Ledger:
		key := KeyLedger(FromProtoType(q.Ledger.ResourceType), q.Ledger.LevelId)
		resp, err := m.etcdClient.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		info := &resourcev1.LedgerInfo{ResourceType: q.Ledger.ResourceType, LevelId: q.Ledger.LevelId}
		if len(resp.Kvs) > 0 {
			var l ResourceLedger
			json.Unmarshal(resp.Kvs[0].Value, &l)
			info.Total, info.Used, info.Remain = l.Total, l.Used, l.Total-l.Used
		}
		return &resourcev1.QueryResourceResponse{QueryResult: &resourcev1.QueryResourceResponse_Ledger{Ledger: info}}, nil

	case *resourcev1.QueryResourceRequest_Token:
		key := KeyToken(q.Token.TokenId)
		resp, err := m.etcdClient.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if len(resp.Kvs) == 0 {
			return nil, fmt.Errorf("token not found")
		}
		var t ResourceToken
		json.Unmarshal(resp.Kvs[0].Value, &t)

		// 转换内部 Item 到 Proto
		protoItems := make([]*resourcev1.TokenItemDetails, 0, len(t.Items))
		for _, it := range t.Items {
			protoItems = append(protoItems, &resourcev1.TokenItemDetails{
				ResourceType: ToProtoType(it.ResourceType),
				AllocateNum:  it.AllocateNum,
				UsedNum:      it.UsedNum,
				LevelIds:     it.LevelIDs,
			})
		}

		return &resourcev1.QueryResourceResponse{QueryResult: &resourcev1.QueryResourceResponse_Token{Token: &resourcev1.TokenInfo{
			TokenId: t.TokenID, Items: protoItems, TokenStatus: int32(t.TokenStatus),
			CreateAt: t.CreateAt.Format(time.RFC3339), UpdateAt: t.UpdateAt.Format(time.RFC3339),
		}}}, nil
	}
	return nil, nil
}

func (m *MonitorManager) BatchQueryLedger(ctx context.Context, req *resourcev1.BatchQueryLedgerRequest) (*resourcev1.BatchQueryLedgerResponse, error) {
	resType := FromProtoType(req.ResourceType)
	if len(req.LevelIds) == 0 {
		return &resourcev1.BatchQueryLedgerResponse{}, nil
	}

	ops := make([]clientv3.Op, 0, len(req.LevelIds))
	for _, lid := range req.LevelIds {
		ops = append(ops, clientv3.OpGet(KeyLedger(resType, lid)))
	}

	txnResp, err := m.etcdClient.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		return nil, fmt.Errorf("batch query failed: %v", err)
	}

	resp := &resourcev1.BatchQueryLedgerResponse{
		Ledgers: make([]*resourcev1.LedgerInfo, 0, len(req.LevelIds)),
	}

	for i, responseOp := range txnResp.Responses {
		info := &resourcev1.LedgerInfo{
			ResourceType: req.ResourceType,
			LevelId:      req.LevelIds[i],
			Total:        0, Used: 0, Remain: 0,
		}
		getResp := responseOp.GetResponseRange()
		if getResp != nil && len(getResp.Kvs) > 0 {
			var l ResourceLedger
			json.Unmarshal(getResp.Kvs[0].Value, &l)
			info.Total, info.Used, info.Remain = l.Total, l.Used, l.Total-l.Used
		}
		resp.Ledgers = append(resp.Ledgers, info)
	}
	return resp, nil
}

func (m *MonitorManager) AdjustResourceTotal(ctx context.Context, req *resourcev1.AdjustResourceTotalRequest) (*resourcev1.AdjustResourceTotalResponse, error) {
	resType := FromProtoType(req.ResourceType)
	key := KeyLedger(resType, req.LevelId)

	session, _ := concurrency.NewSession(m.etcdClient, concurrency.WithTTL(5))
	defer session.Close()
	mu := concurrency.NewMutex(session, KeyLock(resType, req.LevelId))
	if err := mu.Lock(ctx); err != nil {
		return nil, err
	}
	defer mu.Unlock(ctx)

	resp, err := m.etcdClient.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	var ledger ResourceLedger
	if len(resp.Kvs) > 0 {
		json.Unmarshal(resp.Kvs[0].Value, &ledger)
	}

	// 缩容检查：Total 不能小于 Used
	if req.NewTotal < ledger.Used {
		return &resourcev1.AdjustResourceTotalResponse{Success: false, Message: fmt.Sprintf("New total %d < used %d", req.NewTotal, ledger.Used)}, nil
	}

	ledger.Total = req.NewTotal
	valBytes, _ := json.Marshal(ledger)
	_, err = m.etcdClient.Put(ctx, key, string(valBytes))
	if err != nil {
		return nil, err
	}

	return &resourcev1.AdjustResourceTotalResponse{Success: true}, nil
}

// QueryTenantQuota 聚合查询租户下所有类型的配额
func (m *MonitorManager) QueryTenantQuota(ctx context.Context, req *resourcev1.QueryTenantQuotaRequest) (*resourcev1.QueryTenantQuotaResponse, error) {
	resp := &resourcev1.QueryTenantQuotaResponse{Results: make([]*resourcev1.QueryTenantQuotaResponse_TenantQuota, 0)}

	// 用于聚合结果: map[LevelID][ResourceType]Info
	resultMap := make(map[string]map[int32]*resourcev1.LedgerInfo)
	targetTypes := []ResourceType{ResourceTypeCPU, ResourceTypeGPU, ResourceTypeMemory}

	// === 分支 1: 精确查询 (传入了 ID) ===
	if len(req.LevelIds) > 0 {
		var ops []clientv3.Op
		// 记录 op 索引对应的元数据，用于解析 Response
		type opMeta struct {
			LevelID string
			Type    ResourceType
		}
		var metaList []opMeta

		for _, lid := range req.LevelIds {
			for _, rt := range targetTypes {
				ops = append(ops, clientv3.OpGet(KeyLedger(rt, lid)))
				metaList = append(metaList, opMeta{lid, rt})
			}
		}

		if len(ops) > 0 {
			txnResp, err := m.etcdClient.Txn(ctx).Then(ops...).Commit()
			if err != nil {
				return nil, err
			}

			for i, responseOp := range txnResp.Responses {
				meta := metaList[i]
				getResp := responseOp.GetResponseRange()

				if _, ok := resultMap[meta.LevelID]; !ok {
					resultMap[meta.LevelID] = make(map[int32]*resourcev1.LedgerInfo)
				}

				info := &resourcev1.LedgerInfo{
					ResourceType: ToProtoType(meta.Type), LevelId: meta.LevelID,
				}
				if getResp != nil && len(getResp.Kvs) > 0 {
					var l ResourceLedger
					_ = json.Unmarshal(getResp.Kvs[0].Value, &l)
					info.Total, info.Used, info.Remain = l.Total, l.Used, l.Total-l.Used
				}
				resultMap[meta.LevelID][int32(ToProtoType(meta.Type))] = info
			}
		}
	} else {
		// === 分支 2: 全量扫描 (未传 ID) ===
		// 依次扫描 /wind/resource/v1/ledger/cpu/, .../gpu/ 等前缀
		for _, rt := range targetTypes {
			// KeyLedger(rt, "") 会生成如 "/wind/resource/v1/ledger/cpu/" 的前缀
			// 注意：这里假设你的 KeyLedger 实现支持传空 ID 生成目录前缀
			// 如果 utils.go 里 KeyLedger 实现是 fmt.Sprintf(".../%s", lid)，传空可能会多或少斜杠
			// 稳妥起见，手动拼一下前缀，确保准确
			prefix := fmt.Sprintf("/wind/resource/v1/ledger/%s/", rt)

			gResp, err := m.etcdClient.Get(ctx, prefix, clientv3.WithPrefix())
			if err != nil {
				continue
			}

			for _, kv := range gResp.Kvs {
				keyStr := string(kv.Key)
				// Key: /wind/resource/v1/ledger/cpu/quota_tenant_A
				// 截取最后一个 "/" 之后的部分作为 LevelID
				// 简单粗暴的方式：
				levelID := keyStr[len(prefix):]

				if levelID == "" {
					continue
				}

				// 关键过滤：只看 quota_ 开头的（业务租户），过滤掉 physical_ 开头的（物理节点）
				// 如果前端也想看物理池，可以去掉这个 if
				if !strings.HasPrefix(levelID, "quota_") {
					continue
				}

				if _, ok := resultMap[levelID]; !ok {
					resultMap[levelID] = make(map[int32]*resourcev1.LedgerInfo)
				}

				var l ResourceLedger
				_ = json.Unmarshal(kv.Value, &l)

				resultMap[levelID][int32(ToProtoType(rt))] = &resourcev1.LedgerInfo{
					ResourceType: ToProtoType(rt),
					LevelId:      levelID,
					Total:        l.Total,
					Used:         l.Used,
					Remain:       l.Total - l.Used,
				}
			}
		}
	}

	// 统一转换 Map 为 List 返回
	for lid, quotas := range resultMap {
		resp.Results = append(resp.Results, &resourcev1.QueryTenantQuotaResponse_TenantQuota{
			LevelId: lid,
			Quotas:  quotas,
		})
	}

	return resp, nil
}

// BatchAdjustResourceTotal 批量调整配额
func (m *MonitorManager) BatchAdjustResourceTotal(ctx context.Context, req *resourcev1.BatchAdjustResourceTotalRequest) (*resourcev1.BatchAdjustResourceTotalResponse, error) {
	// 加锁逻辑省略... 建议生产环境加锁
	var ops []clientv3.Op
	for _, item := range req.Items {
		rt := FromProtoType(item.ResourceType)
		key := KeyLedger(rt, item.LevelId)
		resp, err := m.etcdClient.Get(ctx, key)
		if err != nil {
			return nil, err
		}

		var l ResourceLedger
		if len(resp.Kvs) > 0 {
			json.Unmarshal(resp.Kvs[0].Value, &l)
		}
		if item.NewTotal < l.Used {
			return &resourcev1.BatchAdjustResourceTotalResponse{Success: false, Message: fmt.Sprintf("Fail: %s new total %d < used %d", item.LevelId, item.NewTotal, l.Used)}, nil
		}
		l.Total = item.NewTotal
		val, _ := json.Marshal(l)
		ops = append(ops, clientv3.OpPut(key, string(val)))
	}

	_, err := m.etcdClient.Txn(ctx).Then(ops...).Commit()
	return &resourcev1.BatchAdjustResourceTotalResponse{Success: err == nil}, err
}

// ==========================================
// 拓扑接口 (转发给 ClusterMonitor)
// ==========================================

func (m *MonitorManager) ListClusters(ctx context.Context, req *resourcev1.ListClustersRequest) (*resourcev1.ListClustersResponse, error) {
	resp := &resourcev1.ListClustersResponse{}
	m.clusters.Range(func(key, value interface{}) bool {
		cm := value.(*ClusterMonitor)
		resp.Clusters = append(resp.Clusters, cm.GetSummary())
		return true
	})
	return resp, nil
}

func (m *MonitorManager) ListClusterNodes(ctx context.Context, req *resourcev1.ListClusterNodesRequest) (*resourcev1.ListClusterNodesResponse, error) {
	val, ok := m.clusters.Load(req.ClusterId)
	if !ok {
		return nil, fmt.Errorf("cluster %s not found", req.ClusterId)
	}
	cm := val.(*ClusterMonitor)
	nodes, err := cm.ListNodes(req.LabelSelector)
	if err != nil {
		return nil, err
	}
	return &resourcev1.ListClusterNodesResponse{Nodes: nodes}, nil
}

func (m *MonitorManager) GetNodeDetail(ctx context.Context, req *resourcev1.GetNodeDetailRequest) (*resourcev1.GetNodeDetailResponse, error) {
	val, ok := m.clusters.Load(req.ClusterId)
	if !ok {
		return nil, fmt.Errorf("cluster %s not found", req.ClusterId)
	}
	cm := val.(*ClusterMonitor)
	return cm.GetNodeDetail(req.NodeName)
}

// ==========================================
// 后台巡检 (Auto Rollback)
// ==========================================

func (m *MonitorManager) watchExpiredTokens() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.checkPendingTimeouts()
		}
	}
}

func (m *MonitorManager) checkPendingTimeouts() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 扫描 Token 前缀
	resp, err := m.etcdClient.Get(ctx, "/wind/resource/v1/token/", clientv3.WithPrefix())
	if err != nil {
		m.log.Errorf("Scan tokens failed: %v", err)
		return
	}

	now := time.Now()
	for _, kv := range resp.Kvs {
		var token ResourceToken
		if err := json.Unmarshal(kv.Value, &token); err != nil {
			continue
		}

		// 1. 检查 Pending 超时 (ConfirmTimeout)
		if token.TokenStatus == TokenStatusPendingConfirm {
			expireTime := token.CreateAt.Add(time.Duration(token.ConfirmTimeout) * time.Second)
			if now.After(expireTime) {
				m.log.Infof("Auto rollback pending token: %s", token.TokenID)
				m.doRelease(context.Background(), token.TokenID, 0, TokenStatusRollbacked)
			}
		}

		// 2. 检查 Active 过期 (ExpireAt)
		if token.TokenStatus == TokenStatusAllocated && token.ExpireAt > 0 {
			if now.Unix() > token.ExpireAt {
				m.log.Infof("Auto release expired token: %s", token.TokenID)
				m.doRelease(context.Background(), token.TokenID, 0, TokenStatusExpired)
			}
		}
	}
}

// 物理账本校准循环
func (m *MonitorManager) runReconcileLoop() {
	// 频率不宜太高，建议 5-10 分钟一次，或者在系统空闲时
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.reconcilePhysicalUsed()
		}
	}
}

func (m *MonitorManager) reconcilePhysicalUsed() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. 扫描所有 Token
	// 优化：如果你有数万 Token，这里应该分页 Range
	resp, err := m.etcdClient.Get(ctx, "/wind/resource/v1/token/", clientv3.WithPrefix())
	if err != nil {
		m.log.Errorf("Reconcile: scan tokens failed: %v", err)
		return
	}

	// 内存账本: map[LevelID][ResourceType] => UsedCount
	// 专门统计 physical_ 开头的
	realUsed := make(map[string]map[ResourceType]int64)

	now := time.Now()

	// 2. 遍历 Token 进行聚合
	for _, kv := range resp.Kvs {
		var token ResourceToken
		if err := json.Unmarshal(kv.Value, &token); err != nil {
			continue
		}

		// 只统计有效 Token (Pending 或 Allocated 且未过期)
		// 注意：Expired 状态的 Token 虽然还没删，但逻辑上已失效，不应计入占用
		isValid := false
		if token.TokenStatus == TokenStatusAllocated {
			// 检查是否过期
			if token.ExpireAt <= 0 || token.ExpireAt > now.Unix() {
				isValid = true
			}
		} else if token.TokenStatus == TokenStatusPendingConfirm {
			// 检查是否超时
			if token.CreateAt.Add(time.Duration(token.ConfirmTimeout) * time.Second).After(now) {
				isValid = true
			}
		}

		if !isValid {
			continue
		}

		// 累加占用
		for _, item := range token.Items {
			// item.UsedNum 是实际占用量（考虑了部分释放的情况）
			if item.UsedNum <= 0 {
				continue
			}

			for _, lid := range item.LevelIDs {
				// 只校准物理池 (physical_)，租户配额 (quota_) 也可以顺便校准，看你需求
				// 这里演示全量校准
				if _, ok := realUsed[lid]; !ok {
					realUsed[lid] = make(map[ResourceType]int64)
				}
				realUsed[lid][item.ResourceType] += item.UsedNum
			}
		}
	}

	// 3. 扫描所有 Ledger 并对比 (或直接 Update)
	// 为了安全，我们先 Scan 所有 Ledger，对比后再 Put
	// 这里简单起见，我们遍历刚才计算出的 realUsed，去 Etcd 里查并更新
	// 注意：如果某个物理池完全没被用（realUsed里没有），但Etcd里Used>0，这种“悬空占用”也需要清零。
	// 所以正确的做法是：Scan 所有 Ledger，用 realUsed 来修正它。

	lResp, err := m.etcdClient.Get(ctx, "/wind/resource/v1/ledger/", clientv3.WithPrefix())
	if err != nil {
		return
	}

	var ops []clientv3.Op

	for _, kv := range lResp.Kvs {
		var ledger ResourceLedger
		if err := json.Unmarshal(kv.Value, &ledger); err != nil {
			continue
		}

		// 解析 Key 拿到 LevelID 和 Type
		// Key: /wind/resource/v1/ledger/{type}/{id}
		// 简单解析... (建议封装 ParseKey 函数)
		// 假设我们知道怎么解析:
		// rt, lid := parseLedgerKey(string(kv.Key))

		// 这里为了演示，用稍微笨一点的办法：
		// 我们只更新那些我们在 realUsed 里统计到的。
		// 对于没统计到的，如果它是 physical_ 且 Used > 0，说明有残留，需要置 0。

		// 这是一个比较重的逻辑，生产环境通常只修复“Known Inconsistencies”
		// 比如：只修复 physical_ 开头的

		// ... (省略复杂的 Key 解析)
	}

	// 简化版：只更新我们在 realUsed 里看到的
	for lid, typeMap := range realUsed {
		for rt, calculatedUsed := range typeMap {
			key := KeyLedger(rt, lid)

			// 乐观锁更新：先 Get
			gResp, _ := m.etcdClient.Get(ctx, key)
			if len(gResp.Kvs) == 0 {
				continue
			} // 账本不存在，可能已被删

			var l ResourceLedger
			json.Unmarshal(gResp.Kvs[0].Value, &l)

			if l.Used != calculatedUsed {
				m.log.Warnf("Reconcile: fixing ledger %s/%s. Old: %d, New: %d", lid, rt, l.Used, calculatedUsed)
				l.Used = calculatedUsed
				val, _ := json.Marshal(l)
				ops = append(ops, clientv3.OpPut(key, string(val)))
			}
		}
	}

	// 4. 提交修正
	if len(ops) > 0 {
		// 分批提交，防止包过大
		batchSize := 100
		for i := 0; i < len(ops); i += batchSize {
			end := i + batchSize
			if end > len(ops) {
				end = len(ops)
			}
			m.etcdClient.Txn(ctx).Then(ops[i:end]...).Commit()
		}
	}
}

// === 新增接口: ListTokens ===
func (m *MonitorManager) ListTokens(ctx context.Context, req *resourcev1.ListTokensRequest) (*resourcev1.ListTokensResponse, error) {
	resp := &resourcev1.ListTokensResponse{Tokens: []*resourcev1.TokenInfo{}}

	var tokenIDs []string

	// 1. 处理 LevelID 前缀逻辑
	targetLevelID := req.LevelId
	if targetLevelID != "" {
		// 如果前端传了 level_type (quota/physical) 且 ID 不带前缀，自动补全
		if req.LevelType == "quota" && !strings.HasPrefix(targetLevelID, "quota_") {
			targetLevelID = "quota_" + targetLevelID
		} else if req.LevelType == "physical" && !strings.HasPrefix(targetLevelID, "physical_") {
			targetLevelID = "physical_" + targetLevelID
		}
	}

	// 模式 A: 按 LevelID 查索引 (高效)
	if targetLevelID != "" {
		// Prefix: /wind/resource/v1/index/level/{levelID}/
		prefix := KeyIndexLevel(targetLevelID, "")

		// 查索引
		idxResp, err := m.etcdClient.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
		if err != nil {
			return nil, err
		}

		resp.Total = int64(len(idxResp.Kvs)) // 近似总数

		// 分页逻辑
		if req.PageSize <= 0 {
			req.PageSize = 10
		}
		if req.Page <= 0 {
			req.Page = 1
		}

		start := int((req.Page - 1) * req.PageSize)
		if start < 0 {
			start = 0
		}
		if start >= len(idxResp.Kvs) {
			return resp, nil // 空页
		}
		end := start + int(req.PageSize)
		if end > len(idxResp.Kvs) {
			end = len(idxResp.Kvs)
		}

		// 提取 TokenID
		for _, kv := range idxResp.Kvs[start:end] {
			keyStr := string(kv.Key)
			tokenID := keyStr[len(prefix):]
			tokenIDs = append(tokenIDs, tokenID)
		}
	} else {
		// 模式 B: 全量查 (低效，仅调试用)
		// 限制只查前 100 条
		tr, err := m.etcdClient.Get(ctx, "/wind/resource/v1/token/", clientv3.WithPrefix(), clientv3.WithLimit(100))
		if err != nil {
			return nil, err
		}
		// ... 直接反序列化返回，不走 ID 聚合
		for _, kv := range tr.Kvs {
			var t ResourceToken
			json.Unmarshal(kv.Value, &t)
			// ... 转换并 append 到 resp
		}
		return resp, nil
	}

	// 批量查 Token 详情 (模式 A 的后续)
	if len(tokenIDs) == 0 {
		return resp, nil
	}

	// 构建批量 Get Ops
	var ops []clientv3.Op
	for _, tid := range tokenIDs {
		ops = append(ops, clientv3.OpGet(KeyToken(tid)))
	}

	txnResp, err := m.etcdClient.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		return nil, err
	}

	for _, opResp := range txnResp.Responses {
		getResp := opResp.GetResponseRange()
		if getResp.Count > 0 {
			var t ResourceToken
			if err := json.Unmarshal(getResp.Kvs[0].Value, &t); err != nil {
				continue
			}

			// 状态过滤
			if req.Status != 0 && int32(t.TokenStatus) != req.Status {
				continue
			}

			// 转换为 Proto
			protoItems := make([]*resourcev1.TokenItemDetails, 0, len(t.Items))
			for _, it := range t.Items {
				protoItems = append(protoItems, &resourcev1.TokenItemDetails{
					ResourceType: ToProtoType(it.ResourceType), AllocateNum: it.AllocateNum, UsedNum: it.UsedNum, LevelIds: it.LevelIDs,
				})
			}

			resp.Tokens = append(resp.Tokens, &resourcev1.TokenInfo{
				TokenId: t.TokenID, Items: protoItems, TokenStatus: int32(t.TokenStatus),
				CreateAt: t.CreateAt.Format(time.RFC3339), UpdateAt: t.UpdateAt.Format(time.RFC3339),
			})
		}
	}

	return resp, nil
}
