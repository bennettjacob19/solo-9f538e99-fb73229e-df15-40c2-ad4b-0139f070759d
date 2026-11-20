# 节点排空值班台

检修 Kubernetes 节点前的排空工具：FastAPI 网页选节点，先封锁，再按轮次驱逐
Pod，只在集群确认后才提示“可下线”。

另有一个 **NodePort 本地后端排查** 页面：从某个节点的 NodePort 连不上服务时，
填写命名空间、Service 名和 NodePort，查看各节点可用的本地后端 IP 和实际端口。

## 规则

### 节点排空

| 要求 | 实现 |
| --- | --- |
| 先标记不可调度，再列出本轮 Pod | `DrainManager.start()` 先 `PATCH` 节点 `spec.unschedulable=true`，再 `GET` 该节点的 Pod；封锁失败则不会产生轮次、也不会展示任何待驱逐 Pod |
| 保留 DaemonSet 与静态 Pod | `Pod.exempt_reason`：`DaemonSet`、`静态 Pod`（mirror pod / config.source=file,http）、以及已结束（Succeeded/Failed）的 Pod 均不驱逐，也不阻挡“可下线” |
| 用 Eviction 按 UID 驱逐 | `POST /api/v1/namespaces/{ns}/pods/{name}/eviction`（`policy/v1`），`deleteOptions.preconditions.uid` 为列表时的 UID |
| 同名替代者留待续排 | 若同名 Pod 的 UID 已变化，旧 UID 的驱逐决定不迁移；新对象在执行中被标为 `留待续排`，在重新读取时进入新的一轮 |
| 两节点并行不超共用 PDB 额度 | 预算判定完全交给 API Server：它返回 429 即视为“受预算阻挡”。应用不做本地配额缓存，因此并行轮次共享同一份额度 |
| 页面标出受预算阻挡的 Pod，同轮继续 | 429 只影响该 Pod 的状态行（`受预算阻挡`），循环继续处理下一个 Pod |
| 中断保留封锁与已发生的驱逐 | 中断只阻止后续驱逐；节点保持封锁，轮次记录与已发出的驱逐都保留 |
| 继续排空重新读取集群 | `resume()` 重新 `GET` 节点与 Pod，必要时补封锁，然后 `refresh()` 对账，不会沿用旧快照 |
| 获准驱逐但未离开显示“待退出” | 驱逐成功后状态为 `待退出`，直到再次检查发现对象已消失才变为 `已离开` |
| 复核后才显示可下线 | `_verify_offline()` 每次独立重读：节点当前仍不可调度 **且** 已无普通 Pod，才置为“可下线” |
| 本地演练端 | `simulator/` 提供本地 Kubernetes API 演练端（节点/Pod/PDB/Eviction/Service/EndpointSlice），网页可调预算、同名替换、令 Pod 退出，以及切换端点状态、断开订阅、删除 Slice、令旧版本过期 |
| docker compose 启动 | `docker compose up --build` |

### NodePort 本地后端排查

| 要求 | 实现 |
| --- | --- |
| 只支持 `externalTrafficPolicy=Local` 的 IPv4 TCP 入口 | `app/nodeport.py:describe_service()` 校验 Service 类型、策略、地址族与端口协议；其余一律拒绝并说明原因（`Cluster` 策略下节点可转发到别的节点，没有“本地后端”这一说） |
| 按节点列出本地后端 IP 和实际端口 | 从 Service 的 EndpointSlice 取 `addresses` 与 `ports`；端口取 EndpointSlice 的实际目标端口，而不是 NodePort 或 Service 端口 |
| 有就绪后端时只列就绪项 | `build_node_views()`：`conditions.ready=true`（且非 terminating）的端点 |
| 否则把仍可服务的终止中后端列为备用 | 节点无就绪端点时，列出 `conditions.terminating=true` 且 `serving` 非 false 的端点，并标为“备用（终止中）” |
| 两类都没有标为无可用本地后端 | 该节点显示“无可用本地后端” |
| 列取并订阅所选 Service 的 EndpointSlice | list 带 `labelSelector=kubernetes.io/service-name=<name>`；watch 从 list 返回的 `resourceVersion` 起订阅，先回放列表之后的变更，不漏事件 |
| 两个页面共用一路订阅 | `NodePortHub` 按 `(namespace, service)` 共享一个 list+watch 订阅；每个页面持有 server 端 session，仅作引用计数 |
| 关闭其中一个，另一个仍须更新 | 关闭一个 session 只减引用；仍存活的 session 使订阅持续；无 session 后才停止 watch |
| 断线时把旧结果标为待同步 | watch 结束/建立失败时保留旧快照并把状态置为 `stale`（页面显示“待同步”），随后自动重试 |
| 版本过期返回 410 后用完整新列表替换 | watcher 收到 410（HTTP 或 `ERROR` 事件）后丢弃位置标记，重新 list 并以新 `resourceVersion` 续订；断线期间删除的 Slice 因此消失 |
| 演练页可切换端点状态、断开订阅、删除 Slice、令旧版本过期 | `simulator/` 的 `/api/sim/endpoints/...`、`/api/sim/watch`、`/api/sim/slices/...`、`/api/sim/slices/expire` |
| 恢复后已删除的后端须消失 | 410 → 重新 list 覆盖整份快照，删除的端点/Slice 不会残留 |

## 启动（docker compose）

```bash
docker compose up --build
```

* 值班台：<http://localhost:8000>
* NodePort 本地后端排查：<http://localhost:8000/nodeport>
* 演练控制台：<http://localhost:8080>

值班台在 compose 中通过 `DRAIN_KUBE_HOST=http://simulator:8080` 直连演练端，
无需 kubeconfig。

## 连接真实集群

程序支持三种模式，均通过环境变量配置：

```bash
# 集群内（ServiceAccount）
DRAIN_KUBE_MODE=in-cluster

# 运维机上的 kubeconfig
DRAIN_KUBE_MODE=kubeconfig DRAIN_KUBECONFIG=~/.kube/config DRAIN_KUBE_CONTEXT=prod

# 显式 API Server + Bearer Token
DRAIN_KUBE_MODE=host DRAIN_KUBE_HOST=https://10.0.0.1:6443 \
  DRAIN_KUBE_TOKEN_FILE=/var/run/secrets/token DRAIN_KUBE_CA_CERT=/etc/ca.crt
```

所需权限（ClusterRole）：

```yaml
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "patch"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["pods/eviction"]
    verbs: ["create"]
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["get", "list"]
  - apiGroups: ["policy"]
    resources: ["poddisruptionbudgets"]
    verbs: ["get", "list"]
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["get", "list", "watch"]
```

可选：`DRAIN_STATE_FILE=/data/rounds.json` 将轮次记录（封锁决定、已发出的
驱逐、状态行）落盘，进程重启后仍能“继续排空”。集群仍是唯一事实来源：续排
时一定会重新读取节点与 Pod。

## 本地开发

```bash
python3 -m pip install -r requirements.txt
python3 -m pip install -r tests/requirements-dev.txt   # pytest + httpx
python3 -m pytest
```

不装依赖直接跑演练端与值班台：

```bash
uvicorn simulator.main:app --port 8080
DRAIN_KUBE_HOST=http://127.0.0.1:8080 DRAIN_SIM_URL=http://127.0.0.1:8080 \
  uvicorn app.main:app --port 8000
```

## 页面状态含义

| 状态 | 含义 |
| --- | --- |
| 待驱逐 | 本轮尚未对其发出 Eviction |
| 待退出 | Eviction 已被接受，Pod 对象仍在（含删除中） |
| 已离开 | 重新读取后该 UID 已不在节点上 |
| 受预算阻挡 | API Server 以 429 拒绝，通常因 PDB；同轮其他 Pod 继续 |
| 留待续排 | 同名替代者或驱逐冲突，留给下一轮处理 |

“可下线”只在轮次页面显示，并且必须同时满足：节点不可调度、节点上已无普通
Pod。DaemonSet、静态 Pod 和已结束的 Pod 不计入。

### NodePort 页面状态

| 状态 | 含义 |
| --- | --- |
| 就绪后端 | 该节点至少有一个就绪端点；只列就绪端点 |
| 备用（终止中） | 该节点没有就绪端点，但仍有终止中且仍在服务的端点 |
| 无可用本地后端 | 该节点既无就绪端点，也无仍可服务的终止中端点 |
| 已同步 | list+watch 订阅存活，展示的是当前快照 |
| 待同步 | 订阅已断开或用旧版本，保留旧结果并自动重新 list+watch |

## 目录

```
app/
  main.py      FastAPI 路由（表单 POST + redirect，刷新不会重复驱逐）
  drain.py     轮次编排、状态机、复核逻辑
  kube.py      Kubernetes 客户端封装（封锁、列表、Eviction、PDB、Service、EndpointSlice 订阅）
  nodeport.py  NodePort 本地后端：校验、按节点归类、共享 list+watch 订阅
  models.py    轮次与 Pod 状态模型
  store.py     可选的轮次落盘
  templates/   页面
simulator/
  main.py      本地 Kubernetes API 演练端 + 控制台（含 Service/EndpointSlice 与订阅演练）
tests/         针对演练端的端到端测试
```

## 设计取舍

* **预算判定以 API Server 为准。** 页面展示的 PDB 数字仅供解释；是否放行由
  429 决定。这样多节点并行、多副本进程都不会超额。
* **UID 是对象身份。** 所有驱逐都带 UID 前置条件；同名重建不会被误驱逐。
* **复核是独立读。** “可下线”不依赖轮次内缓存，每次检查都重新读取节点与 Pod。
* **中断不清状态。** 中断只切换阶段；封锁和已发出的驱逐保留，续排时重新读取。
* **NodePort 只认 Local。** `Cluster` 策略下节点可转发到别的节点，无法回答
  “这台节点的本地后端是谁”，因此直接拒绝并解释，而不是给出误导性结果。
* **先 list 后 watch，位置取自 list 的版本。** watch 从 list 的
  `resourceVersion` 起订阅，list 与 watch 之间的变更由服务端回放，不会漏。
* **410 用完整列表重建，而不是续订。** 这让断线期间删除的 Slice/端点消失；
  重建后再从新版本续订。
* **订阅共享、session 引用计数。** 同一 Service 的多页面只开一路 watch；
  关闭一个页面只减引用，最后一个页面离开（或超时未轮询）才停止。
* **断线保留旧结果。** 订阅断开时不伪造“最新”，旧快照标为“待同步”，
  页面可看出它可能是旧的。
