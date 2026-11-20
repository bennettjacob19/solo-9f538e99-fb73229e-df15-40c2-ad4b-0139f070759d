# 节点排空值班台

检修 Kubernetes 节点前的排空工具：FastAPI 网页选节点，先封锁，再按轮次驱逐
Pod，只在集群确认后才提示“可下线”。

## 规则

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
| 本地演练端 | `simulator/` 提供本地 Kubernetes API 演练端（节点/Pod/PDB/Eviction 子资源），网页可调预算、同名替换、令 Pod 退出 |
| docker compose 启动 | `docker compose up --build` |

## 启动（docker compose）

```bash
docker compose up --build
```

* 值班台：<http://localhost:8000>
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
  - apiGroups: ["policy"]
    resources: ["poddisruptionbudgets"]
    verbs: ["get", "list"]
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

## 目录

```
app/
  main.py      FastAPI 路由（表单 POST + redirect，刷新不会重复驱逐）
  drain.py     轮次编排、状态机、复核逻辑
  kube.py      Kubernetes 客户端封装（封锁、列表、Eviction、PDB）
  models.py    轮次与 Pod 状态模型
  store.py     可选的轮次落盘
  templates/   页面
simulator/
  main.py      本地 Kubernetes API 演练端 + 控制台
tests/         针对演练端的端到端测试
```

## 设计取舍

* **预算判定以 API Server 为准。** 页面展示的 PDB 数字仅供解释；是否放行由
  429 决定。这样多节点并行、多副本进程都不会超额。
* **UID 是对象身份。** 所有驱逐都带 UID 前置条件；同名重建不会被误驱逐。
* **复核是独立读。** “可下线”不依赖轮次内缓存，每次检查都重新读取节点与 Pod。
* **中断不清状态。** 中断只切换阶段；封锁和已发出的驱逐保留，续排时重新读取。
