# PBFT 共识协议 Java 实现

**Practical Byzantine Fault Tolerance — Java Demo**

------

## 中文

### 项目简介

本项目用 Java 实现了 **PBFT（实用拜占庭容错）** 共识协议的核心三阶段流程，模拟了区块链网络中多个节点之间如何在存在恶意节点的情况下达成一致。

PBFT 是联盟链（如 FISCO BCOS、Hyperledger Fabric）中广泛使用的共识机制，能够在最多 f 个拜占庭节点存在的情况下保证系统的安全性与活性，节点总数需满足 **3f+1**。

------

### 协议原理

#### 节点模型

```
总节点数 N = 3f + 1
最大容错数 f = 1（本项目）
共识阈值  Q = 2f + 1 = 3
```

#### 三阶段流程

```
Client
  │
  ▼
Primary（主节点）
  │  PRE-PREPARE（携带 digest、viewNumber、seqNumber）
  ├──────────────────────────────────► Replica A
  ├──────────────────────────────────► Replica B
  └──────────────────────────────────► Replica C
                                            │
              ◄── PREPARE（各副本广播）──────┘
              当节点收到 2f+1 条一致 PREPARE 后
                            │
              ◄── COMMIT（各节点广播）───────┘
              当节点收到 2f+1 条一致 COMMIT 后
                            │
                      本地提交执行
```

#### 安全性核心推导

两个不同决议各自至少需要 2f+1 票，两个集合之和超过 3f+1（总节点数），必然存在交集。交集中至少有 1 个诚实节点，诚实节点不会对两个不同决议都投票，因此两个不同决议不可能同时达成共识。

------

### 模块结构

```
src/main/java/com/wangan/
├── Constant.java        # 全局常量：f、总节点数、共识阈值 QUORUM
├── Message.java         # 消息模型：类型、viewNumber、seqNumber、digest、senderId
├── NetwokContext.java   # 网络上下文：节点列表、广播方法
├── Node.java            # 节点核心：消息队列、三阶段处理逻辑、提交状态
└── PBFTdemo.java        # 入口：case1 正常共识、case2 主节点作恶
```

#### 关键设计

**消息去重**：`msgCountMap` 使用 `ConcurrentHashMap<String, Set<Integer>>`，以 senderId 为 key 去重，防止同一节点重复计票。

**自票处理**：节点广播 PREPARE/COMMIT 后，本地直接将自己的 id 加入计数集合，广播不回传给自己，符合 PBFT 论文标准定义。

**并发安全**：消息队列使用 `LinkedBlockingQueue`，计数和广播状态使用 `ConcurrentHashMap`，节点线程与接收线程安全隔离。

**幂等保护**：`msgBroadMap` 使用 `putIfAbsent` 初始化，防止晚到消息覆盖已有广播状态，避免重复广播。

------

### 测试场景

#### case1：正常共识

主节点（Node0）向所有副本广播相同 digest 的 PRE-PREPARE，三阶段顺利推进，全部 4 个节点完成本地提交。

```
预期输出：[case1] 共识达成，4 个节点完成本地提交
```

#### case2：主节点作恶

主节点向不同副本发送不同 digest 的 PRE-PREPARE（"提刀上洛" vs "跑路"），两个阵营各自票数不足 QUORUM=3，共识失败。

```
预期输出：
[case2-提刀上洛] 未能达成共识，仅 0 个节点提交，未达到阈值 3
[case2-跑路]    未能达成共识，仅 0 个节点提交，未达到阈值 3
```

------

### 快速开始

**环境要求**：JDK 8+，Maven 3.x

```bash
git clone https://github.com/foguangsword/pbft.git
cd pbft
mvn clean package
java -jar target/pbft-1.0-SNAPSHOT.jar
```

**依赖**：

| 依赖           | 版本    | 用途               |
| -------------- | ------- | ------------------ |
| `fastjson2`    | 2.0.57  | 消息序列化日志输出 |
| `slf4j-simple` | 1.7.28  | 日志               |
| `lombok`       | 1.18.38 | 样板代码简化       |

------

### 已知局限性

| 说明                                                         |
| ------------------------------------------------------------ |
| 无 View Change：主节点宕机或超时后无法切换新主节点，待后续实现 |
| 无消息签名：senderId 为节点自报，未做密码学验证，真实场景需结合数字签名 |
| 单进程模拟：节点为同进程的不同线程，不涉及真实网络通信       |

------

### 参考资料

- Castro & Liskov，*Practical Byzantine Fault Tolerance*，OSDI 1999
- [FISCO BCOS 技术文档 — 共识机制](https://fisco-bcos-documentation.readthedocs.io/)

------

## English

### Overview

A Java implementation of the **PBFT (Practical Byzantine Fault Tolerance)** consensus protocol, simulating the core three-phase flow across multiple nodes in the presence of Byzantine (malicious) nodes.

PBFT is widely used in permissioned blockchains (e.g. FISCO BCOS, Hyperledger Fabric). It guarantees safety and liveness with up to f faulty nodes, requiring **N ≥ 3f+1** total nodes.

------

### Protocol Design

#### Node Model

```
Total nodes  N = 3f + 1  (4 in this project)
Fault tolerance f = 1
Quorum threshold Q = 2f + 1 = 3
```

#### Three-Phase Flow

```
Primary
  │  PRE-PREPARE (digest, viewNumber, seqNumber)
  ├──► Replica A
  ├──► Replica B
  └──► Replica C
            │
     ◄── PREPARE (each replica broadcasts) ──┘
     Once 2f+1 matching PREPAREs received
            │
     ◄── COMMIT (each node broadcasts) ───────┘
     Once 2f+1 matching COMMITs received
            │
       Local commit
```

#### Safety Argument

Any two conflicting decisions each require 2f+1 votes. Their union exceeds 3f+1 (total nodes), so they must intersect. The intersection contains at least one honest node, which cannot vote for two conflicting decisions — so two conflicting decisions cannot both reach quorum.

------

### Test Cases

**case1 — Normal consensus**: Primary broadcasts the same digest to all replicas. All 4 nodes commit.

```
[case1] 共识达成，4 个节点完成本地提交
```

**case2 — Byzantine primary**: Primary sends different digests to different replicas. Neither faction reaches quorum=3. No node commits.

```
[case2-提刀上洛] 未能达成共识，仅 0 个节点提交，未达到阈值 3
[case2-跑路]    未能达成共识，仅 0 个节点提交，未达到阈值 3
```

------

### Getting Started

**Requirements**: JDK 8+, Maven 3.x

```bash
git clone https://github.com/foguangsword/pbft.git
cd pbft
mvn clean package
java -jar target/pbft-1.0-SNAPSHOT.jar
```

------

### Known Limitations

| Issue                     | Note                                                         |
| ------------------------- | ------------------------------------------------------------ |
| No View Change            | Primary failure handling not yet implemented                 |
| No message signatures     | senderId is self-reported; real deployment needs digital signatures |
| Single-process simulation | Nodes are threads, not real network peers                    |

------

### Reference

- Castro & Liskov, *Practical Byzantine Fault Tolerance*, OSDI 1999
- [FISCO BCOS Documentation](https://fisco-bcos-documentation.readthedocs.io/)

------

### License

MIT