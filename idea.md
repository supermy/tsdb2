**AI 编程最佳实践**

PDCA-AI项目构建过程

Idea->/plan ->TDD(测试驱动)->CI（持续构建）Github CD（持续交付）

Add idea->review /plan->TDD(测试驱动)->CI（持续构建）Github CD（持续交付）

功能描述、关键技术\:基于 RocksDB 的核心架构（LSM-Tree、Column Family、WAL、Compaction），设计一套对标 Arrow+Parquet+DataFusion 的时序存储引擎。 /plan

单元测试&集成测试&真实数据压力测试，测试股概率达标； 多平台多CPU兼容 ；命令行服务；  /plan

**编写** README.md,**编写** docs/architecture.md (**架构图**+**时序图**+**类图**),**编写** docs/user-guide.md (**使用手册**)  /plan

所有代码中文注释  日期时间等相关变量不要写死 命令行进行服务部署/客户端 /plan

review , 实施计划  TDD CI CD

Arrow Flight SQL 作为统一查询接口

**review,引入Flight、DataFusion、Arrow、Parquet优化系统架构； TDD CI CD/spec**

系统架构优化；分业务全流程测试；

FIDAP   rocksdb 模拟 IceBerg ;

FDAP

| 组件                                | 实现方式                                                  | 代码占比  |
| ----------------------------------- | --------------------------------------------------------- | --------- |
| **Apache Arrow**              | 直接依赖库（`arrow-rs` / `arrow2`），**零自研** | 0%        |
| **Apache DataFusion**         | Fork + 深度定制（时序专用优化）                           | 30% 定制  |
| **Apache Parquet**            | 直接依赖 + 自定义写入策略                                 | 10% 定制  |
| **Arrow Flight SQL**          | 基于 `arrow-flight` 实现服务端                          | 20% 定制  |
| **查询规划器**                | 自研（DataFusion 之上封装时序语义）                       | 100% 自研 |
| **存储引擎（WAL/缓冲/刷盘）** | 自研                                                      | 100% 自研 |
