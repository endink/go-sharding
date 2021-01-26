# Go-Sharding


## 简介
数据库分库分表中间件，尽可能兼容 [ShardingSphere](https://github.com/apache/shardingsphere) 的 golang 实现，
基于小米 [Gaea](https://github.com/XiaoMi/Gaea) 魔改，但是路由算法支持 ShardingSphere 的 inline 表达式风格，而不是 Mycat/kingshard 这类晦涩而又不灵活的配置，移除多租户功能（配置太复杂了，部署多套即可）

## 为什么造这个轮子

尝试了 ShardingSphere Proxy, 其有着糟糕的 insert 性能和 CPU 100% 问题，官方 issue 里 CPU 问题给出的回复是让升级到一个 5.0 alpha 版本试试，注意，是试试，
这样的处理实在无法让人接受，我们有大量 insert 性能操作，使用 ShardingSphere Proxy 后造成**数十倍**的性能下降，注意，是数十倍，而且 CPU 100%，通过 jprofiler 观察
其 Command 部分占用了所有的 CPU , 尤其是 SqlParser， 核心组件的严重性能问题除了官方更新，自己基本是无法解决的。


## 全面的重构

小米的代码搬运了 kingshard、Vitess、tidb 等开源项目大量代码，查询计划部分通过表数组索引保存数据到装饰器造成代码难于阅读，
Router 接口强绑定表索引分片方式，使得自己实现特殊分片逻辑成为不可能的任务，好在其解析 SQL 经过生产历练补漏了很多细节，具参考价值

## 重写进度



改建流程彻底重写，重新划分代码结构，原有代码所有逻辑都在 "plan" 中

- [x] 移除小米自己的 logger, 使用 uber zap
- [x] 支持 Mysql 8 登录认证（jdbc 测试通过）
- [x] 支持 Mysql Workbench 连接
- [x] 移除粘贴过来的的 SqlParser 代码， 使用 go module 直接引用 tidb 项目，方便升级
- [x] inline 表达式支
- [x] 打造分片值计算器，彻底爬起数据数组索引方式
- [x] 多分片列支持
- [x] 统一抽象策略接口
- [ ] 重写解释器部分 **(Doing now)**
- [ ] 逻辑表呈现（使用管理工具时合并分片表为逻辑表）
- [ ] 重构路由和查询计划，支持 ShardingSphere 配置风格
- [ ] range 路由支持 
- [ ] 分片计划查看特定 SQL 支持
- [ ] 支持分布式事务
- [ ] 其他优化


## 当前可用性

Main 分支保证可用性，已支持 Mysql 8.0.X 登录协议，dev 分支实现新特性，个人临时工作保存，可能长期无法正常编译
