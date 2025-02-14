1. 实时数据处理与分析
情景
游戏运营团队希望实时监控玩家在线时⻓ 付费⾦额等关键指标 以便及时调整运营策略
要求
a. 设计⼀个基于 AWS Kinesis 或 Kafka 的实时数据处理架构 说明数据流转过程
```
[游戏客户端/服务端] --(1) 日志事件--> [AWS Kinesis Data Streams]
                                      |
                                      |--(2) 实时消费--> [Flink on AWS EMR/KDA]
                                      |--(3) 持久化--> [Amazon S3] (原始数据备份)
                                      |
[Flink 处理结果] --(4) 写入--> [Amazon Redshift/TimeStream] --> (5) 可视化 [Grafana/QuickSight]



说明：
1. 数据采集：游戏客户端/服务端通过SDK发送玩家行为事件（登录、退出、付费等）到Kinesis Data Streams
2. 实时处理：Flink集群持续消费Kinesis数据流，进行窗口聚合计算
3. 数据持久化：原始数据同时备份到S3用于审计和批处理补充
4. 结果存储：处理后的指标写入时序数据库(TimeStream)和数仓(Redshift)
5. 可视化展示：BI工具连接分析数据库生成实时监控看板

b. 选择合适的实时计算框架 如 Spark Streaming Flink 编写伪代码实现关键指标计算

// 定义输入事件POJO
public class PlayerEvent {
    public String userId;
    public String eventType; // LOGIN/LOGOUT/PAYMENT
    public Long timestamp;
    public Double amount; // 仅PAYMENT事件有效
}

// 主处理逻辑
DataStream<PlayerEvent> rawStream = env
    .addSource(new FlinkKinesisConsumer<>(...));

// 在线时长计算
DataStream<Tuple2<String, Long>> onlineTime = rawStream
    .keyBy(e -> e.userId)
    .process(new OnlineTimeCalculator());

// 付费金额计算
DataStream<Tuple2<String, Double>> paymentSum = rawStream
    .filter(e -> "PAYMENT".equals(e.eventType))
    .keyBy(e -> e.userId)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .sum("amount");

// 自定义在线时长计算器
public static class OnlineTimeCalculator 
    extends KeyedProcessFunction<String, PlayerEvent, Tuple2<String, Long>> {
    
    private ValueState<Long> loginTimeState;

    public void processElement(PlayerEvent event, Context ctx, 
        Collector<Tuple2<String, Long>> out) {
        
        if ("LOGIN".equals(event.eventType)) {
            loginTimeState.update(event.timestamp);
        } else if ("LOGOUT".equals(event.eventType)) {
            Long loginTime = loginTimeState.value();
            Long duration = event.timestamp - loginTime;
            out.collect(Tuple2.of(event.userId, duration));
            loginTimeState.clear();
        }
    }
}


c. 讨论如何保证数据处理的低延迟和⾼可靠性 以及如何应对流量⾼峰
低延迟保障：
  分层并行处理：
  - Kinesis分片数 = 峰值吞吐量 / 单分片容量（1MB/s）
  - Flink算子并行度与Kinesis分片数对齐
  网络优化：
  - 使用相同Region的组件（Kinesis-Flink-Redshift在同一个AWS区域）

高可靠性保证：
端到端精确一次（Exactly-Once）：
  # Flink配置
  execution.checkpointing.interval: 60000
  execution.checkpointing.mode: EXACTLY_ONCE
  kinesis.producer.aggregationMaxCount: 1 # 禁用聚合

流量高峰应对：
1.自动扩展： Kinesis分片自动伸缩<br>2. Flink TaskManager自动扩缩容（基于CPU/吞吐量指标） 
2.降级方案： 动态调整计算窗口大小（5min→10min）<br>2. 非关键指标计算降级 



2. 数据仓库与 BI 系统设计
情景
游戏设计团队需要⼀个数据仓库和 BI 系统 以便深⼊分析玩家⾏为 优化游戏关卡设计

要求
a. 设计⼀个基于 AWS Redshift 的数据仓库 说明维度建模过程 如何⽀持玩家⾏为的多维度分析


建模玩家维度表（玩家维度，关卡维度），事实表（关卡交互事实表）

多维度分析：
时间维度下钻：从季度→月→周→日→小时分析关卡尝试时段分布
玩家属性交叉：VIP等级 vs 关卡失败原因相关性分析
设备性能影响：低端设备在复杂关卡的放弃率对比


b. 对⽐⼏种主流 BI ⼯具(如 Tableau Superset QuickSight) 推荐⼀个最适合的⽅案 并说明
理由
tableau：需要额外连接器，授权费用高，丰富的自定义图表类型
superset: 开源，社区活跃,支持二次开发，依赖第三方集成
QuickSight：无缝对接Redshift，按会话计费，快速构建实时运营看板

推荐：
Amazon QuickSight
原生集成优势：直接读取Redshift物化视图，自动同步表结构变更
游戏专项模板：内置玩家留存漏斗、关卡热度地图等预设分析模版
权限管控体系：与AWS IAM深度整合，实现字段级数据权限控制
成本效益：按活跃用户数计费，无需预置服务器资源


c. 讨论如何与游戏设计团队⾼效协作 提供数据⽀持和分析洞察
游戏设计团队->>提交分析需求模板->数据团队 开发专题模板/自定义报表


3. 数据分析与产品改进
情景
通过分析发现 某个游戏关卡的通过率明显低于其他关卡 怀疑可能存在设计缺陷

要求
a. 请给出发现该问题的思路 需要分析哪些数据 可以⽤ SQL 或 Python 代码表达核⼼思路

全局关卡通过率对比，异常关卡定位，然后维度下钻分析找到有问题的维度和关卡：

/* 关卡基础通过率分析 */
WITH level_stats AS (
  SELECT 
    level_id,
    COUNT(DISTINCT session_id) AS attempts,
    COUNT(CASE WHEN event_type = 'success' THEN 1 END) AS successes,
    COUNT(CASE WHEN event_type = 'abandon' THEN 1 END) AS abandons
  FROM game_events
  WHERE event_category = 'level_progress'
  GROUP BY 1
)
SELECT 
  level_id,
  successes*1.0 / attempts AS success_rate,
  abandons*1.0 / attempts AS abandon_rate,
  PERCENT_RANK() OVER (ORDER BY success_rate) AS percentile_rank
FROM level_stats
WHERE attempts > 100  -- 过滤低样本关卡
ORDER BY success_rate ASC
LIMIT 5;


b. 根据你⾃⼰玩游戏的经验 提出 2~3 个优化该游戏关卡的假设 并设计 A/B 实验⽅案进⾏验证

假设提出：
难度曲线假设：当前关卡难度陡增，需要调整敌人强度梯度
引导缺失假设：关键机制未明确提示，导致玩家认知障碍
挫败体验假设：死亡惩罚过重，建议增加中途存档点

c. 讨论如何捕捉和响应玩家反馈 持续优化游戏体验 提⾼⽤户留存
玩家反馈收集：
游戏内评论区、社交平台、问卷调查

玩家反馈分析：
情感分析、关键词提取、用户画像

反馈响应：
根据反馈类型调整游戏机制、设计新关卡、提供支持服务