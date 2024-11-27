# kafka with spark 3.2.1

本篇文章主要介绍如何使用kafka作为外部数据源，使用spark处理流式数据。

## Kafka

Kafka是一个分布式的消息队列，主要应用于大数据实时处理当中。

kafka的设计优点如下：

* 批量处理思想
* 磁盘顺序读写
* 零拷贝技术
* 页缓存加速消息读写
* 分区与并行处理
* 数据压缩

## Spark Structed Streaming

**使用前提： 此处介绍的是使用kafka作为外部数据的输入源**

Spark处理流式数据时，推进使用Spark Structured Streaming， 而不是简单的Spark Streaming处理RDD的方式。

在将数据写入外部存储时，如果外部存储支持事务，则可以通过事务来保证数据操作的原子性和一致性。

下面是不同场景下根据外部存储的情况来处理服务的故障容错性。

1. checkpoint机制：checkpoint机制是Spark Structured Streaming提供的一种容错机制，

它可以将流处理过程中产生的中间结果数据保存到外部存储系统（如HDFS）中，以便在发生故障时恢复流处理过程。

比如，在不手动提交kafka的消费偏移时，如果故障发生，Spark Structured Streaming可以从检查点中获取上次成功处理的数据偏移量，并重新启动流处理过程，从该偏移量开始处理数据。

该机制在大多数情况下都会使用。

2. 使用外部存储的事务机制：Spark Structed Streaming也可以使用外部存储的事务机制来保证数据操作的原子性和一致性。数据处理和offset的更新可以作为一个事务来执行，这样就可以保证数据操作的原子性和一致性。

3. 数据处理和offset的提交，分两部步进行：在不支持事务的外部存储上，需要先处理数据，再更新offset。 这样，如果数据处理失败，则不会更新offset，从而避免了数据的重复处理。但无法保证数据操作的原子性和一致性。

Spark相关参数配置：[参考官方文档](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)

* kafka.bootstrap.servers： kafka broker list, 多个broker用逗号分隔
* includeHeaders: bool类型，是否包含kafka消息头信息. kafka版本要求（0.11.0.0 or up）
* subscribe: 订阅Kafka主题, 多个topic用逗号分隔
* subscribePattern: 订阅Kafka主题的正则表达式
* assign: 指定消费的topic和partition，格式为：{"topicA":[0,1],"topicB":[2,4]}
* kafka.group.id： string类型， 消费者组id，使用这个参数保时要比较注意，因为其容易影响其他同组的消费者正常消费数据，容易出现一些不可预期的行为，尤其是在一些query开始或重启的时候。一般不需要指定，让 Spark 自行在每个query中生成唯一的 group.id，并依赖 Checkpoint 进行状态恢复和偏移量管理。
* groupIdPrefix: string类型， 消费者组id的前缀，用于生成唯一的group.id的前缀
* checkpointLocation： string类型，checkpoint机制使用的可靠存储路径， 在writeStream中使用
* failOnDataLoss: bool类型，默认为true， 失败时是否停止流处理过程
* startingOffsets:  字符串类型，默认：latest（-1），可选： earliest（-2）， 或自定义：{"topic": {"0": 5, "1": 10}}
* maxOffsetsPerTrigger: int类型， 每个触发间隔最多处理多少条数据
* minOffsetsPerTrigger: int类型， 每个触发间隔最少处理多少条数据
* maxTriggerDelay: time with units类型，每个触发间隔最大延迟时间，默认值：15m，一般与minOffsetsPerTrigger配合使用，即使没有达到minOffsetsPerTrigger，也会在指定延迟时间后触发一次
* startingTimestamp: 消费开始的时间戳
* spark.streaming.backpressure.enabled： bool类型， 自动调整消费速率
* spark.streaming.kafka.maxRatePerPartition:  整型，每个分区每秒最多处理消息数量
* kafka.sink.enableTransaction:  bool类型，启用事务机制， 在writeStream中使用
* kafka.isolation.level： string类型，配置： read_committed， 只读取事务已提交的数据
* enable.auto.commit： bool，默认为false， 必须禁用为false， 确保手动管理偏移量
* spark.streaming.receiver.writeAheadLog.enable:  持久化接收到的原始数据，已处理的批次对应的WAL文件会被清理
* spark.sql.streaming.kafka.useDeprecatedOffsetFetching:  bool类型，默认为false，spark拉取kafka的offset的方式。spark3.0和之前版本，是使用KafkaCousumer拉取offset，spark3.1及之后使用AdminClien方式拉取offset。KafkaCousumer方式可能会导致driver无休止的等待数据，而AdminClient方式不会。
* kafka.partitioner.class: 指定kafka消息分区策略，不指定则使用kafka默认的分区策略，在writeStream中使用

从kafka中读取到的数据格式如下：

| 列名              | 数据类型                            | 备注                                                        |
| ----------------- | ----------------------------------- | ----------------------------------------------------------- |
| key               | binary                              | 使用CAST(value AS STRING) AS value 可将binary转为string类型 |
| value             | binary                              | 同key的处理                                                 |
| topic             | string                              |
| partition         | int                                 |
| offset            | long                                |
| timestamp         | long                                |
| timestampType     | int                                 |
| headers(optional) | array, Array[(String, Array[Byte])] |
 

**消费者缓存**

Spark可以将Kafka消费者在executor中缓存，这在某些情况下可以显著提高性能，比如流式处理中处理时间是关键因素的场景下。

缓存的key的组成： topic、partition、 grouoid 。

相关的配置参数如下：

* spark.kafka.consumer.cache.capacity： int类型， 默认值：64， 缓存的容量
* spark.kafka.consumer.cache.timeout: time with units类型， 默认值：5m， 缓存的超时时间
* spark.kafka.consumer.cache.evictorThreadRunInterval	: time with units类型， 默认值：1m， 清理缓存的间隔时间
* spark.kafka.consumer.cache.jmx.enable: bool类型， 默认值：false， 是否开启JMX监控


**生产者缓存**


**[安全性](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#security)**

在连接Kafka时，进行身份验证相关内容


### Kafka为外部输出（事务支持）

使用Kafka的事务机制（Kafka0.11+）、以及spark的checkpoint机制，保证数据处理的原子性和一致性

此过程是将读取的数据，进行处理后，写入Kafka中的另一个topic中

写入kafka的数据格式如下：

| 列名                | 数据类型                | 备注 |
| ------------------- | ----------------------- | ---- |
| key（optional）     | string or binary        |      |
| value               | string or binary        |      |
| topic               | string                  |      |
| partition(optional) | int                     |      |
| headers(optional)   | array[(string, byte[])] |      |

示例代码：
```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("KafkaTransactionExample")
  .getOrCreate()

// 读取 Kafka 数据
val kafkaStream = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "input-topic")
  .option("startingOffsets", "earliest")
  .option("kafka.isolation.level", "read_committed") // 确保只读取事务提交的数据
  .load()

// 数据处理逻辑
import spark.implicits._
val processedStream = kafkaStream
  .selectExpr("CAST(value AS STRING) AS value")
  .as[String]
  .map(value => s"Processed: $value")

// 写入 Kafka，指定 format 为 kafka
val query = processedStream.writeStream
  .format("kafka") // 指定 Kafka 作为 Sink
  .option("kafka.bootstrap.servers", "localhost:9092") // Kafka 地址
  .option("topic", "output-topic") // 输出目标 Topic
  .option("kafka.sink.enableTransaction", "true") // 开启事务机制
  .option("checkpointLocation", "/mnt/checkpoint") // 检查点路径
  .start()

query.awaitTermination()
```

### 其他外部存储（事务不支持）

使用spark自带的保存数据到外部存储的API

示例代码：
```scala

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("KafkaExample")
  .getOrCreate()

// 恢复消费的偏移量
val offsets = spark.read
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/kafka_offsets")
  .option("dbtable", "offsets")
  .option("user", "root")
  .option("password", "password")
  .load()
  .as[(Int, Long)] // 分区号和偏移量

val customOffsets = offsets.collect().map {
  case (partition, offset) => new TopicPartition("input-topic", partition) -> offset
}.toMap
// 读取 Kafka 数据
val kafkaStream = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "input-topic")
  .option("startingOffsets", customOffsets) // earliest 或 latest 或 自定义 {"topic": {"0": 5, "1": 10}} 视场景而定
  .option("kafka.isolation.level", "read_committed")
  .option("enable.auto.commit", "false") // 禁用自动提交
  .load()

// 数据处理
import spark.implicits._
val processedStream = kafkaStream
  // 注意： 从kafka读取的数据，是二进制格式的，需要转换为字符串，否则无法直接处理
  .selectExpr("CAST(value AS STRING) AS value", "CAST(offset AS LONG) AS offset", "partition")
  .as[(String, Long, Int)]  // 转换为RDD类型来处理了
  .map { case (value, offset, partition) =>
    (value, offset, partition, System.currentTimeMillis())
  } // 添加额外字段（如时间戳）进行处理

// 写入外部存储, 每个Batch处理完成后，写入外部存储，产生多个小文件（每个分区一个）
processedStream.writeStream.foreachBatch { (batchDF, batchId) =>
  // 写入外部存储（如 HDFS 或数据库）
  batchDF.write
    .format("parquet / text / json / csv / orc")
    .mode("append")
    .save("/mnt/hdfs/processed-data")

  // 手动提交 Kafka 偏移量（示例：保存到外部存储）
  batchDF.select("offset", "partition")
    .write
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/kafka_offsets")
    .option("dbtable", "offsets")
    .option("user", "root")
    .option("password", "password")
    .mode("append")
    .save()

  // TODO: 可考虑手动合并每Batch产出的小文件
  // foreachBatch 的外层逻辑运行在 Driver 上
  // 也就是说对batchDF对象的操作是在 Executor 上执行的，其他操作也是在 Driver 上执行的
  // TODO：哪些代码是在 Driver 上执行，哪些是在 Executor 上执行？ 得搞清楚, 有概念

}.start()

```

也可以，使用自定义Writer的方式，该方式比较灵活，可以自定义数据写入到外部存储。
[参考写数据到本地的实现](MyLocalWriter.scala)

使用自定义Writer的方式

示例代码：
```scala
processedStream.writeStream.foreach(new ForeachWriter[Row] {
  override def open(partitionId: Long, version: Long): Boolean = {
    // 初始化连接等操作
    true
  }
  override def process(record: Row): Unit = {
    // 写入操作
  }
  override def close(errorOrNull: Throwable): Unit = {
    // 关闭连接等操作
  }
})
```

## 部署程序

正常的spark程序，使用命令：spark-submit 即可运行。使用 --packages 参数，可将一些依赖的jar包添加进去（自动从仓库中下载， 需要能访问maven仓库， jar的具体添加路径可以查看日志中的输出）

```shell
/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]
```

更多使用方式，[参考文档: 程序提交指南](https://spark.apache.org/docs/latest/submitting-applications.html)



