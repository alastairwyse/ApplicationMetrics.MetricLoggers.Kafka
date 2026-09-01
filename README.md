ApplicationMetrics.MetricLoggers.Kafka
---
An implementation of an [ApplicationMetrics](https://github.com/alastairwyse/ApplicationMetrics) [metric logger](https://github.com/alastairwyse/ApplicationMetrics/blob/master/ApplicationMetrics/IMetricLogger.cs) which writes metric and instrumentation events to a Kafka cluster, and allows consuming the events via a Kafka consumer.

#### Overview

The metric logging is performed by 2 components, a KafkaMetricLogger which writes metrics to a Kafka cluster, and corresponding KafkaMetricConsumer which reads/consumes metrics from the cluster.  Whilst many other implementations of ApplicationMetrics metric loggers write metrics to persistent storage for post-process analysis and reporting, the idea behind the Kafka implementation is to provide a mechanism to allow a programmatic hook/tap into the metric events, to allow realtime decision and action to be taken based on the metric events and values.  An example would be detecting when a system is under high load, and then triggering a process to scale up the system to accomodate.

#### Data Model

Whilst the [IMetricLogger](https://github.com/alastairwyse/ApplicationMetrics/blob/master/ApplicationMetrics/IMetricLogger.cs) interface separates classes representing individual metrics from the assoicated metric values, the Kafka logger combines the metrics and values into an 'instance' class meaning all properties relating to the logging of a given metric are available in a single object.  The base properties (common across all metric types) are listed below...

| Property Name | Description |
| ------------- | ----------- |
| TypeFullName | The fully qualified name of the .NET Type of the metric.  Populated using the [Type.FullName](https://learn.microsoft.com/en-us/dotnet/api/system.type.fullname?view=netstandard-2.0) property of the metric class. |
| Category | The category of the metric.  Value is same as that populated in the KafkaMetricLogger constructor parameter of the same name. |
| Name | The name of the metric. |
| Description | A description of the metric, explaining what it measures and/or represents. |
| EventTime | The timestamp when the metric occurred.  In the case of interval metrics, this is the timestamp when the Begin() method was called (i.e. when the interval started). |

AmountMetricInstance, IntervalMetricInstance, and StatusMetricInstance classes additionally define numeric properties storing their associated metric values.

#### Kafka Setup
Kafka clusters and hence producer and consumer instances are highly configurable.  The constructors for KafkaMetricLogger and KafkaMetricConsumer objects have been designed to allow these configuration parameters to be passed through to the underlying [IProducer](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.IProducer-2.html) and [IConsumer](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.IConsumer-2.html) instances.

##### ProducerConfig and ConsumerConfig

Can be set as constructor param
BootstrapServers set with broker ip/host
GroupId needs to be set on ConsumerConfig
Might need to set AllowAutoCreateTopics
AutoOffsetReset -> link to doco and explanation (https://www.confluent.io/blog/guide-to-consumer-offsets/)
Retention (see https://www.confluent.io/learn/kafka-retention/)

##### TKey Value

Set to Kafka Null.
Messages assigned to topic paritions using round robin approach
See https://www.confluent.io/learn/kafka-message-key/
Messages sent to different partitions could arrive out of order, but this doesn't deviate from existing MetricLoggerBuffer functionality.
EventTime property could be used to reorder if necessary


##### Error and Log Handler
No point throwing exceptions

#### Exception Handling
On consumer worker thread
Maybe show example... will handle error thrown after multiple retries and internal error handler calls
Take remark from consumer constructor... types of events which will cause an exception and what's the effect.

Example is below if I decide to include this in the doco...

```
Kafka Error -> Reason: 1/1 brokers are down; Code: Local_AllBrokersDown, IsFatal: False.
Kafka Error -> Reason: 1/1 brokers are down; Code: Local_AllBrokersDown, IsFatal: False.
Consume Exception -> Exception Exception occurred on message consumer worker thread at 2026-09-01 12:44:18.3421814. occurred...

Unhandled exception. System.Exception: Exception occurred on message consumer worker thread at 2026-09-01 12:44:18.3421814.
 ---> Confluent.Kafka.ConsumeException: Subscribed topic not available: TestTopic: Broker: Unknown topic or partition
   at Confluent.Kafka.Consumer`2.Consume(Int32 millisecondsTimeout)
   at ApplicationMetrics.MetricLoggers.Kafka.KafkaMetricConsumer.Consume() in C:\Development\C#\ApplicationMetrics.MetricLoggers.Kafka\ApplicationMetrics.MetricLoggers.Kafka\KafkaMetricConsumer.cs:line 185
   --- End of inner exception stack trace ---
   at ApplicationMetrics.MetricLoggers.Kafka.KafkaMetricConsumer.Stop() in C:\Development\C#\ApplicationMetrics.MetricLoggers.Kafka\ApplicationMetrics.MetricLoggers.Kafka\KafkaMetricConsumer.cs:line 170
   at KafkaHelloWorld.KafkaMetricConsumerTest.TestMaximalConsumer() in C:\Development\C#\Test Projects\KafkaHelloWorld\KafkaHelloWorld\KafkaMetricConsumerTest.cs:line 113
   at KafkaHelloWorld.Program.Main(String[] args) in C:\Development\C#\Test Projects\KafkaHelloWorld\KafkaHelloWorld\Program.cs:line 18

```

#### TODO
* Possibly document group id https://www.confluent.io/blog/configuring-apache-kafka-consumer-group-ids/ and offsets.
* Doco on exception and log Actions
* Decide what to do with producer idempotence setting (https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#enable-idempotence)
* TKey on producer/consumer should be null be default BUT should have an option to override both TKey type and implementation of have value of TKey is derived (likely by an Action&lt;MetricInstanceBase&gt;)
  * To implement TKey definition will also need client to provide a ValueSerializer
* Put note in doco about overriding TKey... could result in uneven partitioning
* And also put a caveat about null TKey causing metric to arrive out of order (however this happens already in MetricLoggerBase so not losing anything)
* Possibly need to expose an Action&lt;ProducerBuilder&gt; to allow client config
* Create a utility class which consumes from Kafka and writes to another IMetricLogger instance
* If you want to put different metric types on different topics, could use MetricFilter and router to multiple Kafka metric loggers

#### TODO Documentation
* Document need for consumer group in consumer setup stuff
* Standard blurbs that are in all metric logger implementations
* Overriding logging and exception handling AND Exception handler (non-Kafka one)
* Uses prorobuf
* TKey is null -> what are implications for shard partitions
* Uses default auto commit
* Explain how consumer runs on a thread
* Explain error handling on consumer thread
* Talk about events arriving out of order with null TKey (stuff already out of order with Bufferbase)
* Discuss idempotence (read confluent link above)

#### Producer Setup

Minimal setup

```C#
// Setup the producer configuration
ProducerConfig config = new();
config.BootstrapServers = "127.0.0.1:9092";
config.AllowAutoCreateTopics = true;
Action<Exception> bufferProcessingExceptionAction = (Exception e) => { Console.WriteLine($"Exception {e.Message} occurred whilst prcoessing buffers."); };
using (var bufferProcessingStrategy = new SizeLimitedBufferProcessor(5, bufferProcessingExceptionAction, true))
using (var metricLogger = new KafkaMetricLogger("TestCategory", "TestTopic", config, false, bufferProcessingStrategy, IntervalMetricBaseTimeUnit.Nanosecond, true))
{
    metricLogger.Start();

    Guid beginId = metricLogger.Begin(new MessageReceiveTime());
    Thread.Sleep(20);
    metricLogger.Increment(new MessageReceived());
    metricLogger.Add(new MessageSize(), 2661);
    metricLogger.End(beginId, new MessageReceiveTime());

    metricLogger.Stop();
}
```

Maximal setup

```C#
// Setup the producer configuration
ProducerConfig config = new();
config.BootstrapServers = "127.0.0.1:9092";
config.AllowAutoCreateTopics = true;
Action<Exception> bufferProcessingExceptionAction = (Exception e) => { Console.WriteLine($"Exception {e.Message} occurred whilst prcoessing buffers."); };
// Setup logging and error handling actions
Action<Error> kafkaErrorHandlingAction = (Error error) =>
{
    Console.WriteLine($"Kafka Error -> Reason: {error.Reason}; Code: {error.Code}, IsFatal: {error.IsFatal}.");
};
Action<LogMessage> logMessageAction = (LogMessage message) =>
{
    Console.WriteLine($"Kafka Log -> Message: {message.Message}; Level: {message.Level}.");
};
using (var bufferProcessingStrategy = new SizeLimitedBufferProcessor(5, bufferProcessingExceptionAction, true))
using (var metricLogger = new KafkaMetricLogger("TestCategory", "TestTopic", config, false, bufferProcessingStrategy, IntervalMetricBaseTimeUnit.Nanosecond, true, kafkaErrorHandlingAction, logMessageAction))
{
    metricLogger.Start();

    Guid beginId = metricLogger.Begin(new MessageReceiveTime());
    Thread.Sleep(20);
    metricLogger.Increment(new MessageReceived());
    metricLogger.Add(new MessageSize(), 2661);
    metricLogger.End(beginId, new MessageReceiveTime());

    metricLogger.Stop();
}
```

KafkaMetricLogger accepts the following constructor parameters...

| Parameter Name | Description |
| -------------- | ----------- |
| category | The category to log all metrics under.  The ability to specify a category allows instances of the same metrics to be logged, but also distinguished from each other... e.g. in the case of a multi-threaded application, the category could be set to reflect an individual thread. |
| topic | The kafka topic to write metrics to. |
| producerConfig | The configuration to apply to the underlying IProducer&lt;TKey, TValue&gt;. |
| logMetricDescriptionAsBlankString | Whether metric's 'description' fields should be sent as a blank strings (and thereby reducing the message sizes). |
| bufferProcessingStrategy | An object implementing IBufferProcessingStrategy which decides when the buffers holding logged metric events should be flushed (and be written to the Kafka broker). |
| intervalMetricBaseTimeUnit | The base time unit to use to log interval metrics. |
| intervalMetricChecking | Specifies whether an exception should be thrown if the correct order of interval metric logging is not followed (e.g. End() method called before Begin()).  Note that this parameter only has an effect when running in 'non-interleaved' mode. |
| kafkaErrorHandlingAction | An action to invoke if the underlying Kafka IProducer&lt;TKey, TValue&gt; raises a Kafka Error when a metric is written to the cluster.  Accepts a single parameter which is the Error. |
| logMessageAction | An action to invoke when the underlying Kafka IProducer&lt;TKey, TValue&gt; writes a log message.  Accepts a single parameter which is the LogMessage. |

#### Consumer Setup

Minimal setup

```C#
// Setup the consumer configuration
ConsumerConfig config = new();
config.BootstrapServers = "127.0.0.1:9092";
config.GroupId = "TestGroupId";
using (var consumer = new KafkaMetricConsumer("TestTopic", config, 1000))
{
    // Create event handler delegate
    EventHandler<MetricInstanceBase> metricEventReceivedAction = (Object sender, MetricInstanceBase metricInstance) =>
    {
        Console.WriteLine($"Received metric '{metricInstance.Name}', '{metricInstance.Description}', '{metricInstance.TypeFullName}");
    };
    // Subscribe to the 'MetricEventReceived' event
    consumer.MetricEventReceived += metricEventReceivedAction;
    consumer.Start();
    // (wait for application shutdown)
    consumer.Stop();
    consumer.MetricEventReceived -= metricEventReceivedAction;
}
```

After calling the consumer.Start() method, the above code will write output like below to the console.

```
Received metric 'AvailableMemory', 'The amount of free memory in the system in bytes', 'KafkaTest.AvailableMemory
Received metric 'DiskReadTime', 'The time taken to perform a read operation from disk', 'KafkaTest.DiskReadTime
Received metric 'DiskReadOperation', 'A disk read operation', 'KafkaTest.DiskReadOperation
```

KafkaMetricConsumer accepts the following constructor parameters...

Maximal setup

```C#
// Setup the consumer configuration
ConsumerConfig config = new();
config.BootstrapServers = "127.0.0.1:9092";
config.GroupId = "TestGroupId";
// Setup logging and error handling actions
Action<Exception> consumeExceptionAction = (Exception e) =>
{
    Console.WriteLine($"Consume Exception -> Exception {e.Message} occurred...");
};
Action<Error> kafkaErrorHandlingAction = (Error error) =>
{
    Console.WriteLine($"Kafka Error -> Reason: {error.Reason}; Code: {error.Code}, IsFatal: {error.IsFatal}.");
};
Action<LogMessage> logMessageAction = (LogMessage message) =>
{
    Console.WriteLine($"Kafka Log -> Message: {message.Message}; Level: {message.Level}.");
};
using (var consumer = new KafkaMetricConsumer("TestTopic", config, 1000, consumeExceptionAction, kafkaErrorHandlingAction, logMessageAction))
{
    // Create event handler delegate
    EventHandler<MetricInstanceBase> metricEventReceivedAction = (Object sender, MetricInstanceBase metricInstance) =>
    {
        Console.WriteLine($"Received metric '{metricInstance.Name}', '{metricInstance.Description}', '{metricInstance.TypeFullName}");
    };
    // Subscribe to the 'MetricEventReceived' event
    consumer.MetricEventReceived += metricEventReceivedAction;
    consumer.Start();
    // (wait for application shutdown)
    consumer.Stop();
    consumer.MetricEventReceived -= metricEventReceivedAction;
}
```

```
Kafka Log -> Message: [thrd:GroupCoordinator]: GroupCoordinator: 127.0.0.1:9092: Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2049ms in state CONNECT, 3 identical error(s) suppressed); Level: Error.
Kafka Error -> Reason: GroupCoordinator: 127.0.0.1:9092: Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2049ms in state CONNECT, 3 identical error(s) suppressed); Code: Local_Transport, IsFatal: False.
Kafka Error -> Reason: 2/2 brokers are down; Code: Local_AllBrokersDown, IsFatal: False.
Kafka Log -> Message: [thrd:main]: Offset commit (unassigned partitions) failed for 3/3 partition(s) in join-state wait-unassign-to-complete: Local: Waiting for coordinator: TestTopic((null))[0]@729(Local: Waiting for coordinator), TestTopic((null))[1]@698(Local: Waiting for coordinator), TestTopic((null))[2]@827(Local: Waiting for coordinator); Level: Warning.
```

KafkaMetricConsumer accepts the following constructor parameters...

| Parameter Name | Description |
| -------------- | ----------- |
| topic | The kafka topic to read metrics from. |
| consumerConfig | The configuration to apply to the underlying IConsumer&lt;TKey, TValue&gt;. |
| consumeLoopTimeout | The maximum time to wait for a message from the Kafka cluster before timing out and reconnecting (in milliseconds). |
| consumeExceptionAction | An action to invoke if an Exception occurs during message consumption.  Accepts a single parameter which is the Exception. |
| kafkaErrorHandlingAction | An action to invoke if the underlying Kafka IConsumer&lt;TKey, TValue&gt; raises a Kafka Error when a metric is consumed from the cluster.  Accepts a single parameter which is the Error. |
| logMessageAction | An action to invoke when the Kafka IConsumer&lt;TKey, TValue&gt; writes a log message.  Accepts a single parameter which is the LogMessage. |

