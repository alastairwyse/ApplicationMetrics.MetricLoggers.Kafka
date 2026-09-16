ApplicationMetrics.MetricLoggers.Kafka
---
An implementation of an [ApplicationMetrics](https://github.com/alastairwyse/ApplicationMetrics) [metric logger](https://github.com/alastairwyse/ApplicationMetrics/blob/master/ApplicationMetrics/IMetricLogger.cs) which writes metric and instrumentation events to a Kafka cluster, and allows consuming the events via a Kafka consumer.

### Overview

The metric logging is performed by 2 components, a KafkaMetricLogger which writes metrics to a Kafka cluster, and corresponding KafkaMetricConsumer which reads/consumes metrics from the cluster.  Whilst many other implementations of ApplicationMetrics metric loggers write metrics to persistent storage for post-process analysis and reporting, the idea behind the Kafka implementation is to provide a mechanism to allow a programmatic hook/tap into the metric events, to allow realtime decision and action to be taken based on the metric events and values.  An example would be detecting when a system is under high load, and then triggering a process to scale up the system to accomodate.

### Data Model

Whilst the [IMetricLogger](https://github.com/alastairwyse/ApplicationMetrics/blob/master/ApplicationMetrics/IMetricLogger.cs) interface separates classes representing individual metrics from the assoicated metric values, the Kafka logger combines the metrics and values into an 'instance' class meaning all properties relating to the logging of a given metric are available in a single object.  The base properties (common across all metric types) are listed below...

| Property Name | Description |
| ------------- | ----------- |
| TypeFullName | The fully qualified name of the .NET Type of the metric.  Populated using the [Type.FullName](https://learn.microsoft.com/en-us/dotnet/api/system.type.fullname?view=netstandard-2.0) property of the metric class. |
| Category | The category of the metric.  Value is same as that populated in the KafkaMetricLogger constructor parameter of the same name. |
| Name | The name of the metric. |
| Description | A description of the metric, explaining what it measures and/or represents. |
| EventTime | The timestamp when the metric occurred.  In the case of interval metrics, this is the timestamp when the Begin() method was called (i.e. when the interval started). |

AmountMetricInstance, IntervalMetricInstance, and StatusMetricInstance classes additionally define numeric properties storing their associated metric values.

### Kafka Setup
Kafka clusters and hence producer and consumer instances are highly configurable.  The constructors for KafkaMetricLogger and KafkaMetricConsumer objects have been designed to allow these configuration parameters to be passed through to the underlying [IProducer](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.IProducer-2.html) and [IConsumer](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.IConsumer-2.html) instances.

#### ProducerConfig and ConsumerConfig
[ProducerConfig](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.ProducerConfig.html) and [ConsumerConfig](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.ConsumerConfig.html) can be set on the KafkaMetricLogger and KafkaMetricConsumer classes respectively, to control and configure the [IProducer](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.IProducer-2.html) and [IConsumer](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.IConsumer-2.html) instances which implement the interface to the Kafka broker.  The [BootstrapServers](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.ClientConfig.html#Confluent_Kafka_ClientConfig_BootstrapServers) property must be set on the configuration both cases to specify the network location of the broker.  

In the case of the consumer configuration, the [GroupId](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.ConsumerConfig.html#Confluent_Kafka_ConsumerConfig_GroupId) property must also be set (see https://www.confluent.io/blog/configuring-apache-kafka-consumer-group-ids/).

If the Kafka broker is not preconfigured with the relevant topics setup, the producer configuration [AllowAutoCreateTopics](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.ClientConfig.html#Confluent_Kafka_ClientConfig_AllowAutoCreateTopics) property should be set to true.

Both [offset](https://www.confluent.io/blog/guide-to-consumer-offsets/) and [retention](https://www.confluent.io/learn/kafka-retention/) parameters may need to be configured depending on the required behaviour.

#### TKey Value
The [IProducer](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.IProducer-2.html) and [IConsumer](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.IConsumer-2.html) objects which underlie KafkaMetricLogger and KafkaMetricConsumer are generic classes which require specifying key and value types for all messages sent to and consumed from the Kafka broker.  The value type is set to be the [MetricInstanceBase](https://github.com/alastairwyse/ApplicationMetrics.MetricLoggers.Kafka/blob/main/ApplicationMetrics.MetricLoggers.Kafka/Models/MetricInstanceBase.cs) class described [above](#data-model).  The key type is set to be [Kafka's Null](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.Null.html).  As described in the [documentation](https://www.confluent.io/learn/kafka-message-key/#when-no-key-is-provided), when the key is Null and the destination topic is split across multiple partitions, the producer will distribute messages to these partitions in a round-robin manner.  The downside of having a Null key is that the messages can be consumed in a different order to the order they were produced in.  However, the ApplicationMetrics [MetricLoggerBuffer](https://github.com/alastairwyse/ApplicationMetrics/blob/master/ApplicationMetrics.MetricLoggers/MetricLoggerBuffer.cs) class (from which KafkaMetricLogger is derived) already buffers metrics of different types (i.e. acount, amount, etc...) into batches before logging/writing, which can result in out-of-order logging.  It's expected that if metrics are required to be ordered they can by sorted by the 'EventTime' property in the receiving system/store.  Hence using a null Key and the ensuing potential out-of-order consumption, does not degrade the functionality already implicit in ApplicationMetrics.

#### Error and Log Handler
The constructors for KafkaMetricLogger and KafkaMetricConsumer objects allow setting Actions which handle when [Kafka errors](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.Error.html) are raised and [logs](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.LogMessage.html) are written by the underlying IProducer and IConsumer.  These are set via parameters 'kafkaErrorHandlingAction' and 'logMessageAction'.  As per the [Kafka](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.ProducerBuilder-2.html#Confluent_Kafka_ProducerBuilder_2_SetErrorHandler_System_Action_Confluent_Kafka_IProducer__0__1__Confluent_Kafka_Error__) [documentation](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.ProducerBuilder-2.html#Confluent_Kafka_ProducerBuilder_2_SetLogHandler_System_Action_Confluent_Kafka_IProducer__0__1__Confluent_Kafka_LogMessage__) if exceptions are thrown in these Actions, they will be ignored.

#### Exception Handling
The KafkaMetricConsumer performs consumption from the broker on a worker thread.  If a critical/fatal error occurs during consumption, it will not be rethrown on the KafkaMetricConsumer client's thread until the Stop() method is called.  However an Action can be set via constructor parameter 'consumeExceptionAction', which will be invoked as soon as any critical/fatal exception is thrown.

For example, setting the 'consumeExceptionAction' parameter to the following action...

```C#
ConsumerConfig config = new();
config.BootstrapServers = "127.0.0.1:9092";
config.GroupId = "TestGroupId";
Action<Error> kafkaErrorHandlingAction = (Error error) =>
{
    Console.WriteLine($"Kafka Error -> Reason: {error.Reason}; Code: {error.Code}, IsFatal: {error.IsFatal}.");
};
Action<Exception> consumeExceptionAction = (Exception e) =>
{
    Console.WriteLine($"Consume Exception -> {e.Message}");
}; 
using (var consumer = new KafkaMetricConsumer("TestTopic", config, 1000, consumeExceptionAction, kafkaErrorHandlingAction))
{
    // etc...
}
```

...would result in the following information written to the console when the IConsumer encountered a fatal error, and threw and exception (in this case, the configured topic not existing in the broker)...

```
Kafka Error -> Reason: 1/1 brokers are down; Code: Local_AllBrokersDown, IsFatal: False.
Kafka Error -> Reason: 1/1 brokers are down; Code: Local_AllBrokersDown, IsFatal: False.
Consume Exception -> Exception occurred on message consumer worker thread at 2026-09-01 12:44:18.3421814. 
```

On calling the KafkaMetricConsumer Stop() method, the exception would be rethrown...

```
Unhandled exception. System.Exception: Exception occurred on message consumer worker thread at 2026-09-01 12:44:18.3421814.
 ---> Confluent.Kafka.ConsumeException: Subscribed topic not available: TestTopic: Broker: Unknown topic or partition
   at Confluent.Kafka.Consumer`2.Consume(Int32 millisecondsTimeout)
   at ApplicationMetrics.MetricLoggers.Kafka.KafkaMetricConsumer.Consume() in C:\Development\C#\ApplicationMetrics.MetricLoggers.Kafka\ApplicationMetrics.MetricLoggers.Kafka\KafkaMetricConsumer.cs:line 185
   --- End of inner exception stack trace ---
   at ApplicationMetrics.MetricLoggers.Kafka.KafkaMetricConsumer.Stop() in C:\Development\C#\ApplicationMetrics.MetricLoggers.Kafka\ApplicationMetrics.MetricLoggers.Kafka\KafkaMetricConsumer.cs:line 170
(etc...)   
```

### Setup

#### KafkaMetricLogger

The KafkaMetricLogger accepts the following constructor parameters...

| Parameter Name | Description | Required |
| -------------- | ----------- | -------- |
| category | The category to log all metrics under.  The ability to specify a category allows instances of the same metrics to be logged, but also distinguished from each other... e.g. in the case of a multi-threaded application, the category could be set to reflect an individual thread. | Yes |
| topic | The kafka topic to write metrics to. | Yes |
| producerConfig | The configuration to apply to the underlying IProducer&lt;TKey, TValue&gt;. | Yes |
| logMetricDescriptionAsBlankString | Whether metric's 'description' fields should be sent as a blank strings (and thereby reducing the message sizes). | Yes |
| bufferProcessingStrategy | An object implementing IBufferProcessingStrategy which decides when the buffers holding logged metric events should be flushed (and be written to the Kafka broker). | Yes |
| intervalMetricBaseTimeUnit | The base time unit to use to log interval metrics. | Yes |
| intervalMetricChecking | Specifies whether an exception should be thrown if the correct order of interval metric logging is not followed (e.g. End() method called before Begin()).  Note that this parameter only has an effect when running in 'non-interleaved' mode. | Yes |
| kafkaErrorHandlingAction | An action to invoke if the underlying Kafka IProducer&lt;TKey, TValue&gt; raises a Kafka Error when a metric is written to the cluster.  Accepts a single parameter which is the Error. | No |
| logMessageAction | An action to invoke when the underlying Kafka IProducer&lt;TKey, TValue&gt; writes a log message.  Accepts a single parameter which is the LogMessage. | No |

The code below demonstrates the setup and use case including all constructor parameters...

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

#### KafkaMetricConsumer Setup

The KafkaMetricConsumer accepts the following constructor parameters...

| Parameter Name | Description |
| -------------- | ----------- |
| topic | The kafka topic to read metrics from. | Yes |
| consumerConfig | The configuration to apply to the underlying IConsumer&lt;TKey, TValue&gt;. | Yes |
| consumeLoopTimeout | The maximum time to wait for a message from the Kafka cluster before timing out and reconnecting (in milliseconds). | Yes |
| consumeExceptionAction | An action to invoke if an Exception occurs during message consumption.  Accepts a single parameter which is the Exception. | No |
| kafkaErrorHandlingAction | An action to invoke if the underlying Kafka IConsumer&lt;TKey, TValue&gt; raises a Kafka Error when a metric is consumed from the cluster.  Accepts a single parameter which is the Error. | No |
| logMessageAction | An action to invoke when the Kafka IConsumer&lt;TKey, TValue&gt; writes a log message.  Accepts a single parameter which is the LogMessage. | No |

The code below demonstrates the setup and use case including all constructor parameters...

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

After calling the consumer.Start() method, the above code will write output like below to the console.

```
Received metric 'MessageReceived', 'Represents receiving a message from an external source.', 'KafkaTest.MessageReceived
Received metric 'MessageSize', 'The size of a message received.', 'KafkaTest.MessageSize
Received metric 'MessageReceiveTime', 'The time taken to retrieve a message.', 'KafkaTest.MessageReceiveTime
```

### Non-interleaved Method Overloads
Methods which support ['non-interleaved' interval metric logging](https://github.com/alastairwyse/ApplicationMetrics#interleaved-interval-metrics) (i.e. overloads of End() and CancelBegin() methods which _don't_ accept a Guid) will be deprecated in a future version of ApplicationMetrics.  Hence it's recommended to only use the End() and CancelBegin() method overloads which accept a 'beginId' Guid parameter.

### Links
The documentation below was written for version 1.* of ApplicationMetrics.  Minor implementation details may have changed in versions 2.0.0 and above, however the basic principles and use cases documented are still valid.  Note also that this documentation demonstrates the older ['non-interleaved'](https://github.com/alastairwyse/ApplicationMetrics#interleaved-interval-metrics) method of logging interval metrics.

Full documentation for the project...<br />
[http://www.alastairwyse.net/methodinvocationremoting/application-metrics.html](http://www.alastairwyse.net/methodinvocationremoting/application-metrics.html)

A detailed sample implementation...<br />
[http://www.alastairwyse.net/methodinvocationremoting/sample-application-5.html](http://www.alastairwyse.net/methodinvocationremoting/sample-application-5.html)

#### Release History

| Version | Changes |
| ------- | ------- |
| 1.0.0 | Initial release. | 


### TODO
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

### TODO Documentation
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

### Producer Setup


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

### Consumer Setup

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

```
Kafka Log -> Message: [thrd:GroupCoordinator]: GroupCoordinator: 127.0.0.1:9092: Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2049ms in state CONNECT, 3 identical error(s) suppressed); Level: Error.
Kafka Error -> Reason: GroupCoordinator: 127.0.0.1:9092: Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2049ms in state CONNECT, 3 identical error(s) suppressed); Code: Local_Transport, IsFatal: False.
Kafka Error -> Reason: 2/2 brokers are down; Code: Local_AllBrokersDown, IsFatal: False.
Kafka Log -> Message: [thrd:main]: Offset commit (unassigned partitions) failed for 3/3 partition(s) in join-state wait-unassign-to-complete: Local: Waiting for coordinator: TestTopic((null))[0]@729(Local: Waiting for coordinator), TestTopic((null))[1]@698(Local: Waiting for coordinator), TestTopic((null))[2]@827(Local: Waiting for coordinator); Level: Warning.
```
