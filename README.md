ApplicationMetrics.MetricLoggers.Kafka
---
An implementation of an [ApplicationMetrics](https://github.com/alastairwyse/ApplicationMetrics) [metric logger](https://github.com/alastairwyse/ApplicationMetrics/blob/master/ApplicationMetrics/IMetricLogger.cs) which writes metric and instrumentation events to Kafka broker, and allows consuming the events via a Kafka consumer.

#### TODO
* Option to set description blank to reduce space
* Producer should send whole set of metrics to  broker in one hit (similar to SQL Server / Postgres logger bulk insert)
* Decide what to do with producer idempotence setting (https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#enable-idempotence)
* Allow config object to be passed into metric logger (but may have to override some settings where metric implementation stipulates a certain value)
* TKey on producer/consumer should be null be default BUT should have an option to override both TKey type and implementation of have value of TKey is derived (likely by an Action&lt;MetricInstanceBase&gt;)
* Put note in doco about overriding TKey... could result in uneven partitioning
* And also put a caveat about null TKey causing metric to arrive out of order (however this happens already in MetricLoggerBase so not losing anything)
* Possibly need to expose an Action&lt;ProducerBuilder&gt; to allow client config
* Create a utility class which consumes from Kafka and writes to another IMetricLogger instance
