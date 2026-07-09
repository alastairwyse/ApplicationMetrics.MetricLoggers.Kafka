ApplicationMetrics.MetricLoggers.Kafka
---
An implementation of an [ApplicationMetrics](https://github.com/alastairwyse/ApplicationMetrics) [metric logger](https://github.com/alastairwyse/ApplicationMetrics/blob/master/ApplicationMetrics/IMetricLogger.cs) which writes metric and instrumentation events to Kafka broker, and allows consuming the events via a Kafka consumer.

#### TODO
* Option to set description blank to reduce space