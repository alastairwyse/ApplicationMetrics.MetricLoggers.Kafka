/*
 * Copyright 2026 Alastair Wyse (https://github.com/alastairwyse/ApplicationMetrics.MetricLoggers.Kafka/)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using ApplicationMetrics.MetricLoggers;
using Confluent.Kafka;
using NUnit.Framework;
using System;

namespace ApplicationMetrics.MetricLoggers.Kafka.UnitTests
{
    /// <summary>
    /// Unit tests for the ApplicationMetrics.MetricLoggers.Kafka.KafkaMetricLogger class.
    /// </summary>
    public class KafkaMetricLoggerTests
    {
        [Test]
        public void Constructor_TopicParameterNull()
        {
            var e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricLogger = new KafkaMetricLogger("", "127.0.0.1:9092", new SizeLimitedBufferProcessor(1), IntervalMetricBaseTimeUnit.Nanosecond, true);
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'topic' must contain a value."));
            Assert.AreEqual("topic", e.ParamName);


            e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricLogger = new KafkaMetricLogger("", new ProducerConfig(), new SizeLimitedBufferProcessor(1), IntervalMetricBaseTimeUnit.Nanosecond, true);
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'topic' must contain a value."));
            Assert.AreEqual("topic", e.ParamName);
        }

        [Test]
        public void Constructor_TopicParameterWhitespace()
        {
            var e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricLogger = new KafkaMetricLogger(" ", "127.0.0.1:9092", new SizeLimitedBufferProcessor(1), IntervalMetricBaseTimeUnit.Nanosecond, true);
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'topic' must contain a value."));
            Assert.AreEqual("topic", e.ParamName);


            e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricLogger = new KafkaMetricLogger(" ", new ProducerConfig(), new SizeLimitedBufferProcessor(1), IntervalMetricBaseTimeUnit.Nanosecond, true);
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'topic' must contain a value."));
            Assert.AreEqual("topic", e.ParamName);
        }

        [Test]
        public void Constructor_BootstrapServersParameterNull()
        {
            var e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricLogger = new KafkaMetricLogger("testTopic", (String)null, new SizeLimitedBufferProcessor(1), IntervalMetricBaseTimeUnit.Nanosecond, true);
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'bootstrapServers' must contain a value."));
            Assert.AreEqual("bootstrapServers", e.ParamName);
        }

        [Test]
        public void Constructor_BootstrapServersParameterWhitespace()
        {
            var e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricLogger = new KafkaMetricLogger("testTopic", " ", new SizeLimitedBufferProcessor(1), IntervalMetricBaseTimeUnit.Nanosecond, true);
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'bootstrapServers' must contain a value."));
            Assert.AreEqual("bootstrapServers", e.ParamName);
        }
    }
}
