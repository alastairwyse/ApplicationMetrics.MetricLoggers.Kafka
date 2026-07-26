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

using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using ApplicationMetrics.MetricLoggers;
using StandardAbstraction;
using NSubstitute;
using NUnit.Framework;

namespace ApplicationMetrics.MetricLoggers.Kafka.UnitTests
{
    /// <summary>
    /// Unit tests for the ApplicationMetrics.MetricLoggers.Kafka.KafkaMetricLogger class.
    /// </summary>
    public class KafkaMetricLoggerTests
    {
        private String testCategory;
        private String testTopic;
        private String testBootstrapServers;
        private KafkaMetricLogger testKafkaMetricLogger;
        private IBufferProcessingStrategy mockBufferProcessingStrategy;
        private IProducer<Null, Models.MetricInstanceBase> mockProducer;
        private IDateTime mockDateTimeProvider;
        private IStopwatch mockStopwatch;
        private IGuidProvider mockGuidProvider;

        [SetUp]
        protected void SetUp()
        {
            testCategory = "TestCategory";
            testTopic = "TestTopic";
            testBootstrapServers = "127.0.0.1:9092";
            mockProducer = Substitute.For<IProducer<Null, Models.MetricInstanceBase>>();
            testKafkaMetricLogger = new KafkaMetricLogger
            (
                testCategory,
                testTopic, 
                new ProducerConfig(), 
                false,
                mockBufferProcessingStrategy, 
                IntervalMetricBaseTimeUnit.Nanosecond, 
                true, 
                mockProducer, 
                mockDateTimeProvider, 
                mockStopwatch, 
                mockGuidProvider
            );
        }

        [TearDown]
        protected void TearDown()
        {
            testKafkaMetricLogger.Dispose();
        }

        [Test]
        public void Constructor_CategoryParameterNull()
        {
            var e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricLogger = new KafkaMetricLogger(null, testTopic, testBootstrapServers, true, new SizeLimitedBufferProcessor(1), IntervalMetricBaseTimeUnit.Nanosecond, true);
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'category' must contain a value."));
            Assert.AreEqual("category", e.ParamName);


            e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricLogger = new KafkaMetricLogger(null, testTopic, new ProducerConfig(), true, new SizeLimitedBufferProcessor(1), IntervalMetricBaseTimeUnit.Nanosecond, true);
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'category' must contain a value."));
            Assert.AreEqual("category", e.ParamName);
        }

        [Test]
        public void Constructor_CategoryParameterWhitespace()
        {
            var e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricLogger = new KafkaMetricLogger(" ", testTopic, testBootstrapServers, true, new SizeLimitedBufferProcessor(1), IntervalMetricBaseTimeUnit.Nanosecond, true);
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'category' must contain a value."));
            Assert.AreEqual("category", e.ParamName);


            e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricLogger = new KafkaMetricLogger(" ", testTopic, new ProducerConfig(), true, new SizeLimitedBufferProcessor(1), IntervalMetricBaseTimeUnit.Nanosecond, true);
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'category' must contain a value."));
            Assert.AreEqual("category", e.ParamName);
        }

        [Test]
        public void Constructor_TopicParameterNull()
        {
            var e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricLogger = new KafkaMetricLogger(testCategory, null, testBootstrapServers, true, new SizeLimitedBufferProcessor(1), IntervalMetricBaseTimeUnit.Nanosecond, true);
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'topic' must contain a value."));
            Assert.AreEqual("topic", e.ParamName);


            e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricLogger = new KafkaMetricLogger(testCategory, null, new ProducerConfig(), true, new SizeLimitedBufferProcessor(1), IntervalMetricBaseTimeUnit.Nanosecond, true);
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'topic' must contain a value."));
            Assert.AreEqual("topic", e.ParamName);
        }

        [Test]
        public void Constructor_TopicParameterWhitespace()
        {
            var e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricLogger = new KafkaMetricLogger(testCategory, " ", testBootstrapServers, true, new SizeLimitedBufferProcessor(1), IntervalMetricBaseTimeUnit.Nanosecond, true);
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'topic' must contain a value."));
            Assert.AreEqual("topic", e.ParamName);


            e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricLogger = new KafkaMetricLogger(testCategory, " ", new ProducerConfig(), true, new SizeLimitedBufferProcessor(1), IntervalMetricBaseTimeUnit.Nanosecond, true);
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'topic' must contain a value."));
            Assert.AreEqual("topic", e.ParamName);
        }

        [Test]
        public void Constructor_BootstrapServersParameterNull()
        {
            var e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricLogger = new KafkaMetricLogger(testCategory, testTopic, (String)null, true, new SizeLimitedBufferProcessor(1), IntervalMetricBaseTimeUnit.Nanosecond, true);
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'bootstrapServers' must contain a value."));
            Assert.AreEqual("bootstrapServers", e.ParamName);
        }

        [Test]
        public void Constructor_BootstrapServersParameterWhitespace()
        {
            var e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricLogger = new KafkaMetricLogger(testCategory, testTopic, " ", true, new SizeLimitedBufferProcessor(1), IntervalMetricBaseTimeUnit.Nanosecond, true);
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'bootstrapServers' must contain a value."));
            Assert.AreEqual("bootstrapServers", e.ParamName);
        }

        [Test]
        public void ProcessAmountMetricEvents()
        {
            throw new NotImplementedException();
        }
    }
}
