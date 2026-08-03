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
using System.Globalization;
using System.Threading;
using Confluent.Kafka;
using ApplicationMetrics.MetricLoggers.Kafka.Models;
using NSubstitute;
using NUnit.Framework;

namespace ApplicationMetrics.MetricLoggers.Kafka.UnitTests
{
    /// <summary>
    /// Unit tests for the ApplicationMetrics.MetricLoggers.Kafka.KafkaMetricConsumer class.
    /// </summary>
    public class KafkaMetricConsumerTests
    {
        private String testTopic;
        private String testBootstrapServers;
        private Int32 testConsumeLoopTimeout;
        private IConsumer<Ignore, MetricInstanceBase> mockConsumer;

        [SetUp]
        protected void SetUp()
        {
            testTopic = "TestTopic";
            testBootstrapServers = "127.0.0.1:9092";
            testConsumeLoopTimeout = 5000;
            mockConsumer = Substitute.For<IConsumer<Ignore, MetricInstanceBase>>();
        }

        [Test]
        public void Constructor_TopicParameterNull()
        {
            var e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricConsumer = new KafkaMetricConsumer(null, testBootstrapServers, testConsumeLoopTimeout, (Exception consumeException) => { });
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'topic' must contain a value."));
            Assert.AreEqual("topic", e.ParamName);


            e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricConsumer = new KafkaMetricConsumer(null, new ConsumerConfig(), testConsumeLoopTimeout, (Exception consumeException) => { });
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'topic' must contain a value."));
            Assert.AreEqual("topic", e.ParamName);
        }

        [Test]
        public void Constructor_TopicParameterWhitespace()
        {
            var e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricConsumer = new KafkaMetricConsumer(" ", testBootstrapServers, testConsumeLoopTimeout, (Exception consumeException) => { });
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'topic' must contain a value."));
            Assert.AreEqual("topic", e.ParamName);


            e = Assert.Throws<ArgumentException>(delegate
            {
                var testKafkaMetricConsumer = new KafkaMetricConsumer(" ", new ConsumerConfig(), testConsumeLoopTimeout, (Exception consumeException) => { });
            });

            Assert.That(e.Message, Does.StartWith("Parameter 'topic' must contain a value."));
            Assert.AreEqual("topic", e.ParamName);
        }

        [Test]
        public void Consume_ExceptionConsuming()
        {
            Exception consumeActionParameter = null;
            Action<Exception> testConsumeExceptionAction = (Exception consumeException) => { consumeActionParameter = consumeException; };
            var mockException = new Exception("Mock exception");
            mockConsumer.When((consumer) => consumer.Consume(testConsumeLoopTimeout)).Do((callInfo) => throw mockException);
            using (KafkaMetricConsumer testKafkaMetricConsumer = new(testTopic, new ConsumerConfig(), testConsumeLoopTimeout, testConsumeExceptionAction, mockConsumer))
            {
                testKafkaMetricConsumer.Start();
                // Wait for the consume thread to run and catch the exception
                System.Threading.Thread.Sleep(500);

                var e = Assert.Throws<Exception>(delegate
                {
                    testKafkaMetricConsumer.Stop();
                });

                Assert.That(e.Message, Does.StartWith("Exception occurred on message consumer worker thread at "));
                Assert.AreEqual(mockException, e.InnerException);
                Assert.IsNotNull(consumeActionParameter);
                Assert.AreSame(consumeActionParameter, e);
            }
        }

        [Test]
        public void Consume()
        {
            var testCountMetricInstance = new CountMetricInstance
            (
                "ApplicationMetrics.MetricLoggers.Kafka.UnitTests.DiskReadOperation",
                "DiskReadOperation",
                "A disk read operation",
                CreateDataTimeFromString("2026-08-03 22:39:59.0010000")
            );
            var countMetricConsumeResult = new ConsumeResult<Ignore, MetricInstanceBase>();
            countMetricConsumeResult.Topic = testTopic;
            countMetricConsumeResult.Message = new Message<Ignore, MetricInstanceBase>();
            countMetricConsumeResult.Message.Value = testCountMetricInstance;


            // TODO: Tidy up belwo code
            // Not sure that consumer returns null in case of timeout
            // Might be better to make the second call wait/sleep
            Boolean hasReturned = false;
            mockConsumer.Consume(testConsumeLoopTimeout).Returns
            (
                (callInfo) => 
                { 
                    if (hasReturned == false)
                    {
                        hasReturned = true;
                        return countMetricConsumeResult;
                    }
                    else
                    {
                        return null;
                    }
                }
            );
            List<MetricInstanceBase> consumedMetricInstances = new();
            using (ManualResetEvent completeSignal = new(false))
            using (KafkaMetricConsumer testKafkaMetricConsumer = new(testTopic, new ConsumerConfig(), testConsumeLoopTimeout, (Exception consumeException) => { }, mockConsumer))
            {
                EventHandler<MetricInstanceBase> metricEventReceivedAction = (Object sender, MetricInstanceBase metricInstance) => 
                { 
                    consumedMetricInstances.Add(metricInstance);
                    completeSignal.Set();
                };
                testKafkaMetricConsumer.MetricEventReceived += metricEventReceivedAction;
                
                testKafkaMetricConsumer.Start();

                completeSignal.WaitOne();
                testKafkaMetricConsumer.Stop();
                Assert.AreEqual(1, consumedMetricInstances.Count);
                Assert.AreSame(testCountMetricInstance, consumedMetricInstances[0]);
                testKafkaMetricConsumer.MetricEventReceived -= metricEventReceivedAction;
            }
        }

        #region Private/Protected Methods

        /// <summary>
        /// Creates a DateTime from the specified yyyy-MM-dd HH:mm:ss format string.
        /// </summary>
        /// <param name="stringifiedDateTime">The stringified date/time to convert.</param>
        /// <returns>A DateTime.</returns>
        protected System.DateTime CreateDataTimeFromString(String stringifiedDateTime)
        {
            System.DateTime returnDateTime = System.DateTime.ParseExact(stringifiedDateTime, "yyyy-MM-dd HH:mm:ss.fffffff", DateTimeFormatInfo.InvariantInfo);

            return System.DateTime.SpecifyKind(returnDateTime, DateTimeKind.Utc);
        }

        #endregion

    }
}
