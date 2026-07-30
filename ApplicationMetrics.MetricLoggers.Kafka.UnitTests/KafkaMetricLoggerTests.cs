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
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using StandardAbstraction;
using ApplicationMetrics.MetricLoggers;
using NUnit.Framework;
using NSubstitute;

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
        private KafkaMetricLoggerWithProtectedMembers testKafkaMetricLogger;
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
            mockBufferProcessingStrategy = Substitute.For<IBufferProcessingStrategy>();
            mockProducer = Substitute.For<IProducer<Null, Models.MetricInstanceBase>>();
            mockDateTimeProvider = Substitute.For<IDateTime>();
            mockStopwatch = Substitute.For<IStopwatch>();
            mockGuidProvider = Substitute.For<IGuidProvider>();
            testKafkaMetricLogger = new KafkaMetricLoggerWithProtectedMembers
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
        public void ProcessAmountMetricEvents_ExceptionSendingViaProducer()
        {
            DiskBytesRead testAmountMetric = new();
            Int64 testAmount = 1234;
            System.DateTime testEventTime = CreateDataTimeFromString("2026-06-28 22:32:01.0020000");
            List<Tuple<AmountMetric, Int64, System.DateTime>> testAmountMetricEvents = new()
            {
                new Tuple<AmountMetric, Int64, System.DateTime>(testAmountMetric, testAmount, testEventTime)
            };
            var mockException = new Exception("Mock exception");
            mockProducer.ProduceAsync(Arg.Any<String>(), Arg.Any<Message<Null, Models.MetricInstanceBase>>()).Returns(Task.FromException<DeliveryResult<Null, Models.MetricInstanceBase>>(mockException));

            var e = Assert.Throws<Exception>(delegate
            {
                testKafkaMetricLogger.ProcessAmountMetricEvents(testAmountMetricEvents);
            });

            Assert.That(e.Message, Does.StartWith($"Failed to send amount metrics to kafka cluster via producer."));
            Assert.That(e.InnerException.InnerException == mockException);
        }

        [Test]
        public void ProcessAmountMetricEvents()
        {
            DiskBytesRead testAmountMetric = new();
            Int64 testAmount = 1234;
            System.DateTime testEventTime = CreateDataTimeFromString("2026-06-28 22:32:01.0020000");
            List<String> capturedTopics = new();
            List<Tuple<AmountMetric, Int64, System.DateTime>> testAmountMetricEvents = new()
            {
                new Tuple<AmountMetric, Int64, System.DateTime>(testAmountMetric, testAmount, testEventTime)
            };
            List<Models.MetricInstanceBase> capturedMessages = new();
            Action<String> topicArgumentAction = (String topic) => { capturedTopics.Add(topic); };
            Action<Message<Null, Models.MetricInstanceBase>> messageArgumentAction = (Message<Null, Models.MetricInstanceBase> message) => { capturedMessages.Add(message.Value); };
            mockProducer.ProduceAsync(Arg.Do<String>(topicArgumentAction), Arg.Do<Message<Null, Models.MetricInstanceBase>>(messageArgumentAction));

            testKafkaMetricLogger.ProcessAmountMetricEvents(testAmountMetricEvents);

            Assert.AreEqual(1, capturedTopics.Count);
            Assert.AreEqual(testTopic, capturedTopics[0]);
            Assert.AreEqual(1, capturedMessages.Count);
            Assert.IsAssignableFrom<Models.AmountMetricInstance>(capturedMessages[0]);
            Assert.AreEqual(typeof(DiskBytesRead).FullName, capturedMessages[0].TypeFullName);
            Assert.AreEqual(testCategory, capturedMessages[0].Category);
            Assert.AreEqual(testAmountMetric.Name, capturedMessages[0].Name);
            Assert.AreEqual(testAmountMetric.Description, capturedMessages[0].Description);
            Assert.AreEqual(testEventTime, capturedMessages[0].EventTime);
            Assert.AreEqual(testAmount, ((Models.AmountMetricInstance)capturedMessages[0]).Amount);


            testKafkaMetricLogger.Dispose();
            testKafkaMetricLogger = new KafkaMetricLoggerWithProtectedMembers
            (
                testCategory,
                testTopic,
                new ProducerConfig(),
                true, // logMetricDescriptionAsBlankString
                mockBufferProcessingStrategy,
                IntervalMetricBaseTimeUnit.Nanosecond,
                true,
                mockProducer,
                mockDateTimeProvider,
                mockStopwatch,
                mockGuidProvider
            );
            capturedTopics.Clear();
            capturedMessages.Clear();

            testKafkaMetricLogger.ProcessAmountMetricEvents(testAmountMetricEvents);

            Assert.AreEqual(1, capturedTopics.Count);
            Assert.AreEqual(testTopic, capturedTopics[0]);
            Assert.AreEqual(1, capturedMessages.Count);
            Assert.IsAssignableFrom<Models.AmountMetricInstance>(capturedMessages[0]);
            Assert.AreEqual(typeof(DiskBytesRead).FullName, capturedMessages[0].TypeFullName);
            Assert.AreEqual(testCategory, capturedMessages[0].Category);
            Assert.AreEqual(testAmountMetric.Name, capturedMessages[0].Name);
            Assert.AreEqual("", capturedMessages[0].Description);
            Assert.AreEqual(testEventTime, capturedMessages[0].EventTime);
            Assert.AreEqual(testAmount, ((Models.AmountMetricInstance)capturedMessages[0]).Amount);
        }

        [Test]
        public void ProcessCountMetricEvents_ExceptionSendingViaProducer()
        {
            DiskReadOperation testCountMetric = new();
            System.DateTime testEventTime = CreateDataTimeFromString("2026-06-29 21:22:03.0040000");
            List<Tuple<CountMetric, System.DateTime>> testCountMetricEvents = new()
            {
                new Tuple<CountMetric, System.DateTime>(testCountMetric, testEventTime)
            };
            var mockException = new Exception("Mock exception");
            mockProducer.ProduceAsync(Arg.Any<String>(), Arg.Any<Message<Null, Models.MetricInstanceBase>>()).Returns(Task.FromException<DeliveryResult<Null, Models.MetricInstanceBase>>(mockException));

            var e = Assert.Throws<Exception>(delegate
            {
                testKafkaMetricLogger.ProcessCountMetricEvents(testCountMetricEvents);
            });

            Assert.That(e.Message, Does.StartWith($"Failed to send count metrics to kafka cluster via producer."));
            Assert.That(e.InnerException.InnerException == mockException);
        }

        [Test]
        public void ProcessCountMetricEvents()
        {
            DiskReadOperation testCountMetric = new();
            System.DateTime testEventTime = CreateDataTimeFromString("2026-06-29 21:22:03.0040000");
            List<String> capturedTopics = new();
            List<Tuple<CountMetric, System.DateTime>> testCountMetricEvents = new()
            {
                new Tuple<CountMetric, System.DateTime>(testCountMetric, testEventTime)
            };
            List<Models.MetricInstanceBase> capturedMessages = new();
            Action<String> topicArgumentAction = (String topic) => { capturedTopics.Add(topic); };
            Action<Message<Null, Models.MetricInstanceBase>> messageArgumentAction = (Message<Null, Models.MetricInstanceBase> message) => { capturedMessages.Add(message.Value); };
            mockProducer.ProduceAsync(Arg.Do<String>(topicArgumentAction), Arg.Do<Message<Null, Models.MetricInstanceBase>>(messageArgumentAction));

            testKafkaMetricLogger.ProcessCountMetricEvents(testCountMetricEvents);

            Assert.AreEqual(1, capturedTopics.Count);
            Assert.AreEqual(testTopic, capturedTopics[0]);
            Assert.AreEqual(1, capturedMessages.Count);
            Assert.IsAssignableFrom<Models.CountMetricInstance>(capturedMessages[0]);
            Assert.AreEqual(typeof(DiskReadOperation).FullName, capturedMessages[0].TypeFullName);
            Assert.AreEqual(testCategory, capturedMessages[0].Category);
            Assert.AreEqual(testCountMetric.Name, capturedMessages[0].Name);
            Assert.AreEqual(testCountMetric.Description, capturedMessages[0].Description);
            Assert.AreEqual(testEventTime, capturedMessages[0].EventTime);


            testKafkaMetricLogger.Dispose();
            testKafkaMetricLogger = new KafkaMetricLoggerWithProtectedMembers
            (
                testCategory,
                testTopic,
                new ProducerConfig(),
                true, // logMetricDescriptionAsBlankString
                mockBufferProcessingStrategy,
                IntervalMetricBaseTimeUnit.Nanosecond,
                true,
                mockProducer,
                mockDateTimeProvider,
                mockStopwatch,
                mockGuidProvider
            );
            capturedTopics.Clear();
            capturedMessages.Clear();

            testKafkaMetricLogger.ProcessCountMetricEvents(testCountMetricEvents);

            Assert.AreEqual(1, capturedTopics.Count);
            Assert.AreEqual(testTopic, capturedTopics[0]);
            Assert.AreEqual(1, capturedMessages.Count);
            Assert.IsAssignableFrom<Models.CountMetricInstance>(capturedMessages[0]);
            Assert.AreEqual(typeof(DiskReadOperation).FullName, capturedMessages[0].TypeFullName);
            Assert.AreEqual(testCategory, capturedMessages[0].Category);
            Assert.AreEqual(testCountMetric.Name, capturedMessages[0].Name);
            Assert.AreEqual("", capturedMessages[0].Description);
            Assert.AreEqual(testEventTime, capturedMessages[0].EventTime);
        }

        [Test]
        public void ProcessStatusMetricEvents_ExceptionSendingViaProducer()
        {
            AvailableMemory testStatusMetric = new();
            Int64 testValue = 123000;
            System.DateTime testEventTime = CreateDataTimeFromString("2026-06-29 21:25:06.0070000");
            List<Tuple<StatusMetric, Int64, System.DateTime>> testStatusMetricEvents = new()
            {
                new Tuple<StatusMetric, Int64, System.DateTime>(testStatusMetric, testValue, testEventTime)
            };
            var mockException = new Exception("Mock exception");
            mockProducer.ProduceAsync(Arg.Any<String>(), Arg.Any<Message<Null, Models.MetricInstanceBase>>()).Returns(Task.FromException<DeliveryResult<Null, Models.MetricInstanceBase>>(mockException));

            var e = Assert.Throws<Exception>(delegate
            {
                testKafkaMetricLogger.ProcessStatusMetricEvents(testStatusMetricEvents);
            });

            Assert.That(e.Message, Does.StartWith($"Failed to send status metrics to kafka cluster via producer."));
            Assert.That(e.InnerException.InnerException == mockException);
        }

        [Test]
        public void ProcessStatusMetricEvents()
        {
            AvailableMemory testStatusMetric = new();
            Int64 testValue = 123000;
            System.DateTime testEventTime = CreateDataTimeFromString("2026-06-29 21:25:06.0070000");
            List<String> capturedTopics = new();
            List<Tuple<StatusMetric, Int64, System.DateTime>> testStatusMetricEvents = new()
            {
                new Tuple<StatusMetric, Int64, System.DateTime>(testStatusMetric, testValue, testEventTime)
            };
            List<Models.MetricInstanceBase> capturedMessages = new();
            Action<String> topicArgumentAction = (String topic) => { capturedTopics.Add(topic); };
            Action<Message<Null, Models.MetricInstanceBase>> messageArgumentAction = (Message<Null, Models.MetricInstanceBase> message) => { capturedMessages.Add(message.Value); };
            mockProducer.ProduceAsync(Arg.Do<String>(topicArgumentAction), Arg.Do<Message<Null, Models.MetricInstanceBase>>(messageArgumentAction));

            testKafkaMetricLogger.ProcessStatusMetricEvents(testStatusMetricEvents);

            Assert.AreEqual(1, capturedTopics.Count);
            Assert.AreEqual(testTopic, capturedTopics[0]);
            Assert.AreEqual(1, capturedMessages.Count);
            Assert.IsAssignableFrom<Models.StatusMetricInstance>(capturedMessages[0]);
            Assert.AreEqual(typeof(AvailableMemory).FullName, capturedMessages[0].TypeFullName);
            Assert.AreEqual(testCategory, capturedMessages[0].Category);
            Assert.AreEqual(testStatusMetric.Name, capturedMessages[0].Name);
            Assert.AreEqual(testStatusMetric.Description, capturedMessages[0].Description);
            Assert.AreEqual(testEventTime, capturedMessages[0].EventTime);
            Assert.AreEqual(testValue, ((Models.StatusMetricInstance)capturedMessages[0]).Value);


            testKafkaMetricLogger.Dispose();
            testKafkaMetricLogger = new KafkaMetricLoggerWithProtectedMembers
            (
                testCategory,
                testTopic,
                new ProducerConfig(),
                true, // logMetricDescriptionAsBlankString
                mockBufferProcessingStrategy,
                IntervalMetricBaseTimeUnit.Nanosecond,
                true,
                mockProducer,
                mockDateTimeProvider,
                mockStopwatch,
                mockGuidProvider
            );
            capturedTopics.Clear();
            capturedMessages.Clear();

            testKafkaMetricLogger.ProcessStatusMetricEvents(testStatusMetricEvents);

            Assert.AreEqual(1, capturedTopics.Count);
            Assert.AreEqual(testTopic, capturedTopics[0]);
            Assert.AreEqual(1, capturedMessages.Count);
            Assert.IsAssignableFrom<Models.StatusMetricInstance>(capturedMessages[0]);
            Assert.AreEqual(typeof(AvailableMemory).FullName, capturedMessages[0].TypeFullName);
            Assert.AreEqual(testCategory, capturedMessages[0].Category);
            Assert.AreEqual(testStatusMetric.Name, capturedMessages[0].Name);
            Assert.AreEqual("", capturedMessages[0].Description);
            Assert.AreEqual(testEventTime, capturedMessages[0].EventTime);
            Assert.AreEqual(testValue, ((Models.StatusMetricInstance)capturedMessages[0]).Value);
        }

        [Test]
        public void ProcessIntervalMetricEvents_ExceptionSendingViaProducer()
        {
            MessageReceiveTime testIntervalMetric = new();
            Int64 testDuration = 123000;
            System.DateTime testEventTime = CreateDataTimeFromString("2026-06-29 21:30:07.0080000");
            List<Tuple<IntervalMetric, Int64, System.DateTime>> testStatusMetricEvents = new()
            {
                new Tuple<IntervalMetric, Int64, System.DateTime>(testIntervalMetric, testDuration, testEventTime)
            };
            var mockException = new Exception("Mock exception");
            mockProducer.ProduceAsync(Arg.Any<String>(), Arg.Any<Message<Null, Models.MetricInstanceBase>>()).Returns(Task.FromException<DeliveryResult<Null, Models.MetricInstanceBase>>(mockException));

            var e = Assert.Throws<Exception>(delegate
            {
                testKafkaMetricLogger.ProcessIntervalMetricEvents(testStatusMetricEvents);
            });

            Assert.That(e.Message, Does.StartWith($"Failed to send interval metrics to kafka cluster via producer."));
            Assert.That(e.InnerException.InnerException == mockException);
        }

        [Test]
        public void ProcessIntervalMetricEvents()
        {
            MessageReceiveTime testIntervalMetric = new();
            Int64 testDuration = 123000;
            System.DateTime testEventTime = CreateDataTimeFromString("2026-06-29 21:30:07.0080000");
            List<String> capturedTopics = new();
            List<Tuple<IntervalMetric, Int64, System.DateTime>> testStatusMetricEvents = new()
            {
                new Tuple<IntervalMetric, Int64, System.DateTime>(testIntervalMetric, testDuration, testEventTime)
            };
            List<Models.MetricInstanceBase> capturedMessages = new();
            Action<String> topicArgumentAction = (String topic) => { capturedTopics.Add(topic); };
            Action<Message<Null, Models.MetricInstanceBase>> messageArgumentAction = (Message<Null, Models.MetricInstanceBase> message) => { capturedMessages.Add(message.Value); };
            mockProducer.ProduceAsync(Arg.Do<String>(topicArgumentAction), Arg.Do<Message<Null, Models.MetricInstanceBase>>(messageArgumentAction));

            testKafkaMetricLogger.ProcessIntervalMetricEvents(testStatusMetricEvents);

            Assert.AreEqual(1, capturedTopics.Count);
            Assert.AreEqual(testTopic, capturedTopics[0]);
            Assert.AreEqual(1, capturedMessages.Count);
            Assert.IsAssignableFrom<Models.IntervalMetricInstance>(capturedMessages[0]);
            Assert.AreEqual(typeof(MessageReceiveTime).FullName, capturedMessages[0].TypeFullName);
            Assert.AreEqual(testCategory, capturedMessages[0].Category);
            Assert.AreEqual(testIntervalMetric.Name, capturedMessages[0].Name);
            Assert.AreEqual(testIntervalMetric.Description, capturedMessages[0].Description);
            Assert.AreEqual(testEventTime, capturedMessages[0].EventTime);
            Assert.AreEqual(testDuration, ((Models.IntervalMetricInstance)capturedMessages[0]).Duration);


            testKafkaMetricLogger.Dispose();
            testKafkaMetricLogger = new KafkaMetricLoggerWithProtectedMembers
            (
                testCategory,
                testTopic,
                new ProducerConfig(),
                true, // logMetricDescriptionAsBlankString
                mockBufferProcessingStrategy,
                IntervalMetricBaseTimeUnit.Nanosecond,
                true,
                mockProducer,
                mockDateTimeProvider,
                mockStopwatch,
                mockGuidProvider
            );
            capturedTopics.Clear();
            capturedMessages.Clear();

            testKafkaMetricLogger.ProcessIntervalMetricEvents(testStatusMetricEvents);

            Assert.AreEqual(1, capturedTopics.Count);
            Assert.AreEqual(testTopic, capturedTopics[0]);
            Assert.AreEqual(1, capturedMessages.Count);
            Assert.IsAssignableFrom<Models.IntervalMetricInstance>(capturedMessages[0]);
            Assert.AreEqual(typeof(MessageReceiveTime).FullName, capturedMessages[0].TypeFullName);
            Assert.AreEqual(testCategory, capturedMessages[0].Category);
            Assert.AreEqual(testIntervalMetric.Name, capturedMessages[0].Name);
            Assert.AreEqual("", capturedMessages[0].Description);
            Assert.AreEqual(testEventTime, capturedMessages[0].EventTime);
            Assert.AreEqual(testDuration, ((Models.IntervalMetricInstance)capturedMessages[0]).Duration);
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

        #region Nested Classes

        /// <summary>
        /// Version of the KafkaMetricLogger class where private and protected methods are exposed as public so that they can be unit tested.
        /// </summary>
        private class KafkaMetricLoggerWithProtectedMembers : KafkaMetricLogger
        {
            /// <summary>
            /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.Kafka.UnitTests.KafkaMetricLoggerTests+KafkaMetricLoggerWithProtectedMembers class.
            /// </summary>
            /// <param name="category">The category to log all metrics under.</param>
            /// <param name="topic">The kafka topic to write metrics to.</param>
            /// <param name="producerConfig">The configuration to apply to the underlying <see cref="IProducer{TKey, TValue}"/>.</param>
            /// <param name="logMetricDescriptionAsBlankString">Whether metric's 'description' fields should be sent as a blank strings (and thereby reducing the message sizes).</param>
            /// <param name="bufferProcessingStrategy">Object which implements a processing strategy for the buffers (queues).</param>
            /// <param name="intervalMetricBaseTimeUnit">The base time unit to use to log interval metrics.</param>
            /// <param name="intervalMetricChecking">Specifies whether an exception should be thrown if the correct order of interval metric logging is not followed (e.g. End() method called before Begin()).  Note that this parameter only has an effect when running in 'non-interleaved' mode.</param>
            /// <param name="producer">A mock <see cref="IProducer{TKey, TValue}"/>.</param>
            /// <param name="dateTime">A mock <see cref="StandardAbstraction.IDateTime"/>.</param>
            /// <param name="stopwatch">A mock <see cref="StandardAbstraction.IStopwatch"/>.</param>
            /// <param name="guidProvider">A mock <see cref="IGuidProvider"/>.</param>
            public KafkaMetricLoggerWithProtectedMembers
            (
                String category,
                String topic,
                ProducerConfig producerConfig,
                Boolean logMetricDescriptionAsBlankString,
                IBufferProcessingStrategy bufferProcessingStrategy,
                IntervalMetricBaseTimeUnit intervalMetricBaseTimeUnit,
                Boolean intervalMetricChecking,
                IProducer<Null, Models.MetricInstanceBase> producer,
                StandardAbstraction.IDateTime dateTime,
                StandardAbstraction.IStopwatch stopwatch,
                IGuidProvider guidProvider
            )
                 : base
            (
                category,
                topic,
                producerConfig,
                logMetricDescriptionAsBlankString,
                bufferProcessingStrategy,
                intervalMetricBaseTimeUnit,
                intervalMetricChecking,
                producer,
                dateTime,
                stopwatch,
                guidProvider
            )
            {
            }

            public void ProcessCountMetricEvents(IEnumerable<Tuple<CountMetric, System.DateTime>> countMetricEvents)
            {
                var countMetricEventsQueue = new Queue<CountMetricEventInstance>();
                foreach (Tuple<CountMetric, System.DateTime> currentCountMetricEvent in countMetricEvents)
                {
                    countMetricEventsQueue.Enqueue(new CountMetricEventInstance(currentCountMetricEvent.Item1, currentCountMetricEvent.Item2));
                }
                ProcessCountMetricEvents(countMetricEventsQueue);
            }

            public void ProcessAmountMetricEvents(IEnumerable<Tuple<AmountMetric, Int64, System.DateTime>> amountMetricEvents)
            {
                var amountMetricEventsQueue = new Queue<AmountMetricEventInstance>();
                foreach (Tuple<AmountMetric, Int64, System.DateTime> currentAmountMetricEvent in amountMetricEvents)
                {
                    amountMetricEventsQueue.Enqueue(new AmountMetricEventInstance(currentAmountMetricEvent.Item1, currentAmountMetricEvent.Item2, currentAmountMetricEvent.Item3));
                }
                ProcessAmountMetricEvents(amountMetricEventsQueue);
            }

            public void ProcessStatusMetricEvents(IEnumerable<Tuple<StatusMetric, Int64, System.DateTime>> statusMetricEvents)
            {
                var statusMetricEventsQueue = new Queue<StatusMetricEventInstance>();
                foreach (Tuple<StatusMetric, Int64, System.DateTime> currentStatusMetricEvent in statusMetricEvents)
                {
                    statusMetricEventsQueue.Enqueue(new StatusMetricEventInstance(currentStatusMetricEvent.Item1, currentStatusMetricEvent.Item2, currentStatusMetricEvent.Item3));
                }
                ProcessStatusMetricEvents(statusMetricEventsQueue);
            }

            public void ProcessIntervalMetricEvents(IEnumerable<Tuple<IntervalMetric, Int64, System.DateTime>> intervalMetricEvents)
            {
                var intervalMetricEventsQueue = new Queue<Tuple<IntervalMetricEventInstance, Int64>>();
                foreach (Tuple<IntervalMetric, Int64, System.DateTime> currentIntervalMetricEvent in intervalMetricEvents)
                {
                    intervalMetricEventsQueue.Enqueue(new Tuple<IntervalMetricEventInstance, Int64>(new IntervalMetricEventInstance(currentIntervalMetricEvent.Item1, IntervalMetricEventTimePoint.Start, currentIntervalMetricEvent.Item3), currentIntervalMetricEvent.Item2));
                }
                ProcessIntervalMetricEvents(intervalMetricEventsQueue);
            }
        }

        #endregion
    }
}
