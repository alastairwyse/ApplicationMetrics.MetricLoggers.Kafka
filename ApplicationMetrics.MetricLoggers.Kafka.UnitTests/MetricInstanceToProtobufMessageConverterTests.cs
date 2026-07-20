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
using ApplicationMetrics.MetricLoggers.Kafka;
using Proto = ApplicationMetrics.MetricLoggers.Kafka.Grpc.GeneratedCode.V1;
using ApplicationMetrics.MetricLoggers.Kafka.Models;
using NUnit.Framework;

namespace ApplicationMetrics.MetricLoggers.Kafka.UnitTests
{
    /// <summary>
    /// Unit tests for the ApplicationMetrics.MetricLoggers.Kafka.MetricInstanceToProtobufMessageConverter class.
    /// </summary>
    public class MetricInstanceToProtobufMessageConverterTests
    {
        private MetricInstanceToProtobufMessageConverter testMetricInstanceToProtobufMessageConverter;

        [SetUp]
        protected void SetUp()
        {
            testMetricInstanceToProtobufMessageConverter = new MetricInstanceToProtobufMessageConverter();
        }

        [Test]
        public void Convert()
        {
            const String countMetricCategory = "CountMetricCategory";
            DateTime countMetricEventTime = DateTime.UtcNow;
            var messageReceivedMetric = new MessageReceived();
            var messageReceivedInstance = new CountMetricInstance(messageReceivedMetric.GetType().FullName, countMetricCategory, messageReceivedMetric.Name, messageReceivedMetric.Description, countMetricEventTime);

            Proto.MetricInstanceUnion protoCountMetricResult = testMetricInstanceToProtobufMessageConverter.Convert(messageReceivedInstance);

            Assert.AreEqual(messageReceivedMetric.GetType().FullName, protoCountMetricResult.CountMetricInstance.BaseProperties.TypeFullName);
            Assert.AreEqual(countMetricCategory, protoCountMetricResult.CountMetricInstance.BaseProperties.Category);
            Assert.AreEqual(messageReceivedMetric.Name, protoCountMetricResult.CountMetricInstance.BaseProperties.Name);
            Assert.AreEqual(messageReceivedMetric.Description, protoCountMetricResult.CountMetricInstance.BaseProperties.Description);
            Assert.AreEqual(countMetricEventTime, protoCountMetricResult.CountMetricInstance.BaseProperties.EventTime.ToDateTime());
            

            const String amountMetricCategory = "AmountMetricCategory";
            DateTime amountMetricEventTime = DateTime.UtcNow;
            var messageBytesReceivedMetric = new MessageBytesReceived();
            var messageBytesReceivedInstance = new AmountMetricInstance(messageBytesReceivedMetric.GetType().FullName, amountMetricCategory, messageBytesReceivedMetric.Name, messageBytesReceivedMetric.Description, amountMetricEventTime, 10000);

            Proto.MetricInstanceUnion protoAmountMetricResult = testMetricInstanceToProtobufMessageConverter.Convert(messageBytesReceivedInstance);

            Assert.AreEqual(messageBytesReceivedMetric.GetType().FullName, protoAmountMetricResult.AmountMetricInstance.BaseProperties.TypeFullName);
            Assert.AreEqual(amountMetricCategory, protoAmountMetricResult.AmountMetricInstance.BaseProperties.Category);
            Assert.AreEqual(messageBytesReceivedMetric.Name, protoAmountMetricResult.AmountMetricInstance.BaseProperties.Name);
            Assert.AreEqual(messageBytesReceivedMetric.Description, protoAmountMetricResult.AmountMetricInstance.BaseProperties.Description);
            Assert.AreEqual(amountMetricEventTime, protoAmountMetricResult.AmountMetricInstance.BaseProperties.EventTime.ToDateTime());
            Assert.AreEqual(10000, protoAmountMetricResult.AmountMetricInstance.Amount);
            

            const String statusMetricCategory = "StatusMetricCategory";
            DateTime statusMetricEventTime = DateTime.UtcNow;
            var availableMemoryMetric = new AvailableMemory();
            var availableMemoryInstance = new StatusMetricInstance(availableMemoryMetric.GetType().FullName, statusMetricCategory, availableMemoryMetric.Name, availableMemoryMetric.Description, statusMetricEventTime, 2000);

            Proto.MetricInstanceUnion protoStatusMetricResult = testMetricInstanceToProtobufMessageConverter.Convert(availableMemoryInstance);

            Assert.AreEqual(availableMemoryMetric.GetType().FullName, protoStatusMetricResult.StatusMetricInstance.BaseProperties.TypeFullName);
            Assert.AreEqual(statusMetricCategory, protoStatusMetricResult.StatusMetricInstance.BaseProperties.Category);
            Assert.AreEqual(availableMemoryMetric.Name, protoStatusMetricResult.StatusMetricInstance.BaseProperties.Name);
            Assert.AreEqual(availableMemoryMetric.Description, protoStatusMetricResult.StatusMetricInstance.BaseProperties.Description);
            Assert.AreEqual(statusMetricEventTime, protoStatusMetricResult.StatusMetricInstance.BaseProperties.EventTime.ToDateTime());
            Assert.AreEqual(2000, protoStatusMetricResult.StatusMetricInstance.Value);


            const String intervalMetricCategory = "IntervalMetricCategory";
            DateTime intervalMetricEventTime = DateTime.UtcNow;
            var messageProcessingTimeMetric = new MessageProcessingTime();
            var messageProcessingTimeInstance = new IntervalMetricInstance(messageProcessingTimeMetric.GetType().FullName, intervalMetricCategory, messageProcessingTimeMetric.Name, messageProcessingTimeMetric.Description, intervalMetricEventTime, 1234);

            Proto.MetricInstanceUnion protoIntervalMetricResult = testMetricInstanceToProtobufMessageConverter.Convert(messageProcessingTimeInstance);

            Assert.AreEqual(messageProcessingTimeMetric.GetType().FullName, protoIntervalMetricResult.IntervalMetricInstance.BaseProperties.TypeFullName);
            Assert.AreEqual(intervalMetricCategory, protoIntervalMetricResult.IntervalMetricInstance.BaseProperties.Category);
            Assert.AreEqual(messageProcessingTimeMetric.Name, protoIntervalMetricResult.IntervalMetricInstance.BaseProperties.Name);
            Assert.AreEqual(messageProcessingTimeMetric.Description, protoIntervalMetricResult.IntervalMetricInstance.BaseProperties.Description);
            Assert.AreEqual(intervalMetricEventTime, protoIntervalMetricResult.IntervalMetricInstance.BaseProperties.EventTime.ToDateTime());
            Assert.AreEqual(1234, protoIntervalMetricResult.IntervalMetricInstance.Duration);


            MetricInstanceBase countMetricResult = testMetricInstanceToProtobufMessageConverter.Convert(protoCountMetricResult);

            Assert.AreEqual(messageReceivedMetric.GetType().FullName, countMetricResult.TypeFullName);
            Assert.AreEqual(countMetricCategory, countMetricResult.Category);
            Assert.AreEqual(messageReceivedMetric.Name, countMetricResult.Name);
            Assert.AreEqual(messageReceivedMetric.Description, countMetricResult.Description);
            Assert.AreEqual(countMetricEventTime, countMetricResult.EventTime);
            Assert.IsInstanceOf(typeof(CountMetricInstance), countMetricResult);


            MetricInstanceBase amountMetricResult = testMetricInstanceToProtobufMessageConverter.Convert(protoAmountMetricResult);

            Assert.AreEqual(messageBytesReceivedMetric.GetType().FullName, amountMetricResult.TypeFullName);
            Assert.AreEqual(amountMetricCategory, amountMetricResult.Category);
            Assert.AreEqual(messageBytesReceivedMetric.Name, amountMetricResult.Name);
            Assert.AreEqual(messageBytesReceivedMetric.Description, amountMetricResult.Description);
            Assert.AreEqual(amountMetricEventTime, amountMetricResult.EventTime);
            Assert.IsInstanceOf(typeof(AmountMetricInstance), amountMetricResult);
            Assert.AreEqual(10000, ((AmountMetricInstance)amountMetricResult).Amount);


            MetricInstanceBase statusMetricResult = testMetricInstanceToProtobufMessageConverter.Convert(protoStatusMetricResult);

            Assert.AreEqual(availableMemoryMetric.GetType().FullName, statusMetricResult.TypeFullName);
            Assert.AreEqual(statusMetricCategory, statusMetricResult.Category);
            Assert.AreEqual(availableMemoryMetric.Name, statusMetricResult.Name);
            Assert.AreEqual(availableMemoryMetric.Description, statusMetricResult.Description);
            Assert.AreEqual(statusMetricEventTime, statusMetricResult.EventTime);
            Assert.IsInstanceOf(typeof(StatusMetricInstance), statusMetricResult);
            Assert.AreEqual(2000, ((StatusMetricInstance)statusMetricResult).Value);


            MetricInstanceBase intervalMetricResult = testMetricInstanceToProtobufMessageConverter.Convert(protoIntervalMetricResult);

            Assert.AreEqual(messageProcessingTimeMetric.GetType().FullName, intervalMetricResult.TypeFullName);
            Assert.AreEqual(intervalMetricCategory, intervalMetricResult.Category);
            Assert.AreEqual(messageProcessingTimeMetric.Name, intervalMetricResult.Name);
            Assert.AreEqual(messageProcessingTimeMetric.Description, intervalMetricResult.Description);
            Assert.AreEqual(intervalMetricEventTime, intervalMetricResult.EventTime);
            Assert.IsInstanceOf(typeof(IntervalMetricInstance), intervalMetricResult);
            Assert.AreEqual(1234, ((IntervalMetricInstance)intervalMetricResult).Duration);
        }

        #region Nested Classes

        protected class MessageReceived : CountMetric
        {
            protected static String staticName = "MessageReceived";
            protected static String staticDescription = "Represents receiving a message from an external source.";

            public MessageReceived()
            {
                base.name = staticName;
                base.description = staticDescription;
            }
        }

        protected class MessageBytesReceived : AmountMetric
        {
            protected static String staticName = "MessageBytesReceived";
            protected static String staticDescription = "Represents the number of bytes received when receiving a message from an external source.";

            public MessageBytesReceived()
            {
                base.name = staticName;
                base.description = staticDescription;
            }
        }

        protected class AvailableMemory : StatusMetric
        {
            protected static String staticName = "AvailableMemory";
            protected static String staticDescription = "Represents the amount of free memory in the system at a given time.";

            public AvailableMemory()
            {
                base.name = staticName;
                base.description = staticDescription;
            }
        }

        protected class MessageProcessingTime : IntervalMetric
        {
            protected static String staticName = "MessageProcessingTime";
            protected static String staticDescription = "Represents the amount of time taken to process a single message received from a remote source.";

            public MessageProcessingTime()
            {
                base.name = staticName;
                base.description = staticDescription;
            }
        }

        #endregion
    }
}
