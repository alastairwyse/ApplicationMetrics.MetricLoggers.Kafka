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

            Proto.MetricInstanceUnion protoResult = testMetricInstanceToProtobufMessageConverter.Convert(messageReceivedInstance);

            Assert.AreEqual(messageReceivedMetric.GetType().FullName, protoResult.CountMetricInstance.BaseProperties.TypeFullName);
            Assert.AreEqual(countMetricCategory, protoResult.CountMetricInstance.BaseProperties.Category);
            Assert.AreEqual(messageReceivedMetric.Name, protoResult.CountMetricInstance.BaseProperties.Name);
            Assert.AreEqual(messageReceivedMetric.Description, protoResult.CountMetricInstance.BaseProperties.Description);
            Assert.AreEqual(countMetricEventTime, protoResult.CountMetricInstance.BaseProperties.EventTime.ToDateTime());
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
