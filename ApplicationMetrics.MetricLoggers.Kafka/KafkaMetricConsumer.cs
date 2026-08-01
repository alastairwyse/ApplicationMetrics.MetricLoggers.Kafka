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
using Confluent.Kafka;
using ApplicationMetrics.MetricLoggers.Kafka.Models;

namespace ApplicationMetrics.MetricLoggers.Kafka
{
    /// <summary>
    /// Consumes and returns instances of <see cref="IntervalMetricInstance"/> from a Kafka cluster.
    /// </summary>
    public class KafkaMetricConsumer : IDisposable
    {
        // TODO: Test constructor
        //   Is it possible to unit test with a mock producer?
        // Add unit tests for topic null etc...

        /// <summary>The kafka topic to read metrics from.</summary>
        protected String topic;
        /// <summary>The <see cref="IConsumer{TKey, TValue}"/> instance to use to read metric events.</summary>
        protected IConsumer<Ignore, Models.MetricInstanceBase> consumer;
        /// <summary>Indicates whether the object has been disposed.</summary>
        protected Boolean disposed;

        public event EventHandler<MetricInstanceBase> MetricEventReceived;

        /// <summary>
        /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.Kafka.KafkaMetricConsumer class.
        /// </summary>
        /// <param name="topic">The kafka topic to read metrics from.</param>
        /// <param name="bootstrapServers">A list of host/port pairs used to establish the initial connection to the Kafka cluster (see https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#bootstrap-servers for examples).</param>
        public KafkaMetricConsumer(String topic, String bootstrapServers)
        {
            ThrowExceptionIfStringParameterNullOrWhitespace(nameof(topic), topic);

            this.topic = topic;
            var consumerConfig = new ConsumerConfig();
            consumerConfig.BootstrapServers = bootstrapServers;
            var consumerBuilder = new ConsumerBuilder<Ignore, Models.MetricInstanceBase>(consumerConfig);
            consumerBuilder.SetValueDeserializer(new MetricInstanceDeserializer());
            consumer = consumerBuilder.Build();
            disposed = false;
        }

        /// <summary>
        /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.Kafka.KafkaMetricConsumer class.
        /// </summary>
        /// <param name="topic">The kafka topic to read metrics from.</param>
        /// <param name="bootstrapServers">The configuration to apply to the underlying <see cref="IConsumer{TKey, TValue}"/>.</param>
        public KafkaMetricConsumer(String topic, ConsumerConfig consumerConfig)
        {

            ThrowExceptionIfStringParameterNullOrWhitespace(nameof(topic), topic);

            this.topic = topic;
            var consumerBuilder = new ConsumerBuilder<Ignore, Models.MetricInstanceBase>(consumerConfig);
            consumerBuilder.SetValueDeserializer(new MetricInstanceDeserializer());
            consumer = consumerBuilder.Build();
            disposed = false;
        }

        #region Private/Protected Methods

        protected void ThrowExceptionIfStringParameterNullOrWhitespace(String parameterName, String parameterValue)
        {
            if (String.IsNullOrWhiteSpace(parameterValue) == true)
                throw new ArgumentException($"Parameter '{parameterName}' must contain a value.", parameterName);
        }

        #endregion

        #region Finalize / Dispose Methods

        /// <summary>
        /// Releases the unmanaged resources used by the KafkaMetricConsumer.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #pragma warning disable 1591

        ~KafkaMetricConsumer()
        {
            Dispose(false);
        }

        #pragma warning restore 1591

        /// <inheritdoc/>
        protected void Dispose(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    // Free other state (managed objects).
                    consumer.Dispose();
                }
                // Free your own state (unmanaged objects).

                // Set large fields to null.

                disposed = true;
            }
        }

        #endregion
    }
}
