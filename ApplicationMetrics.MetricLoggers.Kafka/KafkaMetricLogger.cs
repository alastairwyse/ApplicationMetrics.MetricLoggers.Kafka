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
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry.Serdes.Protobuf;
using Proto = ApplicationMetrics.MetricLoggers.Kafka.Grpc.GeneratedCode.V1;
using Models = ApplicationMetrics.MetricLoggers.Kafka.Models;

namespace ApplicationMetrics.MetricLoggers.Kafka
{
    /// <summary>
    /// Writes metric events to a Kafka cluster.
    /// </summary>
    public class KafkaMetricLogger : MetricLoggerBuffer
    {
        // TODO:
        //   Need to take topic name on constructor
        //   Call all messages in queue in a loop and then await all tasks at end
        //   How to handle schemas -> remove Confluent.SchemaRegistry package if I don't use
        //     Avoiding schema -> https://franklinlindemberg.medium.com/using-protobuf-with-apache-kafka-and-without-schema-registry-8535f43a2569

        /// <summary>The kafka topic to write metrics to.</summary>
        protected String topic;
        /// <summary>The <see cref="IProducer{TKey, TValue}"/> instance to use to send metric events.</summary>
        protected IProducer<Null, Models.MetricInstanceBase> producer;

        /// <summary>
        /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.Kafka.KafkaMetricLogger class.
        /// </summary>
        /// <param name="topic">The kafka topic to write metrics to.</param>
        /// <param name="bootstrapServers">A list of host/port pairs used to establish the initial connection to the Kafka cluster (see https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#bootstrap-servers for examples).</param>
        /// <param name="bufferProcessingStrategy">Object which implements a processing strategy for the buffers (queues).</param>
        /// <param name="intervalMetricBaseTimeUnit">The base time unit to use to log interval metrics.</param>
        /// <param name="intervalMetricChecking">Specifies whether an exception should be thrown if the correct order of interval metric logging is not followed (e.g. End() method called before Begin()).  Note that this parameter only has an effect when running in 'non-interleaved' mode.</param>
        public KafkaMetricLogger(String topic, String bootstrapServers, IBufferProcessingStrategy bufferProcessingStrategy, IntervalMetricBaseTimeUnit intervalMetricBaseTimeUnit, Boolean intervalMetricChecking)
             : base(bufferProcessingStrategy, intervalMetricBaseTimeUnit, intervalMetricChecking)
        {
            ThrowExceptionIfStringParameterNullOrWhitespace(nameof(topic), topic);
            ThrowExceptionIfStringParameterNullOrWhitespace(nameof(bootstrapServers), bootstrapServers);

            this.topic = topic;
            var producerConfig = new ProducerConfig();
            producerConfig.BootstrapServers = bootstrapServers;
            var producerBuilder = new ProducerBuilder<Null, Models.MetricInstanceBase>(producerConfig);
            producerBuilder.SetValueSerializer(new MetricInstanceSerializer());
            producer = producerBuilder.Build();
        }

        /// <summary>
        /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.Kafka.KafkaMetricLogger class.
        /// </summary>
        /// <param name="topic">The kafka topic to write metrics to.</param>
        /// <param name="bootstrapServers">A list of host/port pairs used to establish the initial connection to the Kafka cluster (see https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#bootstrap-servers for examples).</param>
        /// <param name="bufferProcessingStrategy">Object which implements a processing strategy for the buffers (queues).</param>
        /// <param name="intervalMetricBaseTimeUnit">The base time unit to use to log interval metrics.</param>
        /// <param name="intervalMetricChecking">Specifies whether an exception should be thrown if the correct order of interval metric logging is not followed (e.g. End() method called before Begin()).  Note that this parameter only has an effect when running in 'non-interleaved' mode.</param>
        public KafkaMetricLogger(String topic, ProducerConfig producerConfig, IBufferProcessingStrategy bufferProcessingStrategy, IntervalMetricBaseTimeUnit intervalMetricBaseTimeUnit, Boolean intervalMetricChecking)
             : base(bufferProcessingStrategy, intervalMetricBaseTimeUnit, intervalMetricChecking)
        {
            ThrowExceptionIfStringParameterNullOrWhitespace(nameof(topic), topic);

            this.topic = topic;
            var producerBuilder = new ProducerBuilder<Null, Models.MetricInstanceBase>(producerConfig);
            producerBuilder.SetValueSerializer(new MetricInstanceSerializer());
            producer = producerBuilder.Build();
        }

        /// <inheritdoc/>
        protected override void ProcessAmountMetricEvents(Queue<AmountMetricEventInstance> amountMetricEvents)
        {
            var produceTasks = new List<Task<DeliveryResult<Null, Models.MetricInstanceBase>>>();
            foreach (AmountMetricEventInstance currentAmountMetricEvent in amountMetricEvents)
            {
                var message = new Message<Null, Models.MetricInstanceBase>();
                message.Value = new Models.AmountMetricInstance
                (
                    currentAmountMetricEvent.MetricType.FullName,
                    currentAmountMetricEvent.Metric.Name, 
                    // TODO: Param for blank description
                );
                produceTasks.Add(producer.ProduceAsync(topic,);
            }
        }

        /// <inheritdoc/>
        protected override void ProcessCountMetricEvents(Queue<CountMetricEventInstance> countMetricEvents)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        protected override void ProcessIntervalMetricEvents(Queue<Tuple<IntervalMetricEventInstance, Int64>> intervalMetricEventsAndDurations)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        protected override void ProcessStatusMetricEvents(Queue<StatusMetricEventInstance> statusMetricEvents)
        {
            throw new NotImplementedException();
        }

        protected void ThrowExceptionIfStringParameterNullOrWhitespace(String parameterName, String parameterValue)
        {
            if (String.IsNullOrWhiteSpace(parameterValue) == true)
                throw new ArgumentException($"Parameter '{parameterName}' must contain a value.", parameterName);
        }

        #region Finalize / Dispose Methods

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            if (!disposed)
            {
                try
                {
                    if (disposing)
                    {
                        // Free other state (managed objects).
                        producer.Dispose();
                    }
                    // Free your own state (unmanaged objects).

                    // Set large fields to null.
                }
                finally
                {
                    base.Dispose(disposing);
                }
            }
        }

        #endregion
    }
}
