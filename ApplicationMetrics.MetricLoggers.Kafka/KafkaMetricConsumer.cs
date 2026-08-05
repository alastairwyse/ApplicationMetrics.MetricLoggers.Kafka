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
using System.Runtime.ExceptionServices;
using System.Threading;
using Confluent.Kafka;
using ApplicationMetrics.MetricLoggers.Kafka.Models;

namespace ApplicationMetrics.MetricLoggers.Kafka
{
    /// <summary>
    /// Consumes and returns instances of <see cref="IntervalMetricInstance"/> from a Kafka cluster.
    /// </summary>
    public class KafkaMetricConsumer : IDisposable
    {
        /// <summary>The kafka topic to read metrics from.</summary>
        protected String topic;
        /// <summary>The <see cref="IConsumer{TKey, TValue}"/> instance to use to read metric events.</summary>
        protected IConsumer<Ignore, Models.MetricInstanceBase> consumer;
        /// <summary>The maximum time to wait for a message from the  Kafka cluster before timing out and reconnecting (in milliseconds).</summary>
        protected Int32 consumeLoopTimeout;
        /// <summary>Worker thread which consumes/messages from the Kafka cluster.</summary>
        protected Thread consumeWorkerThread;
        /// <summary>Whether a stop request has been received.</summary>
        protected volatile Boolean stopRequestReceived;
        /// <summary>An action to invoke if an error occurs during message consumption.  Accepts a single parameter which is the <see cref="Exception"/> containing details of the error.</summary>
        protected Action<Exception> consumeExceptionAction;
        /// <summary>Set with any exception and state/context information which occurrs on the worker thread.  Null if no exception has occurred.</summary>
        protected ExceptionDispatchInfo consumeExceptionDispatchInfo;
        /// <summary>Indicates whether the object has been disposed.</summary>
        protected Boolean disposed;

        /// <summary>
        /// An event which is raised when a metric instance is consumed/received.
        /// </summary>
        public event EventHandler<MetricInstanceBase> MetricEventReceived;

        /// <summary>
        /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.Kafka.KafkaMetricConsumer class.
        /// </summary>
        /// <param name="topic">The kafka topic to read metrics from.</param>
        /// <param name="consumerConfig">The configuration to apply to the underlying <see cref="IConsumer{TKey, TValue}"/>.</param>
        /// <param name="consumeLoopTimeout">The maximum time to wait for a message from the  Kafka cluster before timing out and reconnecting (in milliseconds).</param>
        /// <param name="consumeExceptionAction">An action to invoke if an error occurs during message consumption.  Accepts a single parameter which is the <see cref="Exception"/> containing details of the error.</param>
        public KafkaMetricConsumer(String topic, ConsumerConfig consumerConfig, Int32 consumeLoopTimeout, Action<Exception> consumeExceptionAction)
        {
            ThrowExceptionIfStringParameterNullOrWhitespace(nameof(topic), topic);

            this.topic = topic;
            Initiailize(consumerConfig, consumeLoopTimeout, consumeExceptionAction);
        }

        /// <summary>
        /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.Kafka.KafkaMetricConsumer class.
        /// </summary>
        /// <param name="topic">The kafka topic to read metrics from.</param>
        /// <param name="consumerConfig">The configuration to apply to the underlying <see cref="IConsumer{TKey, TValue}"/>.</param>
        /// <param name="consumeLoopTimeout">The maximum time to wait for a message from the  Kafka cluster before timing out and reconnecting (in milliseconds).</param>
        /// <param name="consumeExceptionAction">An action to invoke if an error occurs during message consumption.  Accepts a single parameter which is the <see cref="Exception"/> containing details of the error.</param>
        /// <param name="consumer">A mock <see cref="IConsumer{TKey, TValue}"/>.</param>
        /// <remarks>This constructor is included to facilitate unit testing.</remarks>
        public KafkaMetricConsumer(String topic, ConsumerConfig consumerConfig, Int32 consumeLoopTimeout, Action<Exception> consumeExceptionAction, IConsumer<Ignore, Models.MetricInstanceBase> consumer)
        {
            ThrowExceptionIfStringParameterNullOrWhitespace(nameof(topic), topic);

            this.consumer = consumer;
            this.consumeLoopTimeout = consumeLoopTimeout;
            this.consumeExceptionAction = consumeExceptionAction;
            stopRequestReceived = false;
            consumeWorkerThread = new Thread(() =>
            {
                Consume();
            });
            consumeWorkerThread.Name = "ApplicationMetrics.MetricLoggers.Kafka.KafkaMetricConsumer metric event consumer/receiver worker thread.";
            consumeWorkerThread.IsBackground = true;
            disposed = false;
        }

        /// <summary>
        /// Starts consuming the metric instances.
        /// </summary>
        public void Start()
        {
            consumeWorkerThread.Start();
        }

        /// <summary>
        /// Stops consuming the metric instances.
        /// </summary>
        public void Stop()
        {
            stopRequestReceived = true;
            consumeWorkerThread.Join();
            if (consumeExceptionDispatchInfo != null)
            {
                consumeExceptionDispatchInfo.Throw();
            }
        }

        #region Private/Protected Methods

        /// <summary>
        /// Consumes metric events from the Kafka cluster.
        /// </summary>
        protected void Consume()
        {
            consumer.Subscribe(topic);
            while (stopRequestReceived == false)
            {
                try
                {
                    ConsumeResult<Ignore, MetricInstanceBase> consumeResult = consumer.Consume(consumeLoopTimeout);
                    if (consumeResult != null)
                    {
                        OnMetricEventReceived(consumeResult.Message.Value);
                    }
                }
                catch (Exception e)
                {
                    var wrappedException = new Exception($"Exception occurred on message consumer worker thread at {DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffffff")}.", e);
                    consumeExceptionAction.Invoke(wrappedException);
                    Interlocked.Exchange(ref consumeExceptionDispatchInfo, ExceptionDispatchInfo.Capture(wrappedException));
                    stopRequestReceived = true;
                }
            }
            consumer.Close();
        }

        /// <summary>
        /// Raises the <see cref="KafkaMetricConsumer.MetricEventReceived"/> event.
        /// </summary>
        /// <param name="metricInstance">The metric instance which was consumed/received.</param>
        protected virtual void OnMetricEventReceived(MetricInstanceBase metricInstance)
        {
            if (MetricEventReceived != null)
            {
                MetricEventReceived(this, metricInstance);
            }
        }

        #pragma warning disable 1591

        protected void Initiailize(ConsumerConfig consumerConfig, Int32 consumeLoopTimeout, Action<Exception> consumeExceptionAction)
        {
            var consumerBuilder = new ConsumerBuilder<Ignore, Models.MetricInstanceBase>(consumerConfig);
            consumerBuilder.SetValueDeserializer(new MetricInstanceDeserializer());
            consumer = consumerBuilder.Build();
            this.consumeLoopTimeout = consumeLoopTimeout;
            this.consumeExceptionAction = consumeExceptionAction;
            stopRequestReceived = false;
            consumeWorkerThread = new Thread(() => 
            {
                Consume();
            });
            consumeWorkerThread.Name = "ApplicationMetrics.MetricLoggers.Kafka.KafkaMetricConsumer metric event consumer/receiver worker thread.";
            consumeWorkerThread.IsBackground = true;
            disposed = false;
        }

        protected void ThrowExceptionIfStringParameterNullOrWhitespace(String parameterName, String parameterValue)
        {
            if (String.IsNullOrWhiteSpace(parameterValue) == true)
                throw new ArgumentException($"Parameter '{parameterName}' must contain a value.", parameterName);
        }

        #pragma warning restore 1591

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
