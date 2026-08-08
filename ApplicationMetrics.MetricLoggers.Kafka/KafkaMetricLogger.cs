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

namespace ApplicationMetrics.MetricLoggers.Kafka
{
    /// <summary>
    /// Writes metric events to a Kafka cluster.
    /// </summary>
    public class KafkaMetricLogger : MetricLoggerBuffer
    {
        /// <summary>The category to log all metrics under.</summary>
        protected String category;
        /// <summary>The kafka topic to write metrics to.</summary>
        protected String topic;
        /// <summary>The <see cref="IProducer{TKey, TValue}"/> instance to use to send metric events.</summary>
        protected IProducer<Null, Models.MetricInstanceBase> producer;
        /// <summary>Whether metric's 'description' fields should be sent as a blank strings.</summary>
        protected Boolean logMetricDescriptionAsBlankString;

        /// <summary>
        /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.Kafka.KafkaMetricLogger class.
        /// </summary>
        /// <param name="category">The category to log all metrics under.</param>
        /// <param name="topic">The kafka topic to write metrics to.</param>
        /// <param name="producerConfig">The configuration to apply to the underlying <see cref="IProducer{TKey, TValue}"/>.</param>
        /// <param name="logMetricDescriptionAsBlankString">Whether metric's 'description' fields should be sent as a blank strings (and thereby reducing the message sizes).</param>
        /// <param name="bufferProcessingStrategy">Object which implements a processing strategy for the buffers (queues).</param>
        /// <param name="intervalMetricBaseTimeUnit">The base time unit to use to log interval metrics.</param>
        /// <param name="intervalMetricChecking">Specifies whether an exception should be thrown if the correct order of interval metric logging is not followed (e.g. End() method called before Begin()).  Note that this parameter only has an effect when running in 'non-interleaved' mode.</param>
        /// <param name="kafkaErrorHandlingAction">An action to invoke if a Kafka <see cref="Error"/> occurs when a metric is written to the Kafka cluster..  Accepts a single parameter which is the <see cref="Error"/>.</param>
        /// <param name="logMessageAction">An action to invoke when the underlying Kafka <see cref="IProducer{TKey, TValue}"/> writes a log message.  Accepts a single parameter which is the <see cref="LogMessage"/>.</param>
        /// <remarks>According to the <see href="https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.ProducerBuilder-2.html">documentation</see> for the <see cref="ProducerBuilder{TKey, TValue}.SetErrorHandler(Action{IProducer{TKey, TValue}, Error})"/> and <see cref="ProducerBuilder{TKey, TValue}.SetLogHandler(Action{IProducer{TKey, TValue}, LogMessage})"/> which are used to invoke the <paramref name="kafkaErrorHandlingAction"/> and <paramref name="logMessageAction"/> parameters, exceptions thrown in these actions will be silently ignored.  Hence exceptions thrown in these parameters cannot be caught and acted on by client code.</remarks>
        public KafkaMetricLogger
        (
            String category, 
            String topic, 
            ProducerConfig producerConfig, 
            Boolean logMetricDescriptionAsBlankString, 
            IBufferProcessingStrategy bufferProcessingStrategy, 
            IntervalMetricBaseTimeUnit intervalMetricBaseTimeUnit, 
            Boolean intervalMetricChecking,
            Action<Error> kafkaErrorHandlingAction = null,
            Action<LogMessage> logMessageAction = null
        )
             : base(bufferProcessingStrategy, intervalMetricBaseTimeUnit, intervalMetricChecking)
        {
            ThrowExceptionIfStringParameterNullOrWhitespace(nameof(category), category);
            ThrowExceptionIfStringParameterNullOrWhitespace(nameof(topic), topic);

            this.category = category;
            this.topic = topic;
            this.logMetricDescriptionAsBlankString = logMetricDescriptionAsBlankString;
            var producerBuilder = new ProducerBuilder<Null, Models.MetricInstanceBase>(producerConfig);
            producerBuilder.SetValueSerializer(new MetricInstanceSerializer());
            if (kafkaErrorHandlingAction != null)
            {
                producerBuilder.SetErrorHandler
                (
                    (IProducer<Null, Models.MetricInstanceBase> producer, Error error) =>
                    {
                        kafkaErrorHandlingAction(error);
                    }
                );
            }
            if (logMessageAction != null)
            {
                producerBuilder.SetLogHandler
                (
                    (IProducer<Null, Models.MetricInstanceBase> producer, LogMessage logMessage) =>
                    {
                        logMessageAction(logMessage);
                    }
                );
            }
            producer = producerBuilder.Build();
        }

        /// <summary>
        /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.Kafka.KafkaMetricLogger class.
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
        /// <remarks>This constructor is included to facilitate unit testing.</remarks>
        public KafkaMetricLogger
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
             : base(bufferProcessingStrategy, intervalMetricBaseTimeUnit, intervalMetricChecking, dateTime, stopwatch, guidProvider)
        {
            ThrowExceptionIfStringParameterNullOrWhitespace(nameof(category), category);
            ThrowExceptionIfStringParameterNullOrWhitespace(nameof(topic), topic);

            this.category = category;
            this.topic = topic;
            this.logMetricDescriptionAsBlankString = logMetricDescriptionAsBlankString;
            this.producer = producer;
        }

        #region Private/Protected Methods

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
                    category,
                    currentAmountMetricEvent.Metric.Name,
                    GetMetricDescriptionValue(currentAmountMetricEvent),
                    currentAmountMetricEvent.EventTime,
                    currentAmountMetricEvent.Amount
                );
                produceTasks.Add(producer.ProduceAsync(topic, message));
            }
            try
            {
                Task.WhenAll(produceTasks).Wait();
            }
            catch (Exception e)
            {
                throw new Exception("Failed to send amount metrics to kafka cluster via producer.", e);
            }
        }

        /// <inheritdoc/>
        protected override void ProcessCountMetricEvents(Queue<CountMetricEventInstance> countMetricEvents)
        {
            var produceTasks = new List<Task<DeliveryResult<Null, Models.MetricInstanceBase>>>();
            foreach (CountMetricEventInstance currentCountMetricEvent in countMetricEvents)
            {
                var message = new Message<Null, Models.MetricInstanceBase>();
                message.Value = new Models.CountMetricInstance
                (
                    currentCountMetricEvent.MetricType.FullName,
                    category,
                    currentCountMetricEvent.Metric.Name,
                    GetMetricDescriptionValue(currentCountMetricEvent),
                    currentCountMetricEvent.EventTime
                );
                produceTasks.Add(producer.ProduceAsync(topic, message));
            }
            try
            {
                Task.WhenAll(produceTasks).Wait();
            }
            catch (Exception e)
            {
                throw new Exception("Failed to send count metrics to kafka cluster via producer.", e);
            }
        }

        /// <inheritdoc/>
        protected override void ProcessIntervalMetricEvents(Queue<Tuple<IntervalMetricEventInstance, Int64>> intervalMetricEventsAndDurations)
        {
            var produceTasks = new List<Task<DeliveryResult<Null, Models.MetricInstanceBase>>>();
            foreach (Tuple<IntervalMetricEventInstance, Int64> currentIntervalMetricEvent in intervalMetricEventsAndDurations)
            {
                var message = new Message<Null, Models.MetricInstanceBase>();
                message.Value = new Models.IntervalMetricInstance
                (
                    currentIntervalMetricEvent.Item1.MetricType.FullName,
                    category,
                    currentIntervalMetricEvent.Item1.Metric.Name,
                    GetMetricDescriptionValue(currentIntervalMetricEvent.Item1),
                    currentIntervalMetricEvent.Item1.EventTime,
                    currentIntervalMetricEvent.Item2
                );
                produceTasks.Add(producer.ProduceAsync(topic, message));
            }
            try
            {
                Task.WhenAll(produceTasks).Wait();
            }
            catch (Exception e)
            {
                throw new Exception("Failed to send interval metrics to kafka cluster via producer.", e);
            }
        }

        /// <inheritdoc/>
        protected override void ProcessStatusMetricEvents(Queue<StatusMetricEventInstance> statusMetricEvents)
        {
            var produceTasks = new List<Task<DeliveryResult<Null, Models.MetricInstanceBase>>>();
            foreach (StatusMetricEventInstance currentStatusMetricEvent in statusMetricEvents)
            {
                var message = new Message<Null, Models.MetricInstanceBase>();
                message.Value = new Models.StatusMetricInstance
                (
                    currentStatusMetricEvent.MetricType.FullName,
                    category,
                    currentStatusMetricEvent.Metric.Name,
                    GetMetricDescriptionValue(currentStatusMetricEvent),
                    currentStatusMetricEvent.EventTime,
                    currentStatusMetricEvent.Value
                );
                produceTasks.Add(producer.ProduceAsync(topic, message));
            }
            try
            {
                Task.WhenAll(produceTasks).Wait();
            }
            catch (Exception e)
            {
                throw new Exception("Failed to send status metrics to kafka cluster via producer.", e);
            }
        }

        /// <summary>
        /// Returns the description of the specified <see cref="MetricLoggerBase.MetricEventInstance{T}"/>, or a blank string depending on the values of field 'logMetricDescriptionAsBlankString'.
        /// </summary>
        /// <typeparam name="T">The type of the metric to return the description from.</typeparam>
        /// <param name="metricEventInstance">The metric instance to return the description from.</param>
        /// <returns>The description.</returns>
        protected String GetMetricDescriptionValue<T>(MetricEventInstance<T> metricEventInstance) where T: MetricBase
        {
            if (logMetricDescriptionAsBlankString == true)
            {
                return "";
            }
            else
            {
                return metricEventInstance.Metric.Description;
            }
        }

        #pragma warning disable 1591

        protected void ThrowExceptionIfStringParameterNullOrWhitespace(String parameterName, String parameterValue)
        {
            if (String.IsNullOrWhiteSpace(parameterValue) == true)
                throw new ArgumentException($"Parameter '{parameterName}' must contain a value.", parameterName);
        }

        #pragma warning restore 1591

        #endregion

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
