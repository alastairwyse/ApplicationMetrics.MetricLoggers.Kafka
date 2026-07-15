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

namespace ApplicationMetrics.MetricLoggers.Kafka
{
    /// <summary>
    /// Writes metric events to a Kafka broker.
    /// </summary>
    public class KafkaMetricLogger : MetricLoggerBuffer
    {
        /// <summary>
        ///  Initialises a new instance of the ApplicationMetrics.MetricLoggers.Kafka.KafkaMetricLogger class.
        /// </summary>
        /// <param name="bufferProcessingStrategy">Object which implements a processing strategy for the buffers (queues).</param>
        /// <param name="intervalMetricBaseTimeUnit">The base time unit to use to log interval metrics.</param>
        /// <param name="intervalMetricChecking">Specifies whether an exception should be thrown if the correct order of interval metric logging is not followed (e.g. End() method called before Begin()).  Note that this parameter only has an effect when running in 'non-interleaved' mode.</param>
        public KafkaMetricLogger(IBufferProcessingStrategy bufferProcessingStrategy, IntervalMetricBaseTimeUnit intervalMetricBaseTimeUnit, Boolean intervalMetricChecking)
             : base(bufferProcessingStrategy, intervalMetricBaseTimeUnit, intervalMetricChecking)
        {

        }

        /// <inheritdoc/>
        protected override void ProcessAmountMetricEvents(Queue<AmountMetricEventInstance> amountMetricEvents)
        {
            throw new NotImplementedException();
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
    }
}
