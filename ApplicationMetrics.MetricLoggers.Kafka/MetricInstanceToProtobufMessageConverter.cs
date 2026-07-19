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
using Proto = ApplicationMetrics.MetricLoggers.Kafka.Grpc.GeneratedCode.V1;
using ApplicationMetrics.MetricLoggers.Kafka.Models;

namespace ApplicationMetrics.MetricLoggers.Kafka
{
    /// <summary>
    /// Converts between subclasses of <see cref="MetricInstanceBase"/> and their equivalent protocol buffer message.
    /// </summary>
    public class MetricInstanceToProtobufMessageConverter
    {
        /// <summary>
        /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.Kafka.MetricInstanceToProtobufMessageConverter class.
        /// </summary>
        public MetricInstanceToProtobufMessageConverter()
        {
        }

        /// <summary>
        /// Converts a <see cref="Proto.MetricInstanceUnion"/> message to a subclass of <see cref="MetricInstanceBase"/>.
        /// </summary>
        /// <param name="protobufMetricInstance">The message to convert.</param>
        /// <returns>The message converted to a subclass of <see cref="MetricInstanceBase"/>.</returns>
        public MetricInstanceBase Convert(Proto.MetricInstanceUnion protobufMetricInstance)
        {
            switch (protobufMetricInstance.ItemCase)
            {
                case Proto.MetricInstanceUnion.ItemOneofCase.CountMetricInstance:
                    Proto.CountMetricInstance protobufCountMetricInstance = protobufMetricInstance.CountMetricInstance;
                    return new CountMetricInstance
                    (
                        protobufCountMetricInstance.BaseProperties.TypeFullName,
                        protobufCountMetricInstance.BaseProperties.Category,
                        protobufCountMetricInstance.BaseProperties.Name,
                        protobufCountMetricInstance.BaseProperties.Description,
                        protobufCountMetricInstance.BaseProperties.EventTime.ToDateTime()
                    );

                case Proto.MetricInstanceUnion.ItemOneofCase.AmountMetricInstance:
                    Proto.AmountMetricInstance protobufAmountMetricInstance = protobufMetricInstance.AmountMetricInstance;
                    return new AmountMetricInstance
                    (
                        protobufAmountMetricInstance.BaseProperties.TypeFullName,
                        protobufAmountMetricInstance.BaseProperties.Category,
                        protobufAmountMetricInstance.BaseProperties.Name,
                        protobufAmountMetricInstance.BaseProperties.Description,
                        protobufAmountMetricInstance.BaseProperties.EventTime.ToDateTime(),
                        protobufAmountMetricInstance.Amount
                    );

                case Proto.MetricInstanceUnion.ItemOneofCase.StatusMetricInstance:
                    Proto.StatusMetricInstance protobufStatusMetricInstance = protobufMetricInstance.StatusMetricInstance;
                    return new AmountMetricInstance
                    (
                        protobufStatusMetricInstance.BaseProperties.TypeFullName,
                        protobufStatusMetricInstance.BaseProperties.Category,
                        protobufStatusMetricInstance.BaseProperties.Name,
                        protobufStatusMetricInstance.BaseProperties.Description,
                        protobufStatusMetricInstance.BaseProperties.EventTime.ToDateTime(),
                        protobufStatusMetricInstance.Value
                    );

                case Proto.MetricInstanceUnion.ItemOneofCase.IntervalMetricInstance:
                    Proto.IntervalMetricInstance protobufIntervalMetricInstance = protobufMetricInstance.IntervalMetricInstance;
                    return new AmountMetricInstance
                    (
                        protobufIntervalMetricInstance.BaseProperties.TypeFullName,
                        protobufIntervalMetricInstance.BaseProperties.Category,
                        protobufIntervalMetricInstance.BaseProperties.Name,
                        protobufIntervalMetricInstance.BaseProperties.Description,
                        protobufIntervalMetricInstance.BaseProperties.EventTime.ToDateTime(),
                        protobufIntervalMetricInstance.Duration
                    );

                default:
                    throw new Exception($"Encountered unhandled metric instance protobuf message type '{protobufMetricInstance.GetType().FullName}.");
            }
        }

        /// <summary>
        /// Converts a <see cref="MetricInstanceBase"/> message to a <see cref="Proto.MetricInstanceUnion"/>.
        /// </summary>
        /// <param name="protobufMetricInstance">The metric instance to convert.</param>
        /// <returns>The metric instance converted to a <see cref="Proto.MetricInstanceUnion"/>.</returns>
        public Proto.MetricInstanceUnion Convert(MetricInstanceBase metricInstance)
        {
            Proto.MetricInstanceUnion returnMetricInstanceUnion;
            switch (metricInstance)
            {
                case CountMetricInstance countMetricInstance:
                    var protobufCountMetricInstance = new Proto.CountMetricInstance();
                    protobufCountMetricInstance.BaseProperties = CreateBaseProperties(metricInstance);
                    returnMetricInstanceUnion = new Proto.MetricInstanceUnion();
                    returnMetricInstanceUnion.CountMetricInstance = protobufCountMetricInstance;
                    return returnMetricInstanceUnion;

                case AmountMetricInstance amountMetricInstance:
                    var protobufAmountMetricInstance = new Proto.AmountMetricInstance();
                    protobufAmountMetricInstance.BaseProperties = CreateBaseProperties(metricInstance);
                    protobufAmountMetricInstance.Amount = amountMetricInstance.Amount;
                    returnMetricInstanceUnion = new Proto.MetricInstanceUnion();
                    returnMetricInstanceUnion.AmountMetricInstance = protobufAmountMetricInstance;
                    return returnMetricInstanceUnion;

                case StatusMetricInstance statusMetricInstance:
                    var protobufStatusMetricInstance = new Proto.StatusMetricInstance();
                    protobufStatusMetricInstance.BaseProperties = CreateBaseProperties(metricInstance);
                    protobufStatusMetricInstance.Value = statusMetricInstance.Value;
                    returnMetricInstanceUnion = new Proto.MetricInstanceUnion();
                    returnMetricInstanceUnion.StatusMetricInstance = protobufStatusMetricInstance;
                    return returnMetricInstanceUnion;

                case IntervalMetricInstance intervalMetricInstance:
                    var protobufIntervalMetricInstance = new Proto.IntervalMetricInstance();
                    protobufIntervalMetricInstance.BaseProperties = CreateBaseProperties(metricInstance);
                    protobufIntervalMetricInstance.Duration = intervalMetricInstance.Duration;
                    returnMetricInstanceUnion = new Proto.MetricInstanceUnion();
                    returnMetricInstanceUnion.IntervalMetricInstance = protobufIntervalMetricInstance;
                    return returnMetricInstanceUnion;

                default:
                    throw new Exception($"Encountered unhandled metric instance type '{metricInstance.GetType().FullName}.");
            }
        }

        #region Private/Protected Methods

        /// <summary>
        /// Creates an instance of <see cref="Proto.MetricInstanceBase"/> to use as the 'BaseProperties' field in a <see cref="Proto.MetricInstanceUnion"/>.
        /// </summary>
        /// <param name="metricInstance">The metric to create the base properties from.</param>
        /// <returns>The <see cref="Proto.MetricInstanceBase" />.</returns>
        protected Proto.MetricInstanceBase CreateBaseProperties(MetricInstanceBase metricInstance)
        {
            var baseProperties = new Proto.MetricInstanceBase();
            baseProperties.TypeFullName = metricInstance.TypeFullName;
            baseProperties.Category = metricInstance.Category;
            baseProperties.Name = metricInstance.Name;
            baseProperties.Description = metricInstance.Description;
            baseProperties.EventTime = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(metricInstance.EventTime);

            return baseProperties;
        }

        #endregion
    }
}
