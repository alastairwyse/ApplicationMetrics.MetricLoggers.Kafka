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
using Google.Protobuf;
using Proto = ApplicationMetrics.MetricLoggers.Kafka.Grpc.GeneratedCode.V1;
using ApplicationMetrics.MetricLoggers.Kafka.Models;

namespace ApplicationMetrics.MetricLoggers.Kafka
{
    /// <summary>
    /// Implementation of <see cref="IDeserializer{T}"/> for <see cref="MetricInstanceBase"/> instances.
    /// </summary>
    public class MetricInstanceDeserializer : IDeserializer<MetricInstanceBase>
    {
        /// <summary>Converter to convert between <see cref="MetricInstanceBase"/> and <see cref="Proto.MetricInstanceUnion"/> instances.</summary>
        protected MetricInstanceToProtobufMessageConverter converter;

        /// <summary>
        /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.Kafka.MetricInstanceDeserializer class.
        /// </summary>
        public MetricInstanceDeserializer()
        {
            converter = new MetricInstanceToProtobufMessageConverter();
        }

        /// <inheritdoc/>
        public MetricInstanceBase Deserialize(ReadOnlySpan<Byte> data, Boolean isNull, SerializationContext context)
        {
            Proto.MetricInstanceUnion metricInstanceUnion;
            try
            {
                metricInstanceUnion = Proto.MetricInstanceUnion.Parser.ParseFrom(data);
            }
            catch (Exception e)
            {
                throw new Exception($"Failed to parse {typeof(Proto.MetricInstanceUnion).FullName} from bytes array.", e);
            }
            try
            {
                return converter.Convert(metricInstanceUnion);
            }
            catch (Exception e)
            {
                throw new Exception($"Failed to convert protobuf metric instance to a {typeof(MetricInstanceBase).FullName}.", e);
            }
        }
    }
}
