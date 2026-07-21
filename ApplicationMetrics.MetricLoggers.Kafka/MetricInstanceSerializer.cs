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
    /// Implementation of <see cref="ISerializer{T}"/> for <see cref="MetricInstanceBase"/> instances.
    /// </summary>
    public class MetricInstanceSerializer : ISerializer<MetricInstanceBase>
    {
        /// <summary>Converter to convert between <see cref="MetricInstanceBase"/> and <see cref="Proto.MetricInstanceUnion"/> instances.</summary>
        protected MetricInstanceToProtobufMessageConverter converter;

        /// <summary>
        /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.Kafka.MetricInstanceSerializer class.
        /// </summary>
        public MetricInstanceSerializer()
        {
            converter = new MetricInstanceToProtobufMessageConverter();
        }

        /// <inheritdoc/>
        public byte[] Serialize(MetricInstanceBase data, SerializationContext context)
        {
            try
            {
                return converter.Convert(data).ToByteArray();
            }
            catch (Exception e)
            {
                throw new Exception($"Failed to convert metric instance of type {data.GetType().FullName} to a {typeof(Proto.MetricInstanceUnion).FullName}.", e);
            }
        }
    }
}
