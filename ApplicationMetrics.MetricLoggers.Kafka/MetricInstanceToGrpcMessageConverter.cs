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
using ApplicationMetrics.MetricLoggers.Kafka.Grpc.GeneratedCode.V1;

namespace ApplicationMetrics.MetricLoggers.Kafka
{
    /// <summary>
    /// Converts between subclasses of <see cref="MetricInstanceBase"/> and their equivalent gRPC message.
    /// </summary>
    internal class MetricInstanceToGrpcMessageConverter
    {
        /// <summary>
        /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.Kafka.MetricInstanceToGrpcMessageConverter class.
        /// </summary>
        public MetricInstanceToGrpcMessageConverter() 
        { 
        }

        /// <summary>
        /// Converts a <see cref="MetricInstanceUnion"/> message to a subclass of <see cref="MetricInstanceBase"/>.
        /// </summary>
        /// <param name="protobufMetricInstance">The message to convert.</param>
        /// <returns>The message converted to a subclass of <see cref="MetricInstanceBase"/>.</returns>
        public MetricInstanceBase Convert(MetricInstanceUnion protobufMetricInstance)
        {
            switch (protobufMetricInstance.ItemCase)
            {

            }
        }
    }
}
