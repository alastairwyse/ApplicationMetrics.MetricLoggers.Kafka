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
using ApplicationMetrics;

namespace ApplicationMetrics.MetricLoggers.Kafka.Models
{
    /// <summary>
    /// An instance of a <see cref="CountMetric"/>
    /// </summary>
    public class CountMetricInstance : MetricInstanceBase
    {
        /// <summary>
        /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.Kafka.Models.CountMetricInstance class.
        /// </summary>
        /// <param name="typeFullName">The fully qualified name of the .NET <see cref="Type"/> of the metric.</param>
        /// <param name="name">The name of the metric.</param>
        /// <param name="description">A description of the metric, explaining what it measures and/or represents.</param>
        /// <param name="eventTime">The timestamp when the metric occurred.</param>
        public CountMetricInstance(String typeFullName, String name, String description, DateTime eventTime)
            : base(typeFullName, name, description, eventTime)
        {
        }

        /// <summary>
        /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.Kafka.Models.CountMetricInstance class.
        /// </summary>
        /// <param name="typeFullName">The fully qualified name of the .NET <see cref="Type"/> of the metric.</param>
        /// <param name="category">The category of the metric.</param>
        /// <param name="name">The name of the metric.</param>
        /// <param name="description">A description of the metric, explaining what it measures and/or represents.</param>
        /// <param name="eventTime">The timestamp when the metric occurred.</param>
        public CountMetricInstance(String typeFullName, String category, String name, String description, DateTime eventTime)
            : base(typeFullName, category, name, description, eventTime)
        {
        }
    }
}
