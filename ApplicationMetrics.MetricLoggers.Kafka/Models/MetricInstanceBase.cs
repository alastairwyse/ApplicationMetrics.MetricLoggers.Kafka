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

namespace ApplicationMetrics.MetricLoggers.Kafka.Models
{
    /// <summary>
    /// Base for metric instance classes.
    /// </summary>
    public abstract class MetricInstanceBase
    {
        /// <summary>The fully qualified name of the .NET <see cref="Type"/> of the metric.</summary>
        public String TypeFullName { get; }

        /// <summary>The category of the metric.</summary>
        public String Category { get; }

        /// <summary>The name of the metric.</summary>
        public String Name { get; }

        /// <summary>A description of the metric, explaining what it measures and/or represents.</summary>
        public String Description { get; }

        /// <summary>The timestamp when the metric occurred.</summary>
        public DateTime EventTime { get; }

        /// <summary>
        /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.Kafka.Models.MetricInstanceBase class.
        /// </summary>
        /// <param name="typeFullName">The fully qualified name of the .NET <see cref="Type"/> of the metric.</param>
        /// <param name="name">The name of the metric.</param>
        /// <param name="description">A description of the metric, explaining what it measures and/or represents.</param>
        /// <param name="eventTime">The timestamp when the metric occurred.</param>
        public MetricInstanceBase(String typeFullName, String name, String description, DateTime eventTime)
        {
            if (eventTime.Kind != DateTimeKind.Utc)
                throw new ArgumentException($"Parameter '{nameof(eventTime)}' must be represented as UTC.", nameof(eventTime));

            TypeFullName = typeFullName;
            Category = null;
            Name = name;
            Description = description;
            EventTime = eventTime;
        }

        /// <summary>
        /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.Kafka.Models.MetricInstanceBase class.
        /// </summary>
        /// <param name="typeFullName">The fully qualified name of the .NET <see cref="Type"/> of the metric.</param>
        /// <param name="category">The category of the metric.</param>
        /// <param name="name">The name of the metric.</param>
        /// <param name="description">A description of the metric, explaining what it measures and/or represents.</param>
        /// <param name="eventTime">The timestamp when the metric occurred.</param>
        public MetricInstanceBase(String typeFullName, String category, String name, String description, DateTime eventTime)
            : this(typeFullName, name, description, eventTime)
        {
            Category = category;
        }
    }
}
