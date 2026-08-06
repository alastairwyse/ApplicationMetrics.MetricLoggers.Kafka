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
using ApplicationLogging;

namespace ApplicationMetrics.MetricLoggers.Kafka
{
    /// <summary>
    /// Converts from <see cref="SyslogLevel">SyslogLevels</see> to <see cref="LogLevel">LogLevels</see>.
    /// </summary>
    internal class SyslogLevelToLogLevelConverter
    {
        /// <summary>
        /// Converts a <see cref="SyslogLevel"/> to a <see cref="LogLevel"/>.
        /// </summary>
        public LogLevel Convert(SyslogLevel inputSyslogLevel)
        {
            switch (inputSyslogLevel)
            {
                case SyslogLevel.Alert:
                    return LogLevel.Error;
                case SyslogLevel.Critical: 
                    return LogLevel.Critical;
                case SyslogLevel.Debug: 
                    return LogLevel.Debug;
                case SyslogLevel.Emergency:
                    return LogLevel.Critical;
                case SyslogLevel.Error:
                    return LogLevel.Error;
                case SyslogLevel.Info:
                    return LogLevel.Information;
                case SyslogLevel.Notice:
                    return LogLevel.Information;
                case SyslogLevel.Warning:
                    return LogLevel.Warning;
                default:
                    throw new Exception($"Encountered unhandled {nameof(SyslogLevel)} '{inputSyslogLevel}' when attempting to convert to a {nameof(LogLevel)}.");
            }
        }
    }
}
