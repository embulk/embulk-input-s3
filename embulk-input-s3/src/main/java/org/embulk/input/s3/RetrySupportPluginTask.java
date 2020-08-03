/*
 * Copyright 2018 The Embulk project
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

package org.embulk.input.s3;

import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;

public interface RetrySupportPluginTask extends Task
{
    @Config("maximum_retries")
    @ConfigDefault("7")
    int getMaximumRetries();

    @Config("initial_retry_interval_millis")
    @ConfigDefault("2000")
    int getInitialRetryIntervalMillis();

    @Config("maximum_retry_interval_millis")
    @ConfigDefault("480000")
    int getMaximumRetryIntervalMillis();
}
