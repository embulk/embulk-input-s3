/*
 * Copyright 2017 The Embulk project
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

import java.util.Optional;

/**
 * HttpProxy is config unit for Input/Output plugins' configs.
 *
 * TODO: This unit will be moved to embulk/embulk-plugin-units.git.
 * TODO: Consider using @JsonProperty(defaultValue=...) in Jackson 2.6+.
 */
public interface HttpProxy
    extends Task
{
    @Config("host")
    String getHost();

    @Config("port")
    @ConfigDefault("null")
    Optional<Integer> getPort();

    @Config("https")
    @ConfigDefault("true")
    boolean getHttps();

    @Config("user")
    @ConfigDefault("null")
    Optional<String> getUser();

    @Config("password")
    @ConfigDefault("null")
    Optional<String> getPassword();
}
