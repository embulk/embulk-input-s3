package org.embulk.input.s3;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

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
    public String getHost();

    @Config("port")
    @ConfigDefault("null")
    public Optional<Integer> getPort();

    @Config("https")
    @ConfigDefault("true")
    public boolean getHttps();

    @Config("user")
    @ConfigDefault("null")
    public Optional<String> getUser();

    @Config("password")
    @ConfigDefault("null")
    public Optional<String> getPassword();
}
