package org.embulk.input.s3;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

/**
 * HttpProxy is config unit for Input/Output plugins' configs.
 *
 * TODO
 * This unit will be moved to embulk/embulk-plugin-units.git.
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

    /* TODO We can use this by jackson-core v2.6
    private final String host;
    private final Optional<Integer> port;
    private final boolean https;
    private final Optional<String> user;
    private final Optional<String> password; // TODO use SecretString

    @JsonCreator
    public HttpProxy(
            @JsonProperty("host") String host,
            @JsonProperty("port") Optional<Integer> port,
            @JsonProperty(defaultValue = "true", value = "https", required = false) boolean https,
            @JsonProperty("user") Optional<String> user,
            @JsonProperty("password") Optional<String> password)
    {
        this.host = host;
        this.port = port;
        this.https = https;
        this.user = user;
        this.password = password;
    }
    */
}
