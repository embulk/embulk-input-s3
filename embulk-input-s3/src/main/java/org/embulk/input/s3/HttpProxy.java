package org.embulk.input.s3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

/**
 * HttpProxy is config unit for Input/Output plugins' configs.
 *
 * TODO
 * This unit will be moved to embulk/embulk-plugin-units.git.
 */
public class HttpProxy
{
    private final String host;
    private final Optional<Integer> port;
    private final boolean https;
    private final Optional<String> user;
    private final Optional<String> password; // TODO use SecretString

    @JsonCreator
    public HttpProxy(
            @JsonProperty("host") String host,
            @JsonProperty("port") Optional<Integer> port,
            @JsonProperty("https") boolean https,
            @JsonProperty("user") Optional<String> user,
            @JsonProperty("password") Optional<String> password)
    {
        this.host = host;
        this.port = port;
        this.https = https;
        this.user = user;
        this.password = password;
    }

    public String getHost()
    {
        return host;
    }

    public Optional<Integer> getPort()
    {
        return port;
    }

    public boolean useHttps()
    {
        return https;
    }

    public Optional<String> getUser()
    {
        return user;
    }

    public Optional<String> getPassword()
    {
        return password;
    }
}
