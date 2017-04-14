package org.embulk.input.s3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.Map;

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
    private final boolean useSsl;
    private final Optional<String> user;
    private final Optional<String> password; // TODO use SecretString

    @JsonCreator
    public HttpProxy(
            @JsonProperty("host") String host,
            @JsonProperty("port") Optional<Integer> port,
            @JsonProperty("use_ssl") boolean useSsl,
            @JsonProperty("user") Optional<String> user,
            @JsonProperty("password") Optional<String> password)
    {
        this.host = host;
        this.port = port;
        this.useSsl = useSsl;
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

    public boolean useSsl()
    {
        return useSsl;
    }

    public Optional<String> getUser()
    {
        return user;
    }

    public Optional<String> getPassword()
    {
        return password;
    }

    /**
     * Returns http proxy settings from environment variables. It searches environment variables
     * "HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy" and extracts http proxy settings
     * in the order.
     */
    public static Optional<HttpProxy> createHttpProxyFromEnv()
    {
        return createHttpProxyFromEnv("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy");
    }

    /**
     * Returns http proxy settings from environment variables of given names. It searches specified
     * environment variables and extracts http proxy setting in the order.
     */
    public static Optional<HttpProxy> createHttpProxyFromEnv(String... envNames)
    {
        Map<String, String> env = System.getenv();

        String envVar = null;
        for (String envName : envNames) {
            envVar = env.getOrDefault(envName, "").trim();
            if (!envVar.isEmpty()) {
                break;
            }
        }

        if (envVar == null) {
            return Optional.absent();
        }
        else {
            try {
                return Optional.of(parseHttpProxyFromUrl(envVar));
            }
            catch (URISyntaxException | UnsupportedEncodingException e) {
                return Optional.absent();
            }
        }
    }

    @VisibleForTesting
    static HttpProxy parseHttpProxyFromUrl(String httpProxyString)
            throws URISyntaxException, UnsupportedEncodingException
    {
        URI uri = new URI(httpProxyString);

        String host = uri.getHost();
        Optional<Integer> port = uri.getPort() != -1 ? Optional.of(uri.getPort()) : Optional.<Integer>absent();
        boolean useSsl = "https".equals(uri.getScheme());

        Optional<String> user = Optional.absent();
        Optional<String> password = Optional.absent();
        if (uri.getRawUserInfo() != null) {
            String rawUserInfo = uri.getRawUserInfo();
            int colonIndex = rawUserInfo.indexOf(':');
            if (colonIndex == -1) {
                user = Optional.of(URLDecoder.decode(rawUserInfo, "UTF-8"));
            }
            else {
                user = Optional.of(rawUserInfo.substring(0, colonIndex));
                password = Optional.of(rawUserInfo.substring(colonIndex + 1, rawUserInfo.length()));
            }
        }

        return new HttpProxy(host, port, useSsl, user, password);
    }
}
