package org.embulk.input.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.Task;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;

/**
 * Created by takahiro.nakayama on 3/25/16.
 * This class is utility class for ClientConfiguration
 */
public abstract class ClientConfigurationConfigurable
{
    public interface Task
        extends org.embulk.config.Task
    {
        @Config("protocol")
        @ConfigDefault("null")
        Optional<Protocol> getProtocol();

        @Config("max_connections")
        @ConfigDefault("null") // default: 50
        Optional<Integer> getMaxConnections();

        @Config("user_agent")
        @ConfigDefault("null")
        Optional<String> getUserAgent();

        @Config("local_address")
        @ConfigDefault("null")
        Optional<String> getLocalAddress();

        @Config("proxy_host")
        @ConfigDefault("null")
        Optional<String> getProxyHost();

        @Config("proxy_port")
        @ConfigDefault("null")
        Optional<Integer> getProxyPort();

        @Config("proxy_username")
        @ConfigDefault("null")
        Optional<String> getProxyUsername();

        @Config("proxy_password")
        @ConfigDefault("null")
        Optional<String> getProxyPassword();

        @Config("proxy_domain")
        @ConfigDefault("null")
        Optional<String> getProxyDomain();

        @Config("proxy_workstation")
        @ConfigDefault("null")
        Optional<String> getProxyWorkstation();

        // NOTE: RetryPolicy is a interface
        // @Config("retry_policy")
        // @ConfigDefault("null")
        // Optional<RetryPolicy> getRetryPolicy();

        @Config("max_error_retry")
        @ConfigDefault("null") // default: 3
        Optional<Integer> getMaxErrorRetry();

        @Config("socket_timeout")
        @ConfigDefault("null") // default: 8*60*1000
        Optional<Integer> getSocketTimeout();

        @Config("connection_timeout")
        @ConfigDefault("null")
        Optional<Integer> getConnectionTimeout();

        @Config("request_timeout")
        @ConfigDefault("null")
        Optional<Integer> getRequestTimeout();

        // NOTE: Can use `client_execution_timeout` from v1.10.65
        // @Config("client_execution_timeout")
        // @ConfigDefault("null")
        // Optional<Integer> getClientExecutionTimeout();

        @Config("use_reaper")
        @ConfigDefault("null")
        Optional<Boolean> getUseReaper();

        // NOTE: Can use `use_throttle_retries` from v1.10.65
        // @Config("use_throttle_retries")
        // @ConfigDefault("null")
        // Optional<Boolean> getUseThrottleRetries();

        @Config("use_gzip")
        @ConfigDefault("null")
        Optional<Boolean> getUseGzip();

        @Config("socket_send_buffer_size_hints") // used by SocketBufferSizeHints
        @ConfigDefault("null")
        Optional<Integer> getSocketSendBufferSizeHint();

        @Config("socket_receive_buffer_size_hints") // used by SocketBufferSizeHints
        @ConfigDefault("null")
        Optional<Integer> getSocketReceiveBufferSizeHints();

        @Config("signer_override")
        @ConfigDefault("null")
        Optional<String> getSignerOverride();

        @Config("preemptive_basic_proxy_auth")
        @ConfigDefault("null")
        Optional<Boolean> getPreemptiveBasicProxyAuth();

        @Config("connection_ttl")
        @ConfigDefault("null")
        Optional<Long> getConnectionTTL();

        @Config("connection_max_idle_millis")
        @ConfigDefault("null")
        Optional<Long> getConnectionMaxIdleMillis();

        @Config("use_tcp_keep_alive")
        @ConfigDefault("null")
        Optional<Boolean> getUseTcpKeepAlive();

        // NOTE: DnsResolver is a interface
        // @Config("dns_resolver")
        // @ConfigDefault("null")
        // Optional<DnsResolver> getDnsResolver();

        @Config("response_metadata_cache_size")
        @ConfigDefault("null")
        Optional<Integer> getResponseMetadataCacheSize();

        @Config("secure_random")
        @ConfigDefault("null")
        Optional<SecureRandomTask> getSecureRandom();

        @Config("use_expect_continue")
        @ConfigDefault("null")
        Optional<Boolean> getUseExpectContinue();
    }

    public interface SecureRandomTask
        extends org.embulk.config.Task
    {
        @Config("algorithm")
        String getAlgorithm();

        @Config("provider")
        @ConfigDefault("null")
        Optional<String> getProvider();
    }

    protected ClientConfigurationConfigurable()
    {
    }

    // For backward compatibility
    public static final int DEFAULT_MAX_CONNECTIONS = 50; // SDK default: 50
    public static final int DEFAULT_MAX_ERROR_RETRY = 3;  // SDK default: 3
    public static final int DEFAULT_SOCKET_TIMEOUT = 8*60*1000; // SDK default: 50*1000

    public static ClientConfiguration getClientConfiguration(Task task)
    {
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        setProtocolOrDoNothing(clientConfiguration, task.getProtocol());
        setMaxConnectionsOrDoNothing(clientConfiguration, task.getMaxConnections());
        setUserAgentOrDoNothing(clientConfiguration, task.getUserAgent());

        setLocalAddressOrDoNothing(clientConfiguration, task.getLocalAddress());
        setProxyHostOrDoNothing(clientConfiguration, task.getProxyHost());
        setProxyPortOrDoNothing(clientConfiguration, task.getProxyPort());

        setProxyUsernameOrDoNothing(clientConfiguration, task.getProxyUsername());
        setProxyPasswordOrDoNothing(clientConfiguration, task.getProxyPassword());
        setProxyDomainOrDoNothing(clientConfiguration, task.getProxyDomain());

        setProxyWorkstationOrDoNothing(clientConfiguration, task.getProxyWorkstation());
        setMaxErrorRetryOrDoNothing(clientConfiguration, task.getMaxErrorRetry());
        setSocketTimeoutOrDoNothing(clientConfiguration, task.getSocketTimeout());

        setConnectionTimeoutOrDoNothing(clientConfiguration, task.getConnectionTimeout());
        setRequestTimeoutOrDoNothing(clientConfiguration, task.getRequestTimeout());
        setUseReaperOrDoNothing(clientConfiguration, task.getUseReaper());

        setUseGzipOrDoNothing(clientConfiguration, task.getUseGzip());
        setSocketBufferSizeHintsOrDoNothing(clientConfiguration, task.getSocketSendBufferSizeHint(),
                task.getSocketReceiveBufferSizeHints());

        setSignerOverrideOrDoNothing(clientConfiguration, task.getSignerOverride());
        setPreemptiveBasicProxyAuthOrDoNothing(clientConfiguration, task.getPreemptiveBasicProxyAuth());
        setConnectionTTLOrDoNothing(clientConfiguration, task.getConnectionTTL());

        setConnectionMaxIdleMillisOrDoNothing(clientConfiguration, task.getConnectionMaxIdleMillis());
        setUseTcpKeepAliveOrDoNothing(clientConfiguration, task.getUseTcpKeepAlive());
        setResponseMetadataCacheSizeOrDoNothing(clientConfiguration, task.getResponseMetadataCacheSize());

        setSecureRandomOrDoNothing(clientConfiguration, task.getSecureRandom());
        setUseExpectContinueOrDoNothing(clientConfiguration, task.getUseExpectContinue());

        setRetryPolicy(clientConfiguration);
        setDnsResolver(clientConfiguration);

        return clientConfiguration;
    }

    protected static void setProtocolOrDoNothing(ClientConfiguration clientConfiguration, Optional<Protocol> protocol)
    {
        if (protocol.isPresent()) {
            clientConfiguration.setProtocol(protocol.get());
        }
    }

    protected static void setMaxConnectionsOrDoNothing(ClientConfiguration clientConfiguration, Optional<Integer> maxConnections)
    {
        clientConfiguration.setMaxConnections(maxConnections.or(DEFAULT_MAX_CONNECTIONS));
    }

    protected static void setUserAgentOrDoNothing(ClientConfiguration clientConfiguration, Optional<String> userAgent)
    {
        if (userAgent.isPresent()) {
            clientConfiguration.setUserAgent(userAgent.get());
        }
    }

    protected static void setLocalAddressOrDoNothing(ClientConfiguration clientConfiguration, Optional<String> localAddress)
    {
        if (localAddress.isPresent()) {
            InetAddress inetAddress = getInetAddress(localAddress.get());
            clientConfiguration.setLocalAddress(inetAddress);
        }
    }

    protected static void setProxyHostOrDoNothing(ClientConfiguration clientConfiguration, Optional<String> proxyHost)
    {
        if (proxyHost.isPresent()) {
            clientConfiguration.setProxyHost(proxyHost.get());
        }
    }

    protected static void setProxyPortOrDoNothing(ClientConfiguration clientConfiguration, Optional<Integer> proxyPort)
    {
        if (proxyPort.isPresent()) {
            clientConfiguration.setProxyPort(proxyPort.get());
        }
    }

    protected static void setProxyUsernameOrDoNothing(ClientConfiguration clientConfiguration, Optional<String> proxyUsername)
    {
        if (proxyUsername.isPresent()) {
            clientConfiguration.setProxyUsername(proxyUsername.get());
        }
    }

    protected static void setProxyPasswordOrDoNothing(ClientConfiguration clientConfiguration, Optional<String> proxyPassword)
    {
        if (proxyPassword.isPresent()) {
            clientConfiguration.setProxyPassword(proxyPassword.get());
        }
    }

    protected static void setProxyDomainOrDoNothing(ClientConfiguration clientConfiguration, Optional<String> proxyDomain)
    {
        if (proxyDomain.isPresent()) {
            clientConfiguration.setProxyDomain(proxyDomain.get());
        }
    }

    protected static void setProxyWorkstationOrDoNothing(ClientConfiguration clientConfiguration, Optional<String> proxyWorkstation)
    {
        if (proxyWorkstation.isPresent()) {
            clientConfiguration.setProxyWorkstation(proxyWorkstation.get());
        }
    }

    protected static void setMaxErrorRetryOrDoNothing(ClientConfiguration clientConfiguration, Optional<Integer> maxErrorRetry)
    {
        clientConfiguration.setMaxErrorRetry(maxErrorRetry.or(DEFAULT_MAX_ERROR_RETRY));
    }

    protected static void setSocketTimeoutOrDoNothing(ClientConfiguration clientConfiguration, Optional<Integer> socketTimeout)
    {
        clientConfiguration.setSocketTimeout(socketTimeout.or(DEFAULT_SOCKET_TIMEOUT));
    }

    protected static void setConnectionTimeoutOrDoNothing(ClientConfiguration clientConfiguration, Optional<Integer> connectionTimeout)
    {
        if (connectionTimeout.isPresent()) {
            clientConfiguration.setConnectionTimeout(connectionTimeout.get());
        }
    }

    protected static void setRequestTimeoutOrDoNothing(ClientConfiguration clientConfiguration, Optional<Integer> requestTimeout)
    {
        if (requestTimeout.isPresent()) {
            clientConfiguration.setRequestTimeout(requestTimeout.get());
        }
    }

    protected static void setUseReaperOrDoNothing(ClientConfiguration clientConfiguration, Optional<Boolean> useReaper)
    {
        if (useReaper.isPresent()) {
            clientConfiguration.setUseReaper(useReaper.get());
        }
    }

    protected static void setUseGzipOrDoNothing(ClientConfiguration clientConfiguration, Optional<Boolean> useGzip)
    {
        if (useGzip.isPresent()) {
            clientConfiguration.setUseGzip(useGzip.get());
        }
    }

    protected static void setSocketBufferSizeHintsOrDoNothing(ClientConfiguration clientConfiguration,
            Optional<Integer> socketSendBufferSizeHint, Optional<Integer> socketReceiveBufferSizeHint)
    {
        if (socketSendBufferSizeHint.isPresent() && socketReceiveBufferSizeHint.isPresent()) {
            clientConfiguration.setSocketBufferSizeHints(socketSendBufferSizeHint.get(), socketReceiveBufferSizeHint.get());
        }
    }

    protected static void setSignerOverrideOrDoNothing(ClientConfiguration clientConfiguration, Optional<String> value)
    {
        if (value.isPresent()) {
            clientConfiguration.setSignerOverride(value.get());
        }
    }

    protected static void setPreemptiveBasicProxyAuthOrDoNothing(ClientConfiguration clientConfiguration, Optional<Boolean> preemptiveBasicProxyAuth)
    {
        if (preemptiveBasicProxyAuth.isPresent()) {
            clientConfiguration.setPreemptiveBasicProxyAuth(preemptiveBasicProxyAuth.get());
        }
    }

    protected static void setConnectionTTLOrDoNothing(ClientConfiguration clientConfiguration, Optional<Long> connectionTTL)
    {
        if (connectionTTL.isPresent()) {
            clientConfiguration.setConnectionTTL(connectionTTL.get());
        }
    }

    protected static void setConnectionMaxIdleMillisOrDoNothing(ClientConfiguration clientConfiguration, Optional<Long> connectionMaxIdleMillis)
    {
        if (connectionMaxIdleMillis.isPresent()) {
            clientConfiguration.setConnectionMaxIdleMillis(connectionMaxIdleMillis.get());
        }
    }

    protected static void setUseTcpKeepAliveOrDoNothing(ClientConfiguration clientConfiguration, Optional<Boolean> useTcpKeepAlive)
    {
        if (useTcpKeepAlive.isPresent()) {
            clientConfiguration.setUseTcpKeepAlive(useTcpKeepAlive.get());
        }
    }

    protected static void setResponseMetadataCacheSizeOrDoNothing(ClientConfiguration clientConfiguration, Optional<Integer> responseMetadataCacheSize)
    {
        if (responseMetadataCacheSize.isPresent()) {
            clientConfiguration.setResponseMetadataCacheSize(responseMetadataCacheSize.get());
        }
    }

    protected static void setSecureRandomOrDoNothing(ClientConfiguration clientConfiguration, Optional<SecureRandomTask> secureRandomTask)
    {
        if (secureRandomTask.isPresent()) {
            SecureRandom secureRandom = getSecureRandom(secureRandomTask.get());
            clientConfiguration.setSecureRandom(secureRandom);
        }
    }

    protected static void setUseExpectContinueOrDoNothing(ClientConfiguration clientConfiguration, Optional<Boolean> useExpectContinue)
    {
        if (useExpectContinue.isPresent()) {
            clientConfiguration.setUseExpectContinue(useExpectContinue.get());
        }
    }

    protected static void setRetryPolicy(ClientConfiguration clientConfiguration)
    {
    }

    protected static void setDnsResolver(ClientConfiguration clientConfiguration)
    {
    }


    private static InetAddress getInetAddress(String host)
    {
        try {
            return InetAddress.getByName(host);
        }
        catch (UnknownHostException e) {
            throw new ConfigException(e);
        }
    }

    private static SecureRandom getSecureRandom(SecureRandomTask secureRandomTask)
    {
        try {
            if (secureRandomTask.getProvider().isPresent()) {
                return SecureRandom.getInstance(secureRandomTask.getAlgorithm(), secureRandomTask.getProvider().get());
            }
            else {
                return SecureRandom.getInstance(secureRandomTask.getAlgorithm());
            }
        }
        catch (NoSuchAlgorithmException | NoSuchProviderException e) {
            throw new ConfigException(e);
        }
    }
}
