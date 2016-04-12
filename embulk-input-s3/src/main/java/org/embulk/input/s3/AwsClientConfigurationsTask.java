package org.embulk.input.s3;

import com.google.common.base.Optional;
import com.amazonaws.Protocol;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;
import org.embulk.spi.unit.ByteSize;

public interface AwsClientConfigurationsTask
    extends Task
{
    // NOTE: Can use `client_execution_timeout` from v1.10.65
    // @Config("client_execution_timeout")
    // @ConfigDefault("null")
    // Optional<Integer> getClientExecutionTimeout();

    @Config("connection_max_idle")
    @ConfigDefault("null")
    Optional<Duration> getConnectionMaxIdle();

    @Config("connection_timeout")
    @ConfigDefault("null")  // SDK default: 50sec
    Optional<Duration> getConnectionTimeout();

    @Config("connection_ttl")
    @ConfigDefault("null")
    Optional<Duration> getConnectionTTL();

    // NOTE: DnsResolver is a interface
    // @Config("dns_resolver")
    // @ConfigDefault("null")
    // Optional<DnsResolver> getDnsResolver();

    @Config("local_address")
    @ConfigDefault("null")
    Optional<String> getLocalAddress();

    @Config("max_connections")
    @ConfigDefault("50")
    Optional<Integer> getMaxConnections();

    @Config("max_error_retry")
    @ConfigDefault("3")
    Optional<Integer> getMaxErrorRetry();

    @Config("preemptive_basic_proxy_auth")
    @ConfigDefault("null")
    Optional<Boolean> getPreemptiveBasicProxyAuth();

    @Config("protocol")
    @ConfigDefault("null")  // SDK default: HTTPS
    Optional<Protocol> getProtocol();

    @Config("proxy_domain")
    @ConfigDefault("null")
    Optional<String> getProxyDomain();

    @Config("proxy_host")
    @ConfigDefault("null")
    Optional<String> getProxyHost();

    @Config("proxy_password")
    @ConfigDefault("null")
    Optional<String> getProxyPassword();

    @Config("proxy_port")
    @ConfigDefault("null")
    Optional<Integer> getProxyPort();

    @Config("proxy_username")
    @ConfigDefault("null")
    Optional<String> getProxyUsername();

    @Config("proxy_workstation")
    @ConfigDefault("null")
    Optional<String> getProxyWorkstation();

    @Config("request_timeout")
    @ConfigDefault("null")
    Optional<Duration> getRequestTimeout();

    @Config("response_metadata_cache_size")
    @ConfigDefault("null")
    Optional<ByteSize> getResponseMetadataCacheSize();

    // NOTE: RetryPolicy is a interface
    // @Config("retry_policy")
    // @ConfigDefault("null")
    // Optional<RetryPolicy> getRetryPolicy();

    @Config("secure_random")
    @ConfigDefault("null")
    Optional<SecureRandomTask> getSecureRandom();

    public interface SecureRandomTask
        extends org.embulk.config.Task
    {
        @Config("algorithm")
        String getAlgorithm();

        @Config("provider")
        @ConfigDefault("null")
        Optional<String> getProvider();
    }

    @Config("signer_override")
    @ConfigDefault("null")
    Optional<String> getSignerOverride();

    @Config("socket_timeout")
    @ConfigDefault("\"8min\"")
    Optional<Duration> getSocketTimeout();

    @Config("use_expect_continue")
    @ConfigDefault("null")
    Optional<Boolean> getUseExpectContinue();

    @Config("use_gzip")
    @ConfigDefault("null")
    Optional<Boolean> getUseGzip();

    @Config("user_agent")
    @ConfigDefault("null")
    Optional<String> getUserAgent();

    @Config("use_reaper")
    @ConfigDefault("null")
    Optional<Boolean> getUseReaper();

    @Config("use_tcp_keep_alive")
    @ConfigDefault("null")
    Optional<Boolean> getUseTcpKeepAlive();

    // NOTE: Can use `use_throttle_retries` from v1.10.65
    // @Config("use_throttle_retries")
    // @ConfigDefault("null")
    // Optional<Boolean> getUseThrottleRetries();

    @Config("socket_send_buffer_size_hint") // used by setSocketBufferSizeHints
    @ConfigDefault("null")
    Optional<ByteSize> getSocketSendBufferSizeHint();

    @Config("socket_receive_buffer_size_hint") // used by setSocketBufferSizeHints
    @ConfigDefault("null")
    Optional<ByteSize> getSocketReceiveBufferSizeHint();
}
