package org.embulk.input.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import org.embulk.config.ConfigException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;

public class AwsClientConfigurations
{
    public static ClientConfiguration getClientConfiguration(AwsClientConfigurationsTask task)
    {
        ClientConfiguration c = new ClientConfiguration();

        //if (task.getClientExecutionTimeout().isPresent()) {
        //    c.setClientExecutionTimeout(task.getClientExecutionTimeout().get());
        //}

        if (task.getConnectionMaxIdle().isPresent()) {
            c.setConnectionMaxIdleMillis(task.getConnectionMaxIdle().get().getMillis());
        }

        if (task.getConnectionTimeout().isPresent()) {
            c.setConnectionTimeout((int) task.getConnectionTimeout().get().getMillis());
        }

        if (task.getConnectionTTL().isPresent()) {
            c.setConnectionTTL(task.getConnectionTTL().get().getMillis());
        }

        //if (task.getDnsResolver().isPresent()) {
        //    c.setDnsResolver(task.getDnsResolver().get());
        //}

        if (task.getLocalAddress().isPresent()) {
            try {
                InetAddress addr = InetAddress.getByName(task.getLocalAddress().get());
                c.setLocalAddress(addr);
            }
            catch (UnknownHostException e) {
                throw new ConfigException("Invalid local_address", e);
            }
        }

        if (task.getMaxConnections().isPresent()) {
            c.setMaxConnections(task.getMaxConnections().get());
        }

        if (task.getMaxErrorRetry().isPresent()) {
            c.setMaxErrorRetry(task.getMaxErrorRetry().get());
        }

        if (task.getPreemptiveBasicProxyAuth().isPresent()) {
            c.setPreemptiveBasicProxyAuth(task.getPreemptiveBasicProxyAuth().get());
        }

        if (task.getProtocol().isPresent()) {
            c.setProtocol(task.getProtocol().get());
        }

        if (task.getProxyDomain().isPresent()) {
            c.setProxyDomain(task.getProxyDomain().get());
        }

        if (task.getProxyHost().isPresent()) {
            c.setProxyHost(task.getProxyHost().get());
        }

        if (task.getProxyPassword().isPresent()) {
            c.setProxyPassword(task.getProxyPassword().get());
        }

        if (task.getProxyPort().isPresent()) {
            c.setProxyPort(task.getProxyPort().get());
        }

        if (task.getProxyUsername().isPresent()) {
            c.setProxyUsername(task.getProxyUsername().get());
        }

        if (task.getProxyWorkstation().isPresent()) {
            c.setProxyWorkstation(task.getProxyWorkstation().get());
        }

        if (task.getRequestTimeout().isPresent()) {
            c.setRequestTimeout((int) task.getRequestTimeout().get().getMillis());
        }

        if (task.getResponseMetadataCacheSize().isPresent()) {
            c.setResponseMetadataCacheSize(task.getResponseMetadataCacheSize().get().getBytesInt());
        }

        //if (task.getRetryPolicy().isPresent()) {
        //    c.setRetryPolicy(task.getRetryPolicy().get());
        //}

        if (task.getSecureRandom().isPresent()) {
            try {
                AwsClientConfigurationsTask.SecureRandomTask secureRandomTask = task.getSecureRandom().get();
                SecureRandom rand =
                    secureRandomTask.getProvider().isPresent()
                    ?  SecureRandom.getInstance(secureRandomTask.getAlgorithm(), secureRandomTask.getProvider().get())
                    : SecureRandom.getInstance(secureRandomTask.getAlgorithm());
                c.setSecureRandom(rand);
            }
            catch (NoSuchAlgorithmException | NoSuchProviderException e) {
                throw new ConfigException("Invalid secure_random", e);
            }
        }

        if (task.getSignerOverride().isPresent()) {
            c.setSignerOverride(task.getSignerOverride().get());
        }

        if (task.getSocketTimeout().isPresent()) {
            c.setSocketTimeout(task.getSocketTimeout().get().getMillisInt());
        }

        if (task.getUseExpectContinue().isPresent()) {
            c.setUseExpectContinue(task.getUseExpectContinue().get());
        }

        if (task.getUseGzip().isPresent()) {
            c.setUseGzip(task.getUseGzip().get());
        }

        if (task.getUserAgent().isPresent()) {
            c.setUserAgent(task.getUserAgent().get());
        }

        if (task.getUseReaper().isPresent()) {
            c.setUseReaper(task.getUseReaper().get());
        }

        if (task.getUseTcpKeepAlive().isPresent()) {
            c.setUseTcpKeepAlive(task.getUseTcpKeepAlive().get());
        }

        //if (task.getUseThrottleRetries().isPresent()) {
        //    c.setUseThrottleRetries(task.getUseThrottleRetries().get());
        //}

        if (task.getSocketSendBufferSizeHint().isPresent() && task.getSocketReceiveBufferSizeHint().isPresent()) {
            c.setSocketBufferSizeHints(task.getSocketSendBufferSizeHint().get().getBytesInt(), task.getSocketReceiveBufferSizeHint().get().getBytesInt());
        }
        else if (task.getSocketSendBufferSizeHint().isPresent() || task.getSocketReceiveBufferSizeHint().isPresent()) {
            throw new ConfigException("socket_send_buffer_size_hint and socket_receive_buffer_size_hint must set together");
        }

        return c;
    }
}
