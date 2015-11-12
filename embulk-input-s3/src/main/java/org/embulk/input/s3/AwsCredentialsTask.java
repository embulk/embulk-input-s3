package org.embulk.input.s3;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.spi.unit.LocalFile;

public interface AwsCredentialsTask
{
    @Config("auth_method")
    @ConfigDefault("\"basic\"")
    String getAuthMethod();
    void setAuthMethod(String method);

    @Config("access_key_id")
    @ConfigDefault("null")
    Optional<String> getAccessKeyId();
    void setAccessKeyId(Optional<String> value);

    @Config("secret_access_key")
    @ConfigDefault("null")
    Optional<String> getSecretAccessKey();
    void setSecretAccessKey(Optional<String> value);

    @Config("session_token")
    @ConfigDefault("null")
    Optional<String> getSessionToken();
    void setSessionToken(Optional<String> value);

    @Config("profile_file")
    @ConfigDefault("null")
    Optional<LocalFile> getProfileFile();
    void setProfileFile(Optional<LocalFile> value);

    @Config("profile_name")
    @ConfigDefault("null")
    Optional<String> getProfileName();
    void setProfileName(Optional<String> value);
}
