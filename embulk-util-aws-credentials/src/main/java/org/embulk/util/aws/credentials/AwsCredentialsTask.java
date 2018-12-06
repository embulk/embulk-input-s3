package org.embulk.util.aws.credentials;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.spi.unit.LocalFile;

import java.util.Optional;

public interface AwsCredentialsTask
    extends AwsCredentialsConfig
{
    @Override
    @Config("auth_method")
    @ConfigDefault("\"basic\"")
    String getAuthMethod();

    @Override
    @Config("access_key_id")
    @ConfigDefault("null")
    Optional<String> getAccessKeyId();

    @Override
    @Config("secret_access_key")
    @ConfigDefault("null")
    Optional<String> getSecretAccessKey();

    @Override
    @Config("session_token")
    @ConfigDefault("null")
    Optional<String> getSessionToken();

    @Override
    @Config("profile_file")
    @ConfigDefault("null")
    Optional<LocalFile> getProfileFile();

    @Override
    @Config("profile_name")
    @ConfigDefault("null")
    Optional<String> getProfileName();
}
