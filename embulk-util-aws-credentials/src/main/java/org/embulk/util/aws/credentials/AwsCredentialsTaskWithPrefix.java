package org.embulk.util.aws.credentials;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.spi.unit.LocalFile;

import java.util.Optional;

public interface AwsCredentialsTaskWithPrefix
    extends AwsCredentialsConfig
{
    @Override
    @Config("aws_auth_method")
    @ConfigDefault("\"basic\"")
    String getAuthMethod();

    @Override
    @Config("aws_access_key_id")
    @ConfigDefault("null")
    Optional<String> getAccessKeyId();

    @Override
    @Config("aws_secret_access_key")
    @ConfigDefault("null")
    Optional<String> getSecretAccessKey();

    @Override
    @Config("aws_session_token")
    @ConfigDefault("null")
    Optional<String> getSessionToken();

    @Override
    @Config("aws_profile_file")
    @ConfigDefault("null")
    Optional<LocalFile> getProfileFile();

    @Override
    @Config("aws_profile_name")
    @ConfigDefault("null")
    Optional<String> getProfileName();
}
