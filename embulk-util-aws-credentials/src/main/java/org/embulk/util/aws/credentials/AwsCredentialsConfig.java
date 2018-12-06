package org.embulk.util.aws.credentials;

import org.embulk.spi.unit.LocalFile;

import java.util.Optional;

public interface AwsCredentialsConfig
{
    String getAuthMethod();

    void setAuthMethod(String method);

    Optional<String> getAccessKeyId();

    void setAccessKeyId(Optional<String> value);

    Optional<String> getSecretAccessKey();

    void setSecretAccessKey(Optional<String> value);

    Optional<String> getSessionToken();

    void setSessionToken(Optional<String> value);

    Optional<LocalFile> getProfileFile();

    void setProfileFile(Optional<LocalFile> value);

    Optional<String> getProfileName();

    void setProfileName(Optional<String> value);
}
