package org.embulk.util.aws.credentials;

import com.google.common.base.Optional;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSSessionCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import org.embulk.config.ConfigException;
import org.embulk.spi.Exec;
import org.embulk.spi.unit.LocalFile;
import org.slf4j.Logger;

public abstract class AwsCredentials
{
    private AwsCredentials() { }

    public static AWSCredentialsProvider getAWSCredentialsProvider(AwsCredentialsTaskWithPrefix task)
    {
        return getAWSCredentialsProvider("aws_", task);
    }

    public static AWSCredentialsProvider getAWSCredentialsProvider(AwsCredentialsTask task)
    {
        return getAWSCredentialsProvider("", task);
    }

    private static AWSCredentialsProvider getAWSCredentialsProvider(String prefix, AwsCredentialsConfig task)
    {
        String authMethodOption = prefix + "auth_method";
        String sessionTokenOption = prefix + "session_token";
        String profileFileOption = prefix + "profile_file";
        String profileNameOption = prefix + "profile_name";
        String accessKeyIdOption = prefix + "access_key_id";
        String secretAccessKeyOption = prefix + "secret_access_key";

        switch (task.getAuthMethod()) {
        case "basic":
            // for backward compatibility
            if (!task.getAccessKeyId().isPresent() && !task.getAccessKeyId().isPresent()) {
                final Logger log = Exec.getLogger(AwsCredentials.class);
                log.warn("Both '{}' and '{}' are not set. Assuming that '{}: anonymous' option is set.",
                        accessKeyIdOption, secretAccessKeyOption, authMethodOption);
                log.warn("If you intentionally use anonymous authentication, please set 'auth_method: anonymous' option.");
                log.warn("This behavior will be removed in a future release.");
                reject(task.getSessionToken(), sessionTokenOption);
                reject(task.getProfileFile(), profileFileOption);
                reject(task.getProfileName(), profileNameOption);
                return new AWSCredentialsProvider() {
                    public AWSCredentials getCredentials()
                    {
                        return new AnonymousAWSCredentials();
                    }

                    public void refresh() { }
                };
            }
            else {
                String accessKeyId = require(task.getAccessKeyId(), "'access_key_id', 'secret_access_key'");
                String secretAccessKey = require(task.getSecretAccessKey(), "'secret_access_key'");
                reject(task.getSessionToken(), sessionTokenOption);
                reject(task.getProfileFile(), profileFileOption);
                reject(task.getProfileName(), profileNameOption);
                final BasicAWSCredentials creds = new BasicAWSCredentials(accessKeyId, secretAccessKey);
                return new AWSCredentialsProvider() {
                    public AWSCredentials getCredentials()
                    {
                        return creds;
                    }

                    public void refresh() { }
                };
            }

        case "env":
            reject(task.getAccessKeyId(), accessKeyIdOption);
            reject(task.getSecretAccessKey(), secretAccessKeyOption);
            reject(task.getSessionToken(), sessionTokenOption);
            reject(task.getProfileFile(), profileFileOption);
            reject(task.getProfileName(), profileNameOption);
            return overwriteBasicCredentials(task, new EnvironmentVariableCredentialsProvider().getCredentials());

        case "instance":
            reject(task.getAccessKeyId(), accessKeyIdOption);
            reject(task.getSecretAccessKey(), secretAccessKeyOption);
            reject(task.getSessionToken(), sessionTokenOption);
            reject(task.getProfileFile(), profileFileOption);
            reject(task.getProfileName(), profileNameOption);
            return new InstanceProfileCredentialsProvider();

        case "profile":
            {
                reject(task.getAccessKeyId(), accessKeyIdOption);
                reject(task.getSecretAccessKey(), secretAccessKeyOption);
                reject(task.getSessionToken(), sessionTokenOption);

                String profileName = task.getProfileName().or("default");
                ProfileCredentialsProvider provider;
                if (task.getProfileFile().isPresent()) {
                    ProfilesConfigFile file = new ProfilesConfigFile(task.getProfileFile().get().getFile());
                    provider = new ProfileCredentialsProvider(file, profileName);
                }
                else {
                    provider = new ProfileCredentialsProvider(profileName);
                }
                task.setProfileName(Optional.<String>absent());
                task.setProfileFile(Optional.<LocalFile>absent());

                return overwriteBasicCredentials(task, provider.getCredentials());
            }

        case "properties":
            reject(task.getAccessKeyId(), accessKeyIdOption);
            reject(task.getSecretAccessKey(), secretAccessKeyOption);
            reject(task.getSessionToken(), sessionTokenOption);
            reject(task.getProfileFile(), profileFileOption);
            reject(task.getProfileName(), profileNameOption);
            return overwriteBasicCredentials(task, new SystemPropertiesCredentialsProvider().getCredentials());

        case "anonymous":
            reject(task.getAccessKeyId(), accessKeyIdOption);
            reject(task.getSecretAccessKey(), secretAccessKeyOption);
            reject(task.getSessionToken(), sessionTokenOption);
            reject(task.getProfileFile(), profileFileOption);
            reject(task.getProfileName(), profileNameOption);
            return new AWSCredentialsProvider() {
                public AWSCredentials getCredentials()
                {
                    return new AnonymousAWSCredentials();
                }

                public void refresh() { }
            };

        case "session":
            {
                String accessKeyId = require(task.getAccessKeyId(),
                        "'" + accessKeyIdOption +"', '" + secretAccessKeyOption + "', '" + sessionTokenOption + "'");
                String secretAccessKey = require(task.getSecretAccessKey(),
                        "'" + secretAccessKeyOption + "', '" + sessionTokenOption + "'");
                String sessionToken = require(task.getSessionToken(),
                        "'" + sessionTokenOption + "'");
                reject(task.getProfileFile(), profileFileOption);
                reject(task.getProfileName(), profileNameOption);
                final AWSSessionCredentials creds = new BasicSessionCredentials(accessKeyId, secretAccessKey, sessionToken);
                return new AWSSessionCredentialsProvider() {
                    public AWSSessionCredentials getCredentials()
                    {
                        return creds;
                    }

                    public void refresh() { }
                };
            }

        case "default":
            {
                return new DefaultAWSCredentialsProviderChain();
            }

        default:
            throw new ConfigException(String.format("Unknown auth_method '%s'. Supported methods are basic, instance, profile, properties, anonymous, session and default.",
                        task.getAuthMethod()));
        }
    }

    private static AWSCredentialsProvider overwriteBasicCredentials(AwsCredentialsConfig task, final AWSCredentials creds)
    {
        task.setAuthMethod("basic");
        task.setAccessKeyId(Optional.of(creds.getAWSAccessKeyId()));
        task.setSecretAccessKey(Optional.of(creds.getAWSSecretKey()));
        return new AWSCredentialsProvider() {
            public AWSCredentials getCredentials()
            {
                return creds;
            }

            public void refresh() { }
        };
    }

    private static <T> T require(Optional<T> value, String message)
    {
        if (value.isPresent()) {
            return value.get();
        }
        else {
            throw new ConfigException("Required option is not set: " + message);
        }
    }

    private static <T> void reject(Optional<T> value, String message)
    {
        if (value.isPresent()) {
            throw new ConfigException("Invalid option is set: " + message);
        }
    }
}
