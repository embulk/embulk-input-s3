package org.embulk.input.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.google.common.base.Optional;

import java.util.HashMap;


public enum AWSCredentialProviders {

    SYSTEM_PROPERTY("system") {

        @Override
        public AWSCredentialsProvider createProvider() {
            return new SystemPropertiesCredentialsProvider();
        }
    },
    ENV("env") {

        @Override
        public AWSCredentialsProvider createProvider() {
            return new EnvironmentVariableCredentialsProvider();
        }
    },
    INSTANCE_PROFILE("instance") {
        @Override
        public AWSCredentialsProvider createProvider() {
            return new InstanceProfileCredentialsProvider();
        }
    }, PROFILE("profile") {
        @Override
        public AWSCredentialsProvider createProvider() {
            return new ProfileCredentialsProvider();
        }
    },
    DEFAULT("default"){
        @Override
        public AWSCredentialsProvider createProvider() {
            return new AWSCredentialsProviderChain(
                    new EnvironmentVariableCredentialsProvider(),
                    new SystemPropertiesCredentialsProvider(),
                    new ProfileCredentialsProvider(),
                    new InstanceProfileCredentialsProvider()) {

                public AWSCredentials getCredentials() {
                    try {
                        return super.getCredentials();
                    } catch (AmazonClientException ace) {}
                    return null;
                }
            };
        }
    }
    ;

    private final String name;

    private static final HashMap<String, AWSCredentialProviders> NAME2ENUM = new HashMap<>();
    static {
        for(AWSCredentialProviders p: values()){
            NAME2ENUM.put(p.name, p);
        }
    }

    AWSCredentialProviders(String name){
        this.name = name;
    }

    public abstract AWSCredentialsProvider createProvider();

    public static Optional<AWSCredentialProviders> fromName(String name){
        return Optional.fromNullable(NAME2ENUM.get(name.toLowerCase()));
    }


}
