package org.embulk.input.s3;

import com.amazonaws.auth.AWSCredentialsProvider;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.google.common.base.Optional;
import org.junit.Test;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

public class TestAWSCredentialProviders {

    @Test
    public void testFromName(){
        assertFromName("system", Optional.of(SystemPropertiesCredentialsProvider.class));
        assertFromName("System", Optional.of(SystemPropertiesCredentialsProvider.class));
        assertFromName("env", Optional.of(EnvironmentVariableCredentialsProvider.class));
        assertFromName("instance", Optional.of(InstanceProfileCredentialsProvider.class));
        assertFromName("profile", Optional.of(ProfileCredentialsProvider.class));
        assertFromName("", Optional.absent());
    }

    private void assertFromName(final String name, Optional<Class<? extends AWSCredentialsProvider>> klass){
        assertThat(AWSCredentialProviders.fromName(name).isPresent(), equalTo(klass.isPresent()));
        if(klass.isPresent()){
            assertThat(AWSCredentialProviders.fromName(name).get().createProvider().getClass(), equalTo(klass.get()));
        }

    }

}