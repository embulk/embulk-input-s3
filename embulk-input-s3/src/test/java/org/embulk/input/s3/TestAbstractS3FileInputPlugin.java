package org.embulk.input.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.google.common.base.Optional;
import org.embulk.config.ConfigException;
import org.junit.Test;


import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestAbstractS3FileInputPlugin extends AbstractS3FileInputPlugin{

    @Override
    protected Class<? extends PluginTask> getTaskClass() {
        return AbstractS3FileInputPlugin.PluginTask.class;
    }

    @Test
    public void testGetCredentialsProviderWithProviderSpecified(){
        PluginTask task = mock(PluginTask.class);
        when(task.getAccessKeyId()).thenReturn(Optional.absent());
        when(task.getSecretAccessKey()).thenReturn(Optional.absent());
        when(task.getCredentialProvider()).thenReturn(Optional.of("env"));
        assertThat(getCredentialsProvider(task) instanceof EnvironmentVariableCredentialsProvider, equalTo(true));
    }

    @Test
    public void testGetCredentialsProviderWithProviderFallbackToDefault(){
        PluginTask task = mock(PluginTask.class);
        when(task.getAccessKeyId()).thenReturn(Optional.of("KEY"));
        when(task.getSecretAccessKey()).thenReturn(Optional.of("KEY"));
        when(task.getCredentialProvider()).thenReturn(Optional.absent());
        assertThat(getCredentialsProvider(task) instanceof StaticCredentialsProvider, equalTo(true));
    }

    @Test
    public void testGetCredentialsProviderWithProviderFallbackToDefaultByInvalidProviderName(){
        PluginTask task = mock(PluginTask.class);
        when(task.getAccessKeyId()).thenReturn(Optional.of("KEY"));
        when(task.getSecretAccessKey()).thenReturn(Optional.of("KEY"));
        when(task.getCredentialProvider()).thenReturn(Optional.of("static"));
        assertThat(getCredentialsProvider(task) instanceof StaticCredentialsProvider, equalTo(true));
    }

    @Test(expected = ConfigException.class)
    public void testGetCredentialsProviderThrowsNoAccessKeyWithoutProviderName(){
        PluginTask task = mock(PluginTask.class);
        when(task.getAccessKeyId()).thenReturn(Optional.absent());
        when(task.getSecretAccessKey()).thenReturn(Optional.of("KEY"));
        when(task.getCredentialProvider()).thenReturn(Optional.absent());
        getCredentialsProvider(task);
    }

    @Test(expected = ConfigException.class)
    public void testGetCredentialsProviderThrowsNoAccessKeyWithoutSecretKeyName(){
        PluginTask task = mock(PluginTask.class);
        when(task.getSecretAccessKey()).thenReturn(Optional.absent());
        when(task.getAccessKeyId()).thenReturn(Optional.of("KEY"));
        when(task.getCredentialProvider()).thenReturn(Optional.absent());
        getCredentialsProvider(task);
    }


}