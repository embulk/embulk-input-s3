package org.embulk.input.s3;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.embulk.EmbulkTestRuntime;
import org.embulk.spi.util.RetryExecutor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;

public class TestAbstractS3FileInputPlugin {
    private static AbstractS3FileInputPlugin dummyS3Plugin()
    {
        return new AbstractS3FileInputPlugin()
        {
            @Override
            protected Class<? extends PluginTask> getTaskClass()
            {
                return PluginTask.class;
            }
        };
    }

    private static class SomeException extends RuntimeException
    {
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private AmazonS3 client;

    @Before
    public void setUp() {
        client = mock(AmazonS3.class);
    }

    @Test
    public void addS3DirectObjectShouldRunWithoutRetry() throws RetryExecutor.RetryGiveupException, InterruptedException {
        doReturn(new ObjectMetadata()).when(client).getObjectMetadata(any(GetObjectMetadataRequest.class));
        FileList.Builder builder = new FileList.Builder().pathMatchPattern("");
        dummyS3Plugin().addS3DirectObject(builder, client, "some_bucket", "some_prefix");
    }

    @Test
    public void addS3DirectObjectShouldRetryFirstTimeWhenHavingException() throws RetryExecutor.RetryGiveupException, InterruptedException {
        doThrow(new RuntimeException()).doReturn(new ObjectMetadata())
                .when(client).getObjectMetadata(any(GetObjectMetadataRequest.class));
        FileList.Builder builder = new FileList.Builder().pathMatchPattern("");
        dummyS3Plugin().addS3DirectObject(
                builder, client, "some_bucket", "some_prefix");
        verify(client, times(2)).getObjectMetadata(any(GetObjectMetadataRequest.class));
    }

    @Test(expected = RetryExecutor.RetryGiveupException.class)
    public void addS3DirectObjectShouldThrowExceptionWhenGaveUp() throws RetryExecutor.RetryGiveupException, InterruptedException {
        doThrow(new SomeException()).when(client).getObjectMetadata(any(GetObjectMetadataRequest.class));
        FileList.Builder builder = new FileList.Builder().pathMatchPattern("");
        dummyS3Plugin().addS3DirectObject(
                builder, client, "some_bucket", "some_prefix");
    }
}
