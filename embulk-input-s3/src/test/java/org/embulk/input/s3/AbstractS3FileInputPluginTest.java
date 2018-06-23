package org.embulk.input.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Optional;
import org.embulk.EmbulkTestRuntime;
import org.embulk.spi.util.RetryExecutor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class AbstractS3FileInputPluginTest
{
    private static RetryExecutor retryExecutor()
    {
        return RetryExecutor.retryExecutor()
                .withInitialRetryWait(0)
                .withMaxRetryWait(0);
    }

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

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private AmazonS3 client;

    @Before
    public void createResources()
    {
        client = mock(AmazonS3.class);
    }

    @Test
    public void listS3FilesByPrefix()
    {
        doReturn(new ObjectListing()).when(client).listObjects(any(ListObjectsRequest.class));
        FileList.Builder builder = new FileList.Builder();
        dummyS3Plugin().listS3FilesByPrefix(builder, client, "some_bucket", "some_prefix", Optional.of("last_path"), true);
    }

    @Test
    public void listS3FileByPrefix_with_retry()
    {
        doThrow(new RuntimeException()).doReturn(new ObjectListing())
                .when(client).listObjects(any(ListObjectsRequest.class));
        FileList.Builder builder = new FileList.Builder();
        dummyS3Plugin().listS3FilesByPrefix(
                builder, client, "some_bucket", "some_prefix", Optional.of("last_path"), true,
                retryExecutor().withRetryLimit(1));
    }

    @Test(expected = SomeException.class)
    public void listS3FileByPrefix_on_retry_gave_up_should_throw_the_original_exception()
    {
        doThrow(new SomeException()).doReturn(new ObjectListing())
                .when(client).listObjects(any(ListObjectsRequest.class));
        FileList.Builder builder = new FileList.Builder();
        dummyS3Plugin().listS3FilesByPrefix(
                builder, client, "some_bucket", "some_prefix", Optional.of("last_path"), true,
                retryExecutor().withRetryLimit(0));
    }

    @Test
    public void addS3DirectObject()
    {
        doReturn(new ObjectMetadata()).when(client).getObjectMetadata(any(GetObjectMetadataRequest.class));
        FileList.Builder builder = new FileList.Builder().pathMatchPattern("");
        dummyS3Plugin().addS3DirectObject(builder, client, "some_bucket", "some_prefix");
    }

    @Test
    public void addS3DirectObject_with_retry()
    {
        doThrow(new RuntimeException()).doReturn(new ObjectMetadata())
                .when(client).getObjectMetadata(any(GetObjectMetadataRequest.class));
        FileList.Builder builder = new FileList.Builder().pathMatchPattern("");
        dummyS3Plugin().addS3DirectObject(
                builder, client, "some_bucket", "some_prefix",
                retryExecutor());
    }

    @Test(expected = SomeException.class)
    public void addS3DirectObject_on_retry_gave_up_should_throw_original_exception()
    {
        doThrow(new SomeException()).doReturn(new ObjectMetadata())
                .when(client).getObjectMetadata(any(GetObjectMetadataRequest.class));
        FileList.Builder builder = new FileList.Builder().pathMatchPattern("");
        dummyS3Plugin().addS3DirectObject(
                builder, client, "some_bucket", "some_prefix",
                retryExecutor().withRetryLimit(0));
    }
}
