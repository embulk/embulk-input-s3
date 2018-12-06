package org.embulk.input.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.http.HttpStatus;
import org.embulk.EmbulkTestRuntime;
import org.embulk.spi.util.RetryExecutor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class TestAbstractS3FileInputPlugin
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

    @Test(expected = AmazonServiceException.class)
    public void listS3FileByPrefix_on_retry_gave_up_should_throw_the_original_exception_in_forbidden_code()
    {
        AmazonServiceException exception = new AmazonServiceException("Forbidden exception");
        exception.setStatusCode(HttpStatus.SC_FORBIDDEN);
        exception.setErrorType(AmazonServiceException.ErrorType.Client);

        doThrow(exception).doReturn(new ObjectListing())
                .when(client).listObjects(any(ListObjectsRequest.class));
        FileList.Builder builder = new FileList.Builder();
        dummyS3Plugin().listS3FilesByPrefix(
                builder, client, "some_bucket", "some_prefix", Optional.of("last_path"), true,
                retryExecutor().withRetryLimit(1));
    }

    @Test(expected = AmazonServiceException.class)
    public void listS3FileByPrefix_on_retry_gave_up_should_throw_the_original_exception_in_methodnotallow_code()
    {
        AmazonServiceException exception = new AmazonServiceException("method not allow exception");
        exception.setStatusCode(HttpStatus.SC_METHOD_NOT_ALLOWED);
        exception.setErrorType(AmazonServiceException.ErrorType.Client);

        doThrow(exception).doReturn(new ObjectListing())
                .when(client).listObjects(any(ListObjectsRequest.class));
        FileList.Builder builder = new FileList.Builder();
        dummyS3Plugin().listS3FilesByPrefix(
                builder, client, "some_bucket", "some_prefix", Optional.of("last_path"), true,
                retryExecutor().withRetryLimit(1));
    }

    @Test(expected = AmazonServiceException.class)
    public void listS3FileByPrefix_on_retry_gave_up_should_throw_the_original_exception_in_expiredToken_code()
    {
        AmazonServiceException exception = new AmazonServiceException("expired token exception");
        exception.setStatusCode(HttpStatus.SC_BAD_REQUEST);
        exception.setErrorCode("ExpiredToken");
        exception.setErrorType(AmazonServiceException.ErrorType.Client);

        doThrow(exception).doReturn(new ObjectListing())
                .when(client).listObjects(any(ListObjectsRequest.class));
        FileList.Builder builder = new FileList.Builder();
        dummyS3Plugin().listS3FilesByPrefix(
                builder, client, "some_bucket", "some_prefix", Optional.of("last_path"), true,
                retryExecutor().withRetryLimit(1));
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
