package org.embulk.input.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.embulk.EmbulkTestRuntime;
import org.embulk.input.s3.AbstractS3FileInputPlugin.S3InputStreamReopener;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.embulk.spi.util.RetryExecutor.retryExecutor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class TestS3InputStreamReopener
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private AmazonS3 client;

    @Before
    public void createResources()
    {
        client = mock(AmazonS3.class);
    }

    @Test
    public void reopenS3FileByReopener()
            throws Exception
    {
        String content = "value";

        { // not retry
            doReturn(s3object("in/aa/a", content)).when(client).getObject(any(GetObjectRequest.class));

            S3InputStreamReopener opener = new S3InputStreamReopener(client, new GetObjectRequest("my_bucket", "in/aa/a"), content.length());

            try (InputStream in = opener.reopen(0, new RuntimeException())) {
                BufferedReader r = new BufferedReader(new InputStreamReader(in));
                assertEquals("value", r.readLine());
            }
        }

        { // retry once
            doThrow(new RuntimeException()).doReturn(s3object("in/aa/a", content)).when(client).getObject(any(GetObjectRequest.class));

            S3InputStreamReopener opener = new S3InputStreamReopener(
                    client,
                    new GetObjectRequest("my_bucket", "in/aa/a"),
                    content.length(),
                    retryExecutor()
                            .withInitialRetryWait(0)
                            .withRetryLimit(1));

            try (InputStream in = opener.reopen(0, new RuntimeException())) {
                BufferedReader r = new BufferedReader(new InputStreamReader(in));
                assertEquals("value", r.readLine());
            }
        }
    }

    @Test(expected = AmazonClientException.class)
    public void reopenS3FileByReopener_on_retry_gave_up_should_throw_original_exception() throws Exception
    {
        String content = "value";
        doThrow(new AmazonClientException("no")).doReturn(s3object("in/aa/a", content)).when(client).getObject(any(GetObjectRequest.class));

        S3InputStreamReopener opener = new S3InputStreamReopener(
                client,
                new GetObjectRequest("my_bucket", "in/aa/a"),
                content.length(),
                retryExecutor()
                        .withInitialRetryWait(0)
                        .withRetryLimit(0));

        opener.reopen(0, new RuntimeException());
    }

    @Test(expected = AmazonClientException.class)
    public void reopenS3FileByReopener_on_retry_always_throw_exception()
            throws Exception
    {
        { // always failed call with 2 retries
            S3Object s3Object = mock(S3Object.class);
            doReturn(s3Object).when(client).getObject(any(GetObjectRequest.class));
            doThrow(new AmazonClientException("This exception is thrown when retrying.")).when(s3Object).getObjectContent();
            S3InputStreamReopener opener = new S3InputStreamReopener(
                    client,
                    new GetObjectRequest("my_bucket", "in/aa/a"),
                    "value".length(),
                    retryExecutor()
                            .withInitialRetryWait(0)
                            .withRetryLimit(2));

            try (InputStream in = opener.reopen(0, new AmazonClientException("This exception can be ignored"))) {
                fail("Should throw exception.");
            }
        }
    }

    static S3Object s3object(String key, String value)
    {
        S3Object o = new S3Object();
        o.setObjectContent(new S3ObjectInputStream(new ByteArrayInputStream(value.getBytes()), null));
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(value.length());
        o.setObjectMetadata(om);
        return o;
    }
}
