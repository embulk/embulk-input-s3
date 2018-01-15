package org.embulk.input.s3;

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

import static org.junit.Assert.assertEquals;
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

            S3InputStreamReopener opener = new S3InputStreamReopener(client, new GetObjectRequest("my_bucket", "in/aa/a"), content.length());

            try (InputStream in = opener.reopen(0, new RuntimeException())) {
                BufferedReader r = new BufferedReader(new InputStreamReader(in));
                assertEquals("value", r.readLine());
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
