package org.embulk.input.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import org.embulk.EmbulkTestRuntime;
import org.embulk.input.s3.AbstractS3FileInputPlugin.S3InputStreamReopener;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.junit.Assert.assertEquals;
import static org.embulk.input.s3.TestS3FileInputPlugin.s3object;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class TestS3InputStreamReopener
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private AmazonS3Client client;

    @Before
    public void createResources()
    {
        client = mock(AmazonS3Client.class);
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
}
