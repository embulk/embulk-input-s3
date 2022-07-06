/*
 * Copyright 2015 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.input.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.embulk.EmbulkTestRuntime;
import org.embulk.util.retryhelper.RetryExecutor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

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

            final S3FileInputPlugin.S3InputStreamReopener opener = new S3FileInputPlugin.S3InputStreamReopener(
                    client, new GetObjectRequest("my_bucket", "in/aa/a"), content.length());

            try (InputStream in = opener.reopen(0, new RuntimeException())) {
                BufferedReader r = new BufferedReader(new InputStreamReader(in));
                assertEquals("value", r.readLine());
            }
        }

        { // retry once
            doThrow(new RuntimeException()).doReturn(s3object("in/aa/a", content)).when(client).getObject(any(GetObjectRequest.class));

            final S3FileInputPlugin.S3InputStreamReopener opener = new S3FileInputPlugin.S3InputStreamReopener(
                    client,
                    new GetObjectRequest("my_bucket", "in/aa/a"),
                    content.length(),
                    RetryExecutor.builder()
                            .withInitialRetryWaitMillis(0)
                            .withRetryLimit(1)
                            .build());

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

        final S3FileInputPlugin.S3InputStreamReopener opener = new S3FileInputPlugin.S3InputStreamReopener(
                client,
                new GetObjectRequest("my_bucket", "in/aa/a"),
                content.length(),
                RetryExecutor.builder()
                        .withInitialRetryWaitMillis(0)
                        .withRetryLimit(0)
                        .build());

        opener.reopen(0, new RuntimeException());
    }

    @Test(expected = AmazonClientException.class)
    public void reopenS3FileByReopener_on_retry_always_throw_exception()
            throws Exception
    {
        // always failed call with 2 retries
        doThrow(new AmazonClientException("This exception is thrown when retrying.")).when(client).getObject(any(GetObjectRequest.class));
        final S3FileInputPlugin.S3InputStreamReopener opener = new S3FileInputPlugin.S3InputStreamReopener(
                client,
                new GetObjectRequest("my_bucket", "in/aa/a"),
                "value".length(),
                RetryExecutor.builder()
                        .withInitialRetryWaitMillis(0)
                        .withRetryLimit(2)
                        .build());

        try (InputStream in = opener.reopen(0, new AmazonClientException("This exception can be ignored"))) {
            fail("Should throw exception.");
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
