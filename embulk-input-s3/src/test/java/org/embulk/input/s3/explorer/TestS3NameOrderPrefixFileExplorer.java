/*
 * Copyright 2019 The Embulk project
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

package org.embulk.input.s3.explorer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import org.embulk.EmbulkTestRuntime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.FieldSetter;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestS3NameOrderPrefixFileExplorer
{
    private static final String BUCKET_NAME = "bucket_name";
    private static final String PATH_PREFIX = "path_prefix";
    private static final String LAST_PATH = "last_path";

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Mock
    private AmazonS3 s3Client;

    private S3NameOrderPrefixFileExplorer s3NameOrderPrefixFileExplorer;

    @Before
    public void setUp()
    {
        s3NameOrderPrefixFileExplorer = new S3NameOrderPrefixFileExplorer(BUCKET_NAME, s3Client, null, PATH_PREFIX, false, LAST_PATH);
    }

    @Test
    public void fetch_should_return_list_objects()
    {
        final ObjectListing ol = mock(ObjectListing.class);
        when(s3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(ol);

        s3NameOrderPrefixFileExplorer.fetch();
        final ArgumentCaptor<ListObjectsRequest> listObjectsRequestCaptor = ArgumentCaptor.forClass(ListObjectsRequest.class);

        verify(ol).getNextMarker();
        verify(s3Client).listObjects(listObjectsRequestCaptor.capture());
        final ListObjectsRequest listObjectsRequest = listObjectsRequestCaptor.getValue();
        assertEquals(BUCKET_NAME, listObjectsRequest.getBucketName());
        assertEquals(PATH_PREFIX, listObjectsRequest.getPrefix());
        assertEquals(LAST_PATH, listObjectsRequest.getMarker());
    }

    @Test
    public void hasNext_should_return_false_if_no_lastpath() throws NoSuchFieldException
    {
        new FieldSetter(s3NameOrderPrefixFileExplorer, s3NameOrderPrefixFileExplorer.getClass().getDeclaredField("lastPath")).set(null);
        assertFalse(s3NameOrderPrefixFileExplorer.hasNext());
    }
}
