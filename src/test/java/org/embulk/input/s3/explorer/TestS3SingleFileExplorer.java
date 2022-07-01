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
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.embulk.EmbulkTestRuntime;
import org.embulk.input.s3.FileList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestS3SingleFileExplorer
{
    private static final String PATH = "path";
    private static final String BUCKET_NAME = "bucket_name";

    @Rule
    public EmbulkTestRuntime embulkTestRuntime = new EmbulkTestRuntime();

    @Mock
    private AmazonS3 s3Client;

    @Mock
    private FileList.Builder builder;

    @Mock
    private ObjectMetadata metadata;

    private S3SingleFileExplorer s3SingleFileExplorer;

    @Before
    public void setUp()
    {
        s3SingleFileExplorer = new S3SingleFileExplorer(BUCKET_NAME, s3Client, null, PATH);
    }

    @Test
    public void addToBuilder_should_request_single_object_metadata()
    {
        when(s3Client.getObjectMetadata(any(GetObjectMetadataRequest.class))).thenReturn(metadata);
        when(metadata.getContentLength()).thenReturn(1L);
        s3SingleFileExplorer.addToBuilder(builder);

        verify(builder).add(PATH, 1);
    }
}
