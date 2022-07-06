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
import org.embulk.input.s3.DefaultRetryable;
import org.embulk.input.s3.FileList;
import org.embulk.util.retryhelper.RetryExecutor;

public class S3SingleFileExplorer extends S3FileExplorer
{
    private final String path;

    public S3SingleFileExplorer(final String bucket, final AmazonS3 client, final RetryExecutor retryExecutor, final String path)
    {
        super(bucket, client, retryExecutor);
        this.path = path;
    }

    @Override
    public void addToBuilder(final FileList.Builder builder)
    {
        final GetObjectMetadataRequest objectMetadataRequest = new GetObjectMetadataRequest(bucketName, path);

        final ObjectMetadata objectMetadata = new DefaultRetryable<ObjectMetadata>("Looking up for a single object") {
            @Override
            public ObjectMetadata call()
            {
                return s3Client.getObjectMetadata(objectMetadataRequest);
            }
        }.executeWith(retryExecutor);

        builder.add(path, objectMetadata.getContentLength());
    }
}
