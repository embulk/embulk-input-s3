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
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.embulk.input.s3.DefaultRetryable;
import org.embulk.util.retryhelper.RetryExecutor;

import java.util.List;

public class S3NameOrderPrefixFileExplorer extends S3PrefixFileExplorer
{
    private String lastPath;

    public S3NameOrderPrefixFileExplorer(final String bucketName, final AmazonS3 s3Client, final RetryExecutor retryExecutor,
            final String pathPrefix, final boolean skipGlacierObjects, final String lastPath)
    {
        super(bucketName, s3Client, retryExecutor, pathPrefix, skipGlacierObjects);
        this.lastPath = lastPath;
    }

    @Override
    protected List<S3ObjectSummary> fetch()
    {
        final ListObjectsRequest req = new ListObjectsRequest(bucketName, pathPrefix, lastPath, null, 1024);
        final ObjectListing ol = new DefaultRetryable<ObjectListing>("Listing objects")
        {
            @Override
            public ObjectListing call()
            {
                return s3Client.listObjects(req);
            }
        }.executeWith(retryExecutor);
        lastPath = ol.getNextMarker();

        return ol.getObjectSummaries();
    }

    @Override
    protected ObjectMetadata fetchObjectMetadata(S3ObjectSummary obj) {
        final GetObjectMetadataRequest req = new GetObjectMetadataRequest(obj.getBucketName(), obj.getKey());

        return new DefaultRetryable<ObjectMetadata>("Get object metadata")
        {
            @Override
            public ObjectMetadata call() { return s3Client.getObjectMetadata(req); }
        }.executeWith(retryExecutor);
    }

    @Override
    protected boolean hasNext()
    {
        return lastPath != null;
    }
}
