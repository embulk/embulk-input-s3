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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class S3TimeOrderPrefixFileExplorer extends S3PrefixFileExplorer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(S3TimeOrderPrefixFileExplorer.class);

    private final Optional<Date> from;
    private final Date to;

    private String lastPath;

    private int numOfReq = 0;

    public S3TimeOrderPrefixFileExplorer(final String bucket, final AmazonS3 client, final RetryExecutor retryExecutor,
            final String pathPrefix, final boolean skipGlacierObjects, final Optional<Date> from, final Date to)
    {
        super(bucket, client, retryExecutor, pathPrefix, skipGlacierObjects);
        this.from = from;
        this.to = to;
    }

    @Override
    public List<S3ObjectSummary> fetch()
    {
        ++numOfReq;

        final ListObjectsRequest req = new ListObjectsRequest(bucketName, pathPrefix, lastPath, null, 1024);
        final ObjectListing objectListing = new DefaultRetryable<ObjectListing>("Listing objects")
        {
            @Override
            public ObjectListing call()
            {
                return s3Client.listObjects(req);
            }
        }.executeWith(retryExecutor);
        lastPath = objectListing.getNextMarker();

        return objectListing.getObjectSummaries()
                .stream()
                .filter(s3ObjectSummary -> s3ObjectSummary.getLastModified().before(to)
                        && (!from.isPresent() || s3ObjectSummary.getLastModified().equals(from.get()) || s3ObjectSummary.getLastModified().after(from.get())))
                .collect(Collectors.toList());
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
    public boolean hasNext()
    {
        if (lastPath == null) {
            LOGGER.info("The total number of LIST requests is {}{}.", numOfReq,
                    numOfReq < 10 ? "" : ". Clean up your s3 bucket to reduce the number of requests and improve the ingesting performance");
            return false;
        }
        return true;
    }
}
