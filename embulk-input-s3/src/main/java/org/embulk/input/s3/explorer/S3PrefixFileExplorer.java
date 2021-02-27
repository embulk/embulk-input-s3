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
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import org.embulk.config.ConfigException;
import org.embulk.input.s3.FileList;
import org.embulk.util.retryhelper.RetryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class S3PrefixFileExplorer extends S3FileExplorer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(S3PrefixFileExplorer.class);

    protected String pathPrefix;

    private final boolean skipGlacierObjects;

    public S3PrefixFileExplorer(final String bucketName, final AmazonS3 s3Client, final RetryExecutor retryExecutor, final String pathPrefix, final boolean skipGlacierObjects)
    {
        super(bucketName, s3Client, retryExecutor);
        this.pathPrefix = pathPrefix;
        this.skipGlacierObjects = skipGlacierObjects;
    }

    @Override
    public void addToBuilder(final FileList.Builder builder)
    {
        do {
            final List<S3ObjectSummary> s3ObjectSummaries = fetch();

            for (final S3ObjectSummary s : s3ObjectSummaries) {
                if (s.getStorageClass().equals(StorageClass.Glacier.toString())) {
                    if (skipGlacierObjects) {
                        LOGGER.warn("Skipped \"s3://{}/{}\" that stored at Glacier.", bucketName, s.getKey());
                        continue;
                    }

                    ObjectMetadata objectMetadata = fetchObjectMetadata(s);
                    if (objectMetadata != null && objectMetadata.getRestoreExpirationTime() != null) {
                        LOGGER.info("Restored Glacier object \"s3://{}/{}\" found", bucketName, s.getKey());
                    } else {
                        throw new ConfigException("Detected an object stored at Glacier. Set \"skip_glacier_objects\" option to \"true\" to skip this.");
                    }
                }
                if (s.getSize() > 0) {
                    builder.add(s.getKey(), s.getSize());
                    if (!builder.needsMore()) {
                        LOGGER.warn("Too many files matched, stop listing file");
                        return;
                    }
                }
            }
        } while (hasNext());
    }

    protected abstract List<S3ObjectSummary> fetch();

    protected abstract ObjectMetadata fetchObjectMetadata(final S3ObjectSummary obj);

    protected abstract boolean hasNext();
}
