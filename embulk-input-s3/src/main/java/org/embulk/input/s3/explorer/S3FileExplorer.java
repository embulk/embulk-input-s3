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
import org.embulk.input.s3.FileList;
import org.embulk.spi.util.RetryExecutor;

public abstract class S3FileExplorer
{
    protected String bucketName;
    protected AmazonS3 s3Client;
    protected RetryExecutor retryExecutor;

    public S3FileExplorer(final String bucketName, final AmazonS3 s3Client, final RetryExecutor retryExecutor)
    {
        this.bucketName = bucketName;
        this.s3Client = s3Client;
        this.retryExecutor = retryExecutor;
    }

    public abstract void addToBuilder(FileList.Builder builder);
}
