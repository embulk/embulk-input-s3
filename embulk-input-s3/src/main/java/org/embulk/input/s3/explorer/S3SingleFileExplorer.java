package org.embulk.input.s3.explorer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.embulk.input.s3.DefaultRetryable;
import org.embulk.input.s3.FileList;
import org.embulk.spi.util.RetryExecutor;

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
