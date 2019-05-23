package org.embulk.input.s3.explorer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.embulk.input.s3.DefaultRetryable;
import org.embulk.spi.util.RetryExecutor;

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
    protected boolean hasNext()
    {
        return lastPath != null;
    }
}
