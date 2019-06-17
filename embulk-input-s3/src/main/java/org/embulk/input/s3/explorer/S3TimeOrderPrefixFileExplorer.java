package org.embulk.input.s3.explorer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.lang3.StringUtils;
import org.embulk.input.s3.DefaultRetryable;
import org.embulk.spi.Exec;
import org.embulk.spi.util.RetryExecutor;
import org.slf4j.Logger;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class S3TimeOrderPrefixFileExplorer extends S3PrefixFileExplorer
{
    private static final Logger LOGGER = Exec.getLogger(S3TimeOrderPrefixFileExplorer.class);

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
    public boolean hasNext()
    {
        if (lastPath == null) {
            LOGGER.info("The total number of LIST requests is {}{}.", numOfReq,
                    numOfReq < 10 ? StringUtils.EMPTY : ". Clean up your s3 bucket to reduce the number of requests and improve the ingesting performance");
            return false;
        }
        return true;
    }
}
