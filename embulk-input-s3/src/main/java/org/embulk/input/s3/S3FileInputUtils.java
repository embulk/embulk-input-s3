package org.embulk.input.s3;

import org.embulk.spi.Exec;
import org.embulk.spi.util.RetryExecutor;
import org.slf4j.Logger;

/**
 * Utility class for S3 File Input.
 */
public final class S3FileInputUtils
{
    private S3FileInputUtils()
    {
    }

    public static final <T> T executeWithRetry(int maximumRetries, int initialRetryIntervalMillis, int maximumRetryIntervalMillis, AlwaysRetryRetryable<T> alwaysRetryRetryable)
            throws RetryExecutor.RetryGiveupException, InterruptedException
    {
        return RetryExecutor.retryExecutor()
                .withRetryLimit(maximumRetries)
                .withInitialRetryWait(initialRetryIntervalMillis)
                .withMaxRetryWait(maximumRetryIntervalMillis)
                .runInterruptible(alwaysRetryRetryable);
    }

    public abstract static class AlwaysRetryRetryable<T> implements RetryExecutor.Retryable<T>
    {
        private static final Logger LOGGER = Exec.getLogger(AlwaysRetryRetryable.class);

        @Override
        public abstract T call() throws Exception;

        @Override
        public boolean isRetryableException(Exception exception)
        {
            return true;
        }

        @Override
        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait) throws RetryExecutor.RetryGiveupException
        {
            LOGGER.warn("Retry [{}]/[{}] with retryWait [{}] on exception {}", retryCount, retryLimit, retryWait, exception.getMessage());
        }

        @Override
        public void onGiveup(Exception firstException, Exception lastException) throws RetryExecutor.RetryGiveupException
        {
            LOGGER.error("Giving up retry, first exception is [{}], last exception is [{}]", firstException.getMessage(), lastException.getMessage());
        }
    }
}
