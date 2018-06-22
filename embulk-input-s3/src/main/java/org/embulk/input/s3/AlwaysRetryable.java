package org.embulk.input.s3;

import com.google.common.base.Throwables;
import org.embulk.spi.Exec;
import org.embulk.spi.util.RetryExecutor;
import org.slf4j.Logger;

import static java.lang.String.format;
import static org.embulk.spi.util.RetryExecutor.RetryGiveupException;
import static org.embulk.spi.util.RetryExecutor.Retryable;

/**
 * Always retry, regardless the occurred exceptions,
 * Also provide a default approach for exception propagation.
 */
public abstract class AlwaysRetryable<T> implements Retryable<T>
{
    private static final Logger log = Exec.getLogger(AlwaysRetryable.class);

    private String operationName;

    /**
     * @param operationName the name that will be referred on logging
     */
    public AlwaysRetryable(String operationName)
    {
        this.operationName = operationName;
    }

    @Override
    public boolean isRetryableException(Exception exception)
    {
        return true;
    }

    @Override
    public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
    {
        String message = format("%s failed. Retrying %d/%d after %d seconds. Message: %s",
                operationName, retryCount, retryLimit, retryWait / 1000, exception.getMessage());
        if (retryCount % retryLimit == 0) {
            log.warn(message, exception);
        }
        else {
            log.warn(message);
        }
    }

    @Override
    public void onGiveup(Exception firstException, Exception lastException)
    {
        // Exceptions would be propagated, so it's up to the caller to handle, this is just warning
        log.warn("Giving up on retrying for {}, first exception is [{}], last exception is [{}]",
                operationName, firstException.getMessage(), lastException.getMessage());
    }

    /**
     * Run itself by the supplied executor,
     *
     * This propagates all exceptions (as unchecked) and unwrap RetryGiveupException for the original cause,
     * For convenient, it execute normally without retrying when executor is null.
     *
     * @throws RuntimeException wrap around whatever the original cause of failure (potentially thread interruption)
     */
    public T executeWith(RetryExecutor executor)
    {
        if (executor == null) {
            try {
                return this.call();
            }
            catch (Exception e) {
                Throwables.propagate(e);
            }
        }

        try {
            return executor.runInterruptible(this);
        }
        catch (RetryGiveupException e) {
            throw Throwables.propagate(e.getCause());
        }
        catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Run itself by the supplied executor,
     *
     * This propagates all exceptions (as unchecked), while `propagateAsIsException` will be re-throw as-is whether
     * it is an unchecked or checked exception. Also, if RetryGiveException is thrown, it will be unwrap.
     * For convenient, it execute normally without retrying when executor is null.
     *
     * @throws X whatever checked exception that you decided to propagate directly
     * @throws RuntimeException wrap around whatever the original cause of failure (potentially thread interruption)
     */
    public <X extends Throwable> T executeAndPropagateAsIs(RetryExecutor executor,
                                                           Class<X> propagateAsIsException) throws X
    {
        if (executor == null) {
            try {
                return this.call();
            }
            catch (Exception e) {
                Throwables.propagate(e);
            }
        }

        try {
            return executor.runInterruptible(this);
        }
        catch (RetryGiveupException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), propagateAsIsException);
            throw Throwables.propagate(e.getCause());
        }
        catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }
}
