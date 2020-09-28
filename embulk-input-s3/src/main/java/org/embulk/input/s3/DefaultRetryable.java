/*
 * Copyright 2018 The Embulk project
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

package org.embulk.input.s3;

import com.amazonaws.AmazonServiceException;
import org.apache.http.HttpStatus;
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.embulk.util.retryhelper.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import static java.lang.String.format;

/**
 * Retryable utility, regardless the occurred exceptions,
 * Also provide a default approach for exception propagation.
 */
public class DefaultRetryable<T> implements Retryable<T>
{
    private static final Logger log = LoggerFactory.getLogger(DefaultRetryable.class);
    private static final Set<Integer> NONRETRYABLE_STATUS_CODES = new HashSet<Integer>(2);
    private static final Set<String> NONRETRYABLE_ERROR_CODES = new HashSet<String>(1);
    private String operationName;
    private Callable<T> callable;

    static {
        NONRETRYABLE_STATUS_CODES.add(HttpStatus.SC_FORBIDDEN);
        NONRETRYABLE_STATUS_CODES.add(HttpStatus.SC_METHOD_NOT_ALLOWED);
        NONRETRYABLE_ERROR_CODES.add("ExpiredToken");
    }

    /**
     * @param operationName the name that will be referred on logging
     */
    public DefaultRetryable(String operationName)
    {
        this.operationName = operationName;
    }

    /**
     * @param operationName the name that will be referred on logging
     * @param callable the operation, either define this at construction time or override the call() method
     */
    public DefaultRetryable(String operationName, Callable<T> callable)
    {
        this.operationName = operationName;
        this.callable = callable;
    }

    public DefaultRetryable()
    {
        this("Anonymous operation");
    }

    public DefaultRetryable(Callable<T> callable)
    {
        this("Anonymous operation", callable);
    }

    @Override
    public T call() throws Exception
    {
        if (callable != null) {
            return callable.call();
        }
        else {
            throw new IllegalStateException("Either override call() or construct with a Runnable");
        }
    }

    @Override
    public boolean isRetryableException(Exception exception)
    {
        // No retry on a subset of service exceptions
        if (exception instanceof AmazonServiceException) {
            AmazonServiceException ase = (AmazonServiceException) exception;
            return !NONRETRYABLE_STATUS_CODES.contains(ase.getStatusCode()) && !NONRETRYABLE_ERROR_CODES.contains(ase.getErrorCode());
        }
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
     * This propagates all exceptions (as unchecked) and unwrap RetryGiveupException for the original cause.
     * If the original exception already is a RuntimeException, it will be propagated as is. If not, it will
     * be wrapped around with a RuntimeException.
     *
     * For convenient, it execute normally without retrying when executor is null.
     *
     * @throws RuntimeException the original cause
     */
    public T executeWith(RetryExecutor executor)
    {
        if (executor == null) {
            try {
                return this.call();
            }
            catch (Exception e) {
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                }
                throw new RuntimeException(e);
            }
        }

        try {
            return executor.runInterruptible(this);
        }
        catch (RetryGiveupException e) {
            final Exception cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException(cause);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Run itself by the supplied executor,
     *
     * Same as `executeWith`, this propagates all original exceptions. But `propagateAsIsException` will
     * be re-throw without being wrapped on a RuntimeException, whether it is a checked or unchecked exception.
     *
     * For convenient, it execute normally without retrying when executor is null.
     *
     * @throws X whatever checked exception that you decided to propagate directly
     * @throws RuntimeException wrap around whatever the original cause of failure (potentially thread interruption)
     */
    public <X extends Throwable> T executeWithCheckedException(RetryExecutor executor,
                                                               Class<X> propagateAsIsException) throws X
    {
        if (executor == null) {
            try {
                return this.call();
            }
            catch (Exception e) {
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                }
                throw new RuntimeException(e);
            }
        }

        try {
            return executor.runInterruptible(this);
        }
        catch (RetryGiveupException e) {
            final Exception cause = e.getCause();
            if (cause != null) {
                if (propagateAsIsException.isInstance(cause)) {
                    throw propagateAsIsException.cast(cause);
                }
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException(cause);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
