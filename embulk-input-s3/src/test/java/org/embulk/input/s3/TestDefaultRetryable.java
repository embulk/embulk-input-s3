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

import org.embulk.EmbulkTestRuntime;
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Callable;

import static java.lang.String.format;
import static org.msgpack.core.Preconditions.checkArgument;

public class TestDefaultRetryable
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();  // require for DefaultRetryable's logger

    private static class Deny extends RuntimeException implements Callable
    {
        private int pastCalls = 0;
        private final int targetCalls;
        private Exception exception;

        Deny(int targetCalls)
        {
            super(format("Try harder! (Will pass after %d calls)", targetCalls));
            checkArgument(targetCalls >= 0);
            this.targetCalls = targetCalls;
        }

        static Deny until(int calls)
        {
            return new Deny(calls);
        }

        Deny with(Exception exception)
        {
            this.exception = exception;
            return this;
        }

        @Override
        public Object call() throws Exception
        {
            if (pastCalls < targetCalls) {
                pastCalls++;
                if (exception != null) {
                    throw exception;
                }
                else {
                    throw this;
                }
            }
            pastCalls++;
            return null;
        }
    }

    private static RetryExecutor.Builder retryExecutorBuilder()
    {
        return RetryExecutor.builder()
                .withInitialRetryWaitMillis(0)
                .withMaxRetryWaitMillis(0);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void guarantee_retry_attempts_just_like_Retryable() throws Exception
    {
        retryExecutorBuilder()
                .withRetryLimit(0)
                .build()
                .run(new DefaultRetryable(Deny.until(0)));
        retryExecutorBuilder()
                .withRetryLimit(1)
                .build()
                .run(new DefaultRetryable(Deny.until(1)));
        retryExecutorBuilder()
                .withRetryLimit(2)
                .build()
                .run(new DefaultRetryable(Deny.until(1)));
        retryExecutorBuilder()
                .withRetryLimit(3)
                .build()
                .run(new DefaultRetryable(Deny.until(2)));
    }

    @Test(expected = RetryGiveupException.class)
    @SuppressWarnings("unchecked")
    public void fail_after_exceeding_attempts_just_like_Retryable() throws Exception
    {
        retryExecutorBuilder()
                .withRetryLimit(3)
                .build()
                .run(new DefaultRetryable(Deny.until(4)));
    }

    @Test(expected = Deny.class)
    @SuppressWarnings("unchecked")
    public void execute_should_unwrap_RetryGiveupException() throws Exception
    {
        new DefaultRetryable(Deny.until(4))
                .executeWith(retryExecutorBuilder().withRetryLimit(3).build());
    }

    @Test(expected = RuntimeException.class)
    @SuppressWarnings("unchecked")
    public void execute_should_unwrap_RetryGiveupException_but_rewrap_checked_exception_in_a_RuntimeException()
    {
        new DefaultRetryable(Deny.until(4).with(new Exception("A checked exception")))
                .executeWith(retryExecutorBuilder().withRetryLimit(3).build());
    }

    @Test(expected = IOException.class)
    public void executeAndPropagateAsIs_should_leave_original_exception_unwrapped() throws IOException
    {
        RetryExecutor retryExc = retryExecutorBuilder().withRetryLimit(3).build();
        // An explicit type parameter for operation return type is needed here,
        // Without one, javac (at least on 1.8) will fails to infer the X exception type parameter.
        new DefaultRetryable<Object>() {
            @Override
            public Object call() throws IOException
            {
                throw new IOException();
            }
        }.executeWithCheckedException(retryExc, IOException.class);
    }

    @Test(expected = IllegalStateException.class)
    public void execute_without_an_implementation_should_throw_an_IllegalStateException()
    {
        new DefaultRetryable().executeWith(retryExecutorBuilder().build());
    }
}
