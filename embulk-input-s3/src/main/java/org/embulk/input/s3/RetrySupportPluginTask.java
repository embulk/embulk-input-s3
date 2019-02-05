package org.embulk.input.s3;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

public interface RetrySupportPluginTask extends Task
{
    @Config("maximum_retries")
    @ConfigDefault("7")
    int getMaximumRetries();

    @Config("initial_retry_interval_millis")
    @ConfigDefault("2000")
    int getInitialRetryIntervalMillis();

    @Config("maximum_retry_interval_millis")
    @ConfigDefault("480000")
    int getMaximumRetryIntervalMillis();
}
