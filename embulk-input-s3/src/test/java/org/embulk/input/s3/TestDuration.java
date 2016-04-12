package org.embulk.input.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import static org.embulk.input.s3.TestS3FileInputPlugin.parserConfig;
import static org.embulk.input.s3.TestS3FileInputPlugin.schemaConfig;
import static org.junit.Assert.assertEquals;

public class TestDuration
{
    @Test
    public void testUnits()
    {
        assertDuration(1, "1usec");
        assertDuration(1000L, "1msec");
        assertDuration(1000*1000L, "1sec");
        assertDuration(60*1000*1000L, "1min");
        assertDuration(60*60*1000*1000L, "1hour");
        assertDuration(24*60*60*1000*1000L, "1day");

        assertDuration(2, "2usec");
        assertDuration(2000L, "2msec");
        assertDuration(2000*1000L, "2sec");
        assertDuration(2*60*1000*1000L, "2min");
        assertDuration(2*60*60*1000*1000L, "2hour");
        assertDuration(2*24*60*60*1000*1000L, "2day");

        assertDuration(0, "0.0usec");
        assertDuration(2400L, "2.4msec");
        assertDuration(2400*1000L, "2.4sec");
        assertDuration(144*1000*1000L, "2.4min");
        assertDuration(144*60*1000*1000L, "2.4hour");
        assertDuration(60*60*60*1000*1000L, "2.5day");
    }

    public void assertDuration(long usec, String str)
    {
        assertEquals(usec, Duration.parseDuration(str).roundTo(Duration.Unit.USEC));
        assertEquals(Duration.parseDuration(str), Duration.parseDuration(Duration.parseDuration(str).toString()));
        if (usec % 1000 == 0) {
            assertEquals(usec / 1000L, Duration.parseDuration(str).getMillis());
        }
    }
}
