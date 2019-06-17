package org.embulk.input.s3.utils;

import com.google.common.base.Joiner;
import org.embulk.config.ConfigException;
import org.joda.time.format.DateTimeFormat;

import java.util.Date;
import java.util.List;

public class DateUtils
{
    public static Date parse(final String value, final List<String> supportedFormats)
            throws ConfigException
    {
        for (final String fmt : supportedFormats) {
            try {
                return DateTimeFormat.forPattern(fmt).parseDateTime(value).toDate();
            } catch (final IllegalArgumentException e) {
                // ignorable exception
            }
        }
        throw new ConfigException("Unsupported DateTime value: '" + value + "', supported formats: [" + Joiner.on(",").join(supportedFormats) + "]");
    }

    private DateUtils()
    {
    }
}
