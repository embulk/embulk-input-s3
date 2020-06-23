package org.embulk.input.s3.utils;

import com.google.common.base.Joiner;
import org.embulk.config.ConfigException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class DateUtils
{
    public static Date parse(final String value, final List<String> supportedFormats)
            throws ConfigException
    {
        for (final String fmt : supportedFormats) {
            try {
                return new SimpleDateFormat(fmt).parse(value);
            }
            catch (final ParseException | IllegalArgumentException | NullPointerException e) {
                // ignorable exception
            }
        }
        throw new ConfigException("Unsupported DateTime value: '" + value + "', supported formats: [" + Joiner.on(",").join(supportedFormats) + "]");
    }

    private DateUtils()
    {
    }
}
