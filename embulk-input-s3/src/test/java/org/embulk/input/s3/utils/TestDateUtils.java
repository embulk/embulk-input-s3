package org.embulk.input.s3.utils;

import org.embulk.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestDateUtils
{

    private List<String> relevantFormats;
    private Date expectedDate;

    @Before
    public void setUp() throws ParseException
    {
        relevantFormats = Collections.singletonList("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        expectedDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2011-12-03 10:15:30");
    }

    @Test
    public void testParse()
    {
        final Date actual = DateUtils.parse("2011-12-03T10:15:30.000Z", relevantFormats);
        assertEquals(expectedDate, actual);
    }

    @Test(expected = ConfigException.class)
    public void testNoMilliseconds()
    {
        DateUtils.parse("2011-12-03T10:15:30Z", relevantFormats);
    }

    @Test(expected = ConfigException.class)
    public void testOffsetNotSet()
    {
        DateUtils.parse("2011-12-03T10:15:30.000", relevantFormats);
    }
}
