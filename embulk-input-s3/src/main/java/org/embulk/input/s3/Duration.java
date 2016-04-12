package org.embulk.input.s3;

import java.util.Objects;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.TimeUnit;
import com.google.common.base.Preconditions;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

// This class should be moved to org.embulk.spi.unit
public class Duration
        implements Comparable<Duration>
{
    private static final Pattern PATTERN = Pattern.compile("\\A(\\d+(?:\\.\\d+)?)\\s?([a-zA-Z]*)\\z");

    private final long usec;
    private final Unit displayUnit;

    public Duration(double duration, Unit unit)
    {
        Preconditions.checkArgument(!Double.isInfinite(duration), "duration is infinite");
        Preconditions.checkArgument(!Double.isNaN(duration), "duration is not a number");
        Preconditions.checkArgument(duration >= 0, "duration is negative");
        Preconditions.checkNotNull(unit, "unit is null");
        Preconditions.checkArgument(duration * unit.getFactorToUsec() <= Long.MAX_VALUE, "duration is large than (2^63)-1 in milliseconds");
        this.usec = (long) (duration * unit.getFactorToUsec());
        this.displayUnit = unit;
    }

    @JsonCreator
    @Deprecated
    public Duration(long usec)
    {
        Preconditions.checkArgument(usec >= 0, "duration is negative");
        this.usec = usec;
        this.displayUnit = Unit.MSEC;
    }

    public long getMillis()
    {
        return usec / 1000;
    }

    public int getMillisInt()
    {
        if (usec / 1000 > Integer.MAX_VALUE) {
            throw new RuntimeException("Duration is too large (must be smaller than (2^31)-1 milliseconds, abount 24 days)");
        }
        return (int) (usec / 1000);
    }

    public long roundTo(Unit unit)
    {
        return (long) Math.floor(getValue(unit) + 0.5);
    }

    public double getValue(Unit unit)
    {
        return usec / (double) unit.getFactorToUsec();
    }

    @JsonCreator
    public static Duration parseDuration(String duration)
    {
        Preconditions.checkNotNull(duration, "duration is null");
        Preconditions.checkArgument(!duration.isEmpty(), "duration is empty");

        Matcher matcher = PATTERN.matcher(duration);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid time duration string '" + duration + "'");
        }

        double value = Double.parseDouble(matcher.group(1));  // NumberFormatException extends IllegalArgumentException.

        String unitString = matcher.group(2);
        if (unitString.isEmpty()) {
            return new Duration(value, Unit.SECONDS);
        } else {
            String upperUnitString = unitString.toUpperCase(Locale.ENGLISH);
            for (Unit unit : Unit.values()) {
                if (unit.getUnitString().toUpperCase(Locale.ENGLISH).equals(upperUnitString)) {
                    return new Duration(value, unit);
                }
            }
        }

        throw new IllegalArgumentException("Unknown unit '" + unitString + "'");
    }

    @JsonValue
    @Override
    public String toString()
    {
        double value = getValue(displayUnit);
        String integer = String.format(Locale.ENGLISH, "%d", (long) value);
        String decimal = String.format(Locale.ENGLISH, "%.2f", value);
        if (decimal.equals(integer + ".00")) {
            return integer + displayUnit.getUnitString();
        } else {
            return decimal + displayUnit.getUnitString();
        }
    }

    @Override
    public int compareTo(Duration o)
    {
        return Long.compare(usec, o.usec);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Duration)) {
            return false;
        }
        Duration o = (Duration) obj;
        return this.usec == o.usec;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(usec);
    }

    public enum Unit
    {
        USEC(1L, "usec"),
        MSEC(1000L, "msec"),
        SECONDS(1000L*1000L, "sec"),
        MINUTES(60*1000L*1000L, "min"),
        HOURS(60*60*1000L*1000L, "hour"),
        DAYS(24*60*60*1000L*1000L, "day");

        private final long factorToUsec;
        private final String unitString;

        Unit(long factorToUsec, String unitString)
        {
            this.factorToUsec = factorToUsec;
            this.unitString = unitString;
        }

        long getFactorToUsec()
        {
            return factorToUsec;
        }

        String getUnitString()
        {
            return unitString;
        }
    }
}
