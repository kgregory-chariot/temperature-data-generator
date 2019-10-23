package com.chariotsolutions.datagen.temperature.shared;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 *  An iterator over timestamps: returns values a sequence of values from start to end,
 *  incremented by a given interval.
 */
public class TimeRange
implements Iterator<Long>, Iterable<Long>
{
    private long start;
    private long finish;
    private long interval;
    private long current;

    
    /**
     *  Base constructor.
     */
    public TimeRange(long start, long finish, long interval)
    {
        this.start = start;
        this.finish = finish;
        this.interval = interval;
        this.current = start;
    }
    
    
    /**
     *  Constructor that takes ISO-8601 timestamps. See {@link #parseTimestamp}
     *  for details.
     */
    public TimeRange(String start, String finish, long interval)
    {
        this(parseTimestamp(start), parseTimestamp(finish), interval);
    }
    
    
    public static long parseTimestamp(String value)
    {
        Instant instant = null;
        try
        {
            LocalDateTime local = LocalDateTime.parse(value);
            instant = local.atZone(ZoneId.systemDefault()).toInstant();
        }
        catch (DateTimeParseException ignored)
        {
            OffsetDateTime parsed = OffsetDateTime.parse(value);
            instant = parsed.toInstant();
        }
        
        return instant.toEpochMilli();
    }
    
   
    
    /**
     *  Returns a copy of this range, which can then be used as an iterator
     *  (could just return itself, but then it would only be iterable once.
     */
    @Override
    public Iterator<Long> iterator()
    {
        return new TimeRange(start, finish, interval);
    }

    @Override
    public boolean hasNext()
    {
        return current < finish;
    }

    @Override
    public Long next()
    {
        if (! hasNext())
            throw new NoSuchElementException("can't iterate past end of interval");
        
        long value = current;
        current += interval;
        return value;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException("fixed iteration, no removals allowed");
    }
}
