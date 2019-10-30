package com.chariotsolutions.datagen.temperature.shared;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.time.Instant;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.kdgcommons.io.IOUtil;


/**
 *  Generates text output for a set of data generators over time.
 */
public class DataStream
{
    private Logger logger = LoggerFactory.getLogger(getClass());

    private List<DataGenerator> generators;
    private TimeRange timeRange;
    private Function<Reading,String> xform;


    /**
     *  @param devices      The list of device-level reading generators.
     *  @param timerange    The time range that is iterated to generate readings.
     *  @param xform        A function that creates a string representation of a Reading.
     */
    public DataStream(List<DataGenerator> generators, TimeRange timeRange, Function<Reading,String> xform)
    {
        this.generators = generators;
        this.timeRange = timeRange;
        this.xform = xform;
    }


    /**
     *  Writes a sequence of readings to a writer, potentially switching writers at a set interval.
     *
     *  Note: this method does not close the writer. Caller must close the writer when switch function
     *  is invoked, as well as after this method returns.
     *
     *  @param writer                   Initial output destination. If <code>null</code>, the switch
     *                                  function will be called before starting.
     *  @param writerSwitchInterval     Number of milliseconds (in reading time) between calls to the
     *                                  writer switch function. This can be used to direct output to
     *                                  a different file (for example, breaking stream into daily files).
     *  @param writerSwitchFn           A function that accepts a timestamp and returns a new writer for
     *                                  subsequent output.
     */
    public void writeDirectly(Writer writer, long writerSwitchInterval, BiFunction<Writer,Long,Writer> writerSwitchFn)
    {
        logger.debug("starting");

        if (writer == null)
        {
            writer = writerSwitchFn.apply(writer, timeRange.start);
        }

        PrintWriter out = new PrintWriter(writer);
        for (Long ts : timeRange)
        {
            long timestamp = ts;
            if ((timestamp % writerSwitchInterval) < 1000)
            {
                // switch function is responsible for logging
                out.flush();
                out = new PrintWriter(writerSwitchFn.apply(writer, ts));
            }
            for (DataGenerator generator : generators)
            {
                out.println(xform.apply(generator.next(timestamp)));
            }
        }
        out.flush();
        logger.debug("finished");
    }
    
    
    /**
     *  Writes content in a background thread using a piped writer, so that the content can be passed to
     *  something on the main thread that expects a reader. Logs progress at the specified interval, and
     *  closes the writer after completion (this is necessary to trigger writer end-of-file).
     */
    public Reader writePiped(long logInterval)
    throws IOException
    {
        final PipedOutputStream pos = new PipedOutputStream();
        final PrintWriter out = new PrintWriter(new OutputStreamWriter(pos));
        final PipedInputStream pis = new PipedInputStream(pos);
                
        new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                logger.debug("generating readings");
                writeDirectly(out, logInterval, (writer, timestamp) -> {
                    logger.debug("data timestamp: {}", Instant.ofEpochMilli(timestamp));
                    return writer;
                });
                IOUtil.closeQuietly(out);
                logger.debug("all readings generated");
            }
        }).start();
        return new InputStreamReader(pis, "UTF-8");
    }



}
