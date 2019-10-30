package com.chariotsolutions.datagen.temperature;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.kdgcommons.io.IOUtil;

import com.chariotsolutions.datagen.temperature.shared.DataGenerator;
import com.chariotsolutions.datagen.temperature.shared.TimeRange;


/**
 *  Generates time-series data as a CSV file (an alternative way to load into Postgres).
 */
public class GenerateCSV
{
    public static Logger logger = LoggerFactory.getLogger(GenerateCSV.class);
    
    public static void main(String[] argv)
    throws Exception
    {        
        if (argv.length != 4)
        {
            System.err.println("invocation:");
            System.err.println("    java -cp ... " + GenerateCSV.class.getName() + "FILENAME NUM_DEVICES START_TIMESTAMP END_TIMESTAMP");
            System.exit(1);
        }
         
        String filename = argv[0];
        int numDevices = Integer.valueOf(argv[1]);
        TimeRange timeRange = new TimeRange(argv[2], argv[3], 1000);
        
        try (FileOutputStream fos = new FileOutputStream(filename))
        {
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fos, "UTF-8"));
            BufferedReader in = new BufferedReader(createDataStream(numDevices, timeRange));
            String line = null;
            while ((line = in.readLine()) != null)
            {
                out.write(line);
                out.write('\n');
            }
        }    
    }
    
    private static Reader createDataStream(final int numDevices, final TimeRange timeRange)
    throws Exception
    {
        final List<DataGenerator> generators = DataGenerator.createGenerators(numDevices, 68, .01);
        final PipedOutputStream pos = new PipedOutputStream();
        final PrintWriter out = new PrintWriter(new OutputStreamWriter(pos));
        final PipedInputStream pis = new PipedInputStream(pos);
        
        new Thread(new Runnable() 
        {
            @Override
            public void run() 
            {
                logger.debug("generating readings");
                for (Long timestamp : timeRange)
                {
                    if ((timestamp % 86400000) < 1000)
                    {
                        logger.debug("current timestamp: {}", Instant.ofEpochMilli(timestamp));
                    }
                    for (DataGenerator generator : generators)
                    {
                        out.println(generator.next(timestamp).toCSV());
                    }
                }
                IOUtil.closeQuietly(out);
                logger.debug("all readings generated");
            }
        }).start();
        
        return new InputStreamReader(pis, "UTF-8");
    }
}
