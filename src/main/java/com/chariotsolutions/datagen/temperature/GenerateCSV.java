package com.chariotsolutions.datagen.temperature;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chariotsolutions.datagen.temperature.shared.DataGenerator;
import com.chariotsolutions.datagen.temperature.shared.DataStream;
import com.chariotsolutions.datagen.temperature.shared.Reading;
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
        
        DataStream datastream = new DataStream(DataGenerator.createGenerators(numDevices, 68.0, 0.1),
                                               new TimeRange(argv[2], argv[3], 1000),
                                               Reading::toCSV);
        
        try (FileOutputStream fos = new FileOutputStream(filename))
        {
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fos, "UTF-8"));
            datastream.writeDirectly(
                out, 
                86400000L, 
                (writer, timestamp) -> {
                    logger.debug("current data timestamp is {}", Instant.ofEpochMilli(timestamp));
                    return writer;
                });
        }    
    }
}
