package com.chariotsolutions.datagen.temperature;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.kdgcommons.io.IOUtil;

import com.chariotsolutions.datagen.temperature.shared.DataGenerator;
import com.chariotsolutions.datagen.temperature.shared.Reading;
import com.chariotsolutions.datagen.temperature.shared.TimeRange;


/**
 *  Generates time-series data for a Postgres database. Will create the table or write into
 *  an existing table with the correct schema (see {@link createTableIfNeeded}).
 */
public class PopulatePostgres
{
    public static Logger logger = LoggerFactory.getLogger(PopulateS3.class);
    
    public static void main(String[] argv)
    throws Exception
    {        
        if (argv.length != 7)
        {
            System.err.println("invocation:");
            System.err.println("    java -cp ... " + PopulatePostgres.class.getName() + " DBURL DBUSER DBPASSWORD TABLE_NAME NUM_DEVICES START_TIMESTAMP END_TIMESTAMP");
            System.exit(1);
        }
            
        Properties props = new Properties();
        props.setProperty("user", argv[1]);
        props.setProperty("password", argv[2]);
        try (Connection conn = DriverManager.getConnection(argv[0], props))
        {
            logger.info("successfully connected");
            
            String tableName = argv[3];
            int numDevices = Integer.valueOf(argv[4]);
            TimeRange timeRange = new TimeRange(argv[5], argv[6], 1000);
        
            createTableIfNeeded(conn, tableName);
            Reader datastream = createDataStream(numDevices, timeRange);
            performCopy(conn, tableName, datastream);
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
    
    
    private static void createTableIfNeeded(Connection conn, String tableName)
    throws Exception
    {
        logger.info("creating table: {}", tableName);
        String sql = "create table if not exists " + tableName
                   + "("
                   + "    device      text not null,"
                   + "    reported_at timestamp with time zone not null,"
                   + "    reading     decimal(6,3) not null"
                   + ")";
        try (Statement stmt = conn.createStatement())
        {
            stmt.execute(sql);
        }
    }
    
    
    private static void performCopy(Connection conn, String tableName, Reader data)
    throws Exception
    {              
        String sql = "copy " + tableName
                   + " from STDIN"
                   + " (format csv)";
        
        logger.info("starting copy");
        CopyManager cm = new CopyManager((BaseConnection)conn);
        cm.copyIn(sql, data);
        logger.info("copy completed");
    }
}
