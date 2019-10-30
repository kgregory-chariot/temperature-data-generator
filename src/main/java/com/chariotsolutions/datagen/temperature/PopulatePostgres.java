package com.chariotsolutions.datagen.temperature;

import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chariotsolutions.datagen.temperature.shared.DataGenerator;
import com.chariotsolutions.datagen.temperature.shared.DataStream;
import com.chariotsolutions.datagen.temperature.shared.Reading;
import com.chariotsolutions.datagen.temperature.shared.TimeRange;


/**
 *  Generates time-series data for a Postgres database. Will create the table or write into
 *  an existing table with the correct schema (see {@link createTableIfNeeded}).
 */
public class PopulatePostgres
{
    public static Logger logger = LoggerFactory.getLogger(PopulatePostgres.class);


    public static void main(String[] argv)
    throws Exception
    {
        if (argv.length != 7)
        {
            System.err.println("invocation:");
            System.err.println("    java -cp ... " + PopulatePostgres.class.getName() + " DBURL DBUSER DBPASSWORD TABLE_NAME NUM_DEVICES START_TIMESTAMP END_TIMESTAMP");
            System.exit(1);
        }
        
        String tableName = argv[3];
        int numDevices = Integer.valueOf(argv[4]);
        TimeRange timeRange = new TimeRange(argv[5], argv[6], 1000);
        List<DataGenerator> generators = DataGenerator.createGenerators(numDevices, 68, .1);

        Properties props = new Properties();
        props.setProperty("user", argv[1]);
        props.setProperty("password", argv[2]);
        try (Connection conn = DriverManager.getConnection(argv[0], props))
        {
            logger.info("successfully connected");

            createTableIfNeeded(conn, tableName);
            Reader streamOutput = new DataStream(generators, timeRange, Reading::toCSV).writePiped(3600000);
            performCopy(conn, tableName, streamOutput);
        }
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
