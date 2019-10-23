package com.chariotsolutions.datagen.temperature;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.chariotsolutions.datagen.temperature.shared.DataGenerator;
import com.chariotsolutions.datagen.temperature.shared.Reading;
import com.chariotsolutions.datagen.temperature.shared.TimeRange;

/**
 *  Generates a series of S3 files containing raw JSON data. These files are stored
 *  in the name bucket, with key composed of the provided prefix and an hour-resolution
 *  timestamp.
 */
public class PopulateS3
{
    public static Logger logger = LoggerFactory.getLogger(PopulateS3.class);
    
    
    public static void main(String[] argv)
    throws Exception
    {
        if (argv.length != 5)
        {
            System.err.println("invocation:");
            System.err.println("    java -cp ... " + PopulateS3.class.getName() + " BUCKET PREFIX NUM_DEVICES START_TIMESTAMP END_TIMESTAMP");
            System.exit(1);
        }
        
        S3Writer writer = new S3Writer(argv[0], argv[1]);
        int numDevices = Integer.valueOf(argv[2]);
        TimeRange timeRange = new TimeRange(argv[3], argv[4], 1000);
        
        List<String> deviceIds = new ArrayList<String>(numDevices);
        List<DataGenerator> generators = new ArrayList<DataGenerator>(numDevices);
        for (int ii = 0 ; ii < numDevices ; ii++)
        {
            String deviceId = UUID.randomUUID().toString().replace("-", "");
            deviceIds.add(deviceId);
            generators.add(new DataGenerator(deviceId, 68, .05));
        }
        
        logger.info("device IDs: " + deviceIds);
        for (Long timestamp : timeRange)
        {
            for (DataGenerator gen : generators)
            {
                writer.write(timestamp, gen.next(timestamp, 10));
            }
        }
        
        writer.close();
    }
    
    
    private static class S3Writer
    {
        private String bucket;
        private String prefix;
        
        private Instant currentTimestampHour;
        private String currentFilename;
        private File currentFile;
        private BufferedWriter currentOut;
        
        private TransferManager xferManager;
        
        
        public S3Writer(String bucket, String prefix)
        {
            this.bucket = bucket;
            this.prefix = prefix.endsWith("/") ? prefix : prefix + "/";
        }
        
        
        public void write(long timestamp, Reading reading)
        throws Exception
        {
            switchFilesIfNeeded(timestamp);
            currentOut.write(reading.toJson());
            currentOut.write("\n");
        }
        
        
        public void close()
        throws Exception
        {
            writeCurrentFileToS3();
        }
        
        
        private void switchFilesIfNeeded(long timestamp)
        throws Exception
        {
            Instant truncatedNow = Instant.ofEpochMilli(timestamp).truncatedTo(ChronoUnit.HOURS);
            if (truncatedNow.equals(currentTimestampHour))
                return;
            
            writeCurrentFileToS3();
            
            currentTimestampHour = truncatedNow;
            currentFilename = truncatedNow.toString().replaceAll(":.*", "") + ".json";
            currentFile = File.createTempFile(currentFilename, ".tmp");
            
            logger.info("creating temprary file: " + currentFile);
            currentOut = new BufferedWriter(
                            new OutputStreamWriter(
                                new FileOutputStream(currentFile),
                                "UTF-8"));
        }
        
        
        private void writeCurrentFileToS3()
        throws Exception
        {
            if (currentOut == null)
                return;
            
            if (xferManager == null)
                xferManager = TransferManagerBuilder.defaultTransferManager();
            
            String dest = prefix + currentFilename;
            logger.info("writing file to s3: " + dest);
            currentOut.close();
            Upload upload = xferManager.upload(bucket, dest, currentFile);
            upload.waitForCompletion();
            
            currentFile.delete();
        }
    }
}
