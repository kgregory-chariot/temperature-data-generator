package com.chariotsolutions.datagen.temperature;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.chariotsolutions.datagen.temperature.shared.DataGenerator;
import com.chariotsolutions.datagen.temperature.shared.DataStream;
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

        S3Writer s3Writer = new S3Writer(argv[0], argv[1]);
        int numDevices = Integer.valueOf(argv[2]);
        TimeRange timeRange = new TimeRange(argv[3], argv[4], 1000);

        List<DataGenerator> generators = DataGenerator.createGenerators(numDevices, 68, 0.1);
        logger.info("device IDs: " + DataGenerator.extractDeviceIds(generators));

        new DataStream(generators, timeRange, Reading::toJson)
            .writeDirectly(null, 3600 * 1000, (writer, timestamp) -> s3Writer.uploadCurrentFile(timestamp));

        s3Writer.close();
    }


    private static class S3Writer
    {
        private String bucket;
        private String prefix;

        private String currentFilename;
        private File currentFile;
        private BufferedWriter currentWriter;

        private TransferManager xferManager;


        public S3Writer(String bucket, String prefix)
        {
            this.bucket = bucket;
            this.prefix = prefix.endsWith("/") ? prefix : prefix + "/";
        }


        public Writer uploadCurrentFile(long timestamp)
        {
            try
            {
                writeCurrentFileToS3();
                currentFilename = Instant.ofEpochMilli(timestamp).toString().replaceAll(":.*", "") + ".json";
                currentFile = File.createTempFile(currentFilename, ".tmp");
                currentWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(currentFile), "UTF-8"));
                return currentWriter;
            }
            catch (Exception ex)
            {
                throw new RuntimeException("failed to upload file", ex);
            }

        }


        public void close()
        throws Exception
        {
            writeCurrentFileToS3();
        }


        private void writeCurrentFileToS3()
        throws Exception
        {
            if (currentWriter == null)
                return;

            if (xferManager == null)
                xferManager = TransferManagerBuilder.defaultTransferManager();

            String dest = prefix + currentFilename;
            logger.info("writing file to s3: " + dest);
            currentWriter.close();
            Upload upload = xferManager.upload(bucket, dest, currentFile);
            upload.waitForCompletion();

            currentFile.delete();
        }
    }
}
