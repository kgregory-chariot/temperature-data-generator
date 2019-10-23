package com.chariotsolutions.datagen.temperature.shared;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 *  Generates readings using a normal distribution from a desired value.
 */
public class DataGenerator
{
    private Random rnd = new Random();
    
    private String deviceId;
    private double normal;
    private double stdev;
    
    
    public DataGenerator(String deviceId, double normal, double stdev)
    {
        this.deviceId = deviceId;
        this.normal = normal;
        this.stdev = stdev;
    }
    
    
    /**
     *  Returns a reading using the exact timestamp provided.
     */
    public Reading next(long timestamp)
    {
        double temperature = normal + rnd.nextGaussian() * stdev;
        return new Reading(deviceId, timestamp, temperature);
    }
    
    
    /**
     *  Returns a reading using the timestamp, modified by the provided standard
     *  deviation (because in the real world, exact intervals don't exist).
     */
    public Reading next(long timestamp, long tsdev)
    {
        return next(timestamp + (long)(rnd.nextGaussian() * tsdev));
    }
    
    
    /**
     *  Creates multiple generators, using UUID sans dashes as the device ID.
     */
    public static List<DataGenerator> createGenerators(int count, double normal, double stdev)
    {
        List<DataGenerator> result = new ArrayList<DataGenerator>(count);
        for (int ii = 0 ; ii < count ; ii++)
        {
            String deviceId = UUID.randomUUID().toString().replace("-", "");
            result.add(new DataGenerator(deviceId, normal, stdev));
        }
        return result;
    }
}
