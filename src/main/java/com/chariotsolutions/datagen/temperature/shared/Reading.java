package com.chariotsolutions.datagen.temperature.shared;

import java.time.Instant;

import org.apache.commons.text.StringEscapeUtils;

/**
 *  Holds a temperatore reading, and provides a method to convert it to JSON.
 */
public class Reading
{    
    private String deviceId;
    private long timestamp;
    private double temperature;
    
    
    public Reading(String deviceId, long timestamp, double temperature)
    {
        this.deviceId = deviceId;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }
    
    
    public String toJson()
    {
        return "{"
             + "\"device\": "
             + "\""
             + StringEscapeUtils.escapeJson(deviceId)
             + "\", "
             + "\"timestamp\": "
             + timestamp
             + ", "
             + "\"temperature\": "
             + temperature
             + "}";
    }
    
    
    public String toCSV()
    {
        return "\"" + deviceId + "\","
             + Instant.ofEpochMilli(timestamp)
             + ","
             + temperature;
    }
    
     
}
