package org.mongodb.mlogparse;

import java.io.File;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class LogLineAccumulator {
    
    private String namespace;
    private String operation;
    private File file;
    
    private long count;
    private long total;
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    
    private long totalKeysExamined;
    private long totalDocsExamined;
    
    private long totalReturned = 0;
    
    
    DescriptiveStatistics executionStats = new DescriptiveStatistics();
    
    DescriptiveStatistics keysExaminedStats = new DescriptiveStatistics();
    DescriptiveStatistics docsExaminedStats = new DescriptiveStatistics();
    
    
    public LogLineAccumulator(File file, String command, String namespace2) {
        this.namespace = namespace2;
        this.operation = command;
        this.file = file;
    }

    public void addExecution(long amt) {
        count++;
        total += amt;
        if (amt > max) {
            max = amt;
        }
        if (amt < min) {
            min = amt;
        }
        executionStats.addValue(amt);
    }
    
    public String getKeysExaminedPercentile95() {
        if (totalKeysExamined > 0) {
            return String.format("%10.0f", keysExaminedStats.getPercentile(95));
        }
        return "0";
    }
    
    public String getDocsExaminedPercentile95() {
        if (totalDocsExamined > 0) {
            return String.format("%10.0f", docsExaminedStats.getPercentile(95));
        }
        return "0";
    }
    
    public double getPercentile95() {
        return executionStats.getPercentile(95);
    }
    
    public long getAvgDocsExamined() {
        return totalDocsExamined/count;
    }
    
    public long getTotalDocsExamined() {
        return totalDocsExamined;
    }
    
    public String toString() {
        return String.format("%-55s %-15.15s %10d %10d %10d %10d %10.0f %10d %10d %10d %10s %10s %12.1f %12.1f %10d %10d", namespace, operation, count, 
                min, max, total/count, getPercentile95(), total/1000,
                totalKeysExamined/count, totalDocsExamined/count, 
                getKeysExaminedPercentile95(), getDocsExaminedPercentile95(), totalKeysExamined/1000.0, totalDocsExamined/1000.0, totalReturned/count, getScannedReturnRatio());
        
    }
    
    

    public long getScannedReturnRatio() {
        if (totalReturned > 0) {
            return totalDocsExamined/totalReturned;
        }
        return 0;
    }

    public void addExamined(Integer keysExamined, Integer docsExamined) {
        totalDocsExamined += docsExamined;
        totalKeysExamined += keysExamined;
        keysExaminedStats.addValue(keysExamined);
        docsExaminedStats.addValue(docsExamined);
    }
    
    public void addReturned(Integer nReturned) {
        totalReturned += nReturned;
    }

    public long getCount() {
        return count;
    }
    
    public long getMax() {
        return max;
    }

    public long getAvg() {
        return total/count;
    }

    public long getAvgReturned() {
        return totalReturned/count;
    }

}
