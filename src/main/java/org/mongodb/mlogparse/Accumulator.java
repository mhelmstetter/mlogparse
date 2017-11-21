package org.mongodb.mlogparse;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class Accumulator {
    
    private Map<AccumulatorKey, LogLineAccumulator> accumulators = new HashMap<AccumulatorKey, LogLineAccumulator>();
    
    protected void accumulate(File file, String command, String dbName, String collName, Integer execTime) {
        // TODO add an option to accumulate per file, for now glob all files together
        AccumulatorKey key = new AccumulatorKey(null, dbName, collName, command);
        LogLineAccumulator accum = accumulators.get(key);
        if (accum == null) {
            accum = new LogLineAccumulator(null, command, dbName + "." + collName);
            accumulators.put(key, accum);
        }
        accum.addExecution(execTime);
    }
    
    protected void accumulate(File file, String command, String namespace, Integer execTime) {
        accumulate(file, command, namespace, execTime, null, null, null);
    }
    
    protected void accumulate(File file, String command, String namespace, Integer execTime, Integer keysExamined, Integer docsExamined, Integer nReturned) {
        // TODO add an option to accumulate per file, for now glob all files together
        AccumulatorKey key = new AccumulatorKey(null, namespace, command);
        LogLineAccumulator accum = accumulators.get(key);
        if (accum == null) {
            accum = new LogLineAccumulator(null, command, namespace);
            accumulators.put(key, accum);
        }
        
        
        accum.addExecution(execTime);
        
        if (keysExamined != null) {
            accum.addExamined(keysExamined, docsExamined);
        }
        
        if (nReturned != null) {
            accum.addReturned(nReturned);
        }
        
    }
    
    public LogLineAccumulator getAccumulator(AccumulatorKey key) {
        return accumulators.get(key);
    }
    
    
    public void report() {
        System.out.println(String.format("%-55s %-15s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s %12s %12s %10s %10s", "Namespace", "operation", "count", "min_ms", "max_ms", "avg_ms", "95%_ms", "total_s", "avgKeysEx", "avgDocsEx", "95%_keysEx", "95%_DocsEx", "totKeysEx(K)", "totDocsEx(K)", "avgReturn", "exRetRatio"));
        for (LogLineAccumulator acc : accumulators.values()) {
            System.out.println(acc);
        }
        
    }
    

}
