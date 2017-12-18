package org.mongodb.mlogparse;

import java.io.File;
import java.io.IOException;

import org.json.simple.parser.ParseException;

public interface LogParser {

    void read(File file) throws IOException, ParseException;

    void report();

    Accumulator getAccumulator();
    
    int getUnmatchedCount();

}