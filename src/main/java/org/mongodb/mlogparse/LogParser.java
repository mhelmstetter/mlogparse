package org.mongodb.mlogparse;

import java.io.IOException;

import org.json.simple.parser.ParseException;

public interface LogParser {

    void read() throws IOException, ParseException;

    void report();

    void setFileName(String absolutePath);

    Accumulator getAccumulator();

}