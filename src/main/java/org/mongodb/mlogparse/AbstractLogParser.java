package org.mongodb.mlogparse;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractLogParser implements LogParser {

    protected static final Logger logger = LoggerFactory.getLogger(LogParserApp.class);
    protected String currentLine = null;
    protected String[] fileNames;
    //protected File file;
    
    public static final String FIND = "find";
    public static final String FIND_AND_MODIFY = "findAndModify";
    public static final String UPDATE = "update";
    public static final String INSERT = "insert";
    public static final String DELETE = "delete";
    public static final String DELETE_W = "delete_w";
    public static final String COUNT = "count";
    public static final String UPDATE_W = "update_w";
    public static final String GETMORE = "getMore";
    
    protected Accumulator accumulator;
    
    protected int unmatchedCount = 0;
    
    public void read()  throws IOException, ParseException {
        for (String fileName : fileNames) {
            File f = new File(fileName);
            read(f);
        }
    }
    
 


    protected Object parseJson(BufferedReader in) throws ParseException, IOException {
        JSONParser parser = new JSONParser();
        String json = readJson(in);
        //System.out.println("json: " + json);
        return parser.parse(json);
    }

    private String readJson(BufferedReader in) throws IOException {
        StringBuilder sb = new StringBuilder();
        while ((currentLine = in.readLine()) != null) {
            if (currentLine.length() == 0) {
                continue;
            }
            // System.out.println("-- " + currentLine);
            if (currentLine.startsWith("}") || currentLine.startsWith("]")) {
                sb.append(currentLine);
                break;
            }
            if (currentLine.startsWith("{") && currentLine.endsWith("}")) {
                sb.append(currentLine);
                break;
            }
            if (currentLine.startsWith("[") && currentLine.endsWith("]")) {
                sb.append(currentLine);
                break;
            }
    
            sb.append(currentLine);
    
        }
        return sb.toString();
    }

    
    public Accumulator getAccumulator() {
        return accumulator;
    }

    public void report() {
        accumulator.report();
    }

    public AbstractLogParser() {
        super();
        accumulator = new Accumulator();
    }

    public String[] getFileNames() {
        return fileNames;
    }

    public void setFileNames(String[] fileNames) {
        this.fileNames = fileNames;
    }
    
    public int getUnmatchedCount() {
        return unmatchedCount;
    }

}