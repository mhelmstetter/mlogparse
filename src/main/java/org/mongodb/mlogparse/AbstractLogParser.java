package org.mongodb.mlogparse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractLogParser implements LogParser {

    protected static final Logger logger = LoggerFactory.getLogger(LogParserApp.class);
    protected String currentLine = null;
    protected String fileName;
    protected String dirName;
    protected File file;
    
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
    
    
    
    public void read()  throws IOException, ParseException {
        if (fileName != null) {
            file = new File(fileName);
            readFile();
        } else if (dirName != null) {
            File dir = new File(dirName);
            File[] files = dir.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().contains(".log");
                }
            });
            for (File f : files) {
                file = f;
                System.out.println("Reading " + file);
                readFile();
            }
        }
    }
    
    public abstract void readFile() throws IOException, ParseException;


    protected Object parseJson(BufferedReader in) throws ParseException, IOException {
        JSONParser parser = new JSONParser();
        String json = readJson(in);
        System.out.println("json: " + json);
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

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getDirName() {
        return dirName;
    }

    public void setDirName(String dirName) {
        this.dirName = dirName;
    }

}