package org.mongodb.mlogparse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang3.StringUtils;
import org.json.simple.parser.ParseException;
import org.mongodb.util.MimeTypes;

public class LogParser3x extends AbstractLogParser implements LogParser {

    Pattern collectionListDbNamePattern = Pattern.compile("^.*'(.*)'");
    
    Pattern insertPattern = Pattern.compile("^.+ [A-Z] COMMAND .* command (\\S++).*command: insert \\{.*\\} .* (\\d+)ms$");
    
    // Possibly only with 3.0 and/or with exceptions
    Pattern insertPattern2 = Pattern.compile("^.+ [A-Z] COMMAND .* insert (\\S++) .*(\\d+)ms$");
    
    Pattern commandPattern = Pattern.compile("^.+ [A-Z] COMMAND .+ command (\\S++).*command: (\\S++) \\{ \\S*: (.*) (\\d+)ms$");
    
    Pattern getmorePattern = Pattern.compile("^.+ [A-Z] COMMAND .+ getmore (\\S++).+query: \\{ \\S*:.+keysExamined:(\\d+) docsExamined:(\\d+) .+ (\\d+)ms$");
    
    Pattern queryPattern = Pattern.compile("^.+ [A-Z] COMMAND .+ query (\\S++).+query: \\{ \\S*:.+keysExamined:(\\d+) docsExamined:(\\d+) .+ (\\d+)ms$");
    
    
    Pattern writePattern = Pattern.compile("^.+ [A-Z] WRITE .+ (update|remove) (\\S++).+query: (\\{.*?\\}+)\\s*(.+) (\\d+)ms$");
    
    Pattern updatePlanInfoPattern = Pattern.compile(".*keysExamined:(\\d+) docsExamined:(\\d+).*");
    Pattern jsonPattern = Pattern.compile(".+keysExamined:(\\d+) docsExamined:(\\d+)\\s*(?:\\w+:\\d+ )*?\\s*(?:nreturned:)?(\\d+)? .+");
    //Pattern jsonPattern = Pattern.compile(".+keysExamined:(\\d+) docsExamined:(\\d+).+");
    
    private StringBuilder lineBuffer = null;
    
    public LogParser3x() {
        super();
    }

    public void read(File file) throws IOException, ParseException {
        
        String guess = MimeTypes.guessContentTypeFromName(file.getName());
        logger.debug(guess);
        
        BufferedReader in = null;
        
        if (guess != null && guess.equals(MimeTypes.GZIP)) {
            FileInputStream fis = new FileInputStream(file);
            GZIPInputStream gzis = new GZIPInputStream(fis);
            in = new BufferedReader(new InputStreamReader(gzis));
        } else if (guess != null && guess.equals(MimeTypes.ZIP)) {
            
        } else {
            in = new BufferedReader(new FileReader(file));
        }
        
        //JSONParser parser = new JSONParser();
        
        unmatchedCount = 0;
        //Set<String> commands = new HashSet<String>();
        
        

        int lineNum = 0;
        long start = System.currentTimeMillis();
        while ((currentLine = in.readLine()) != null) {
            lineNum++;
            if (lineBuffer != null) {
                lineBuffer.append(currentLine.trim());
                if (! currentLine.endsWith("ms")) {
                    continue;
                } else {
                    currentLine = lineBuffer.toString();
                    lineBuffer = null;
                }
            } else if (currentLine.length() < 34) {
                continue;
            }
            
            int logComponentStartIndex;
            if (currentLine.charAt(23) == 'Z') {
                logComponentStartIndex = 27;
            } else {
                logComponentStartIndex = 31;
            }
         
            String pre = currentLine.substring(logComponentStartIndex, logComponentStartIndex+3);

            try {
                if (pre.equals("COM")) {

                    Matcher m = insertPattern.matcher(currentLine);
                    if (m.find()) {
                        int pos = 1;
                        String namespace = m.group(pos++);
                        //String json = m.group(pos++);
                        //String planType = m.group(pos++);
                        String execTimeStr = m.group(pos++);
                        Integer execTime = Integer.parseInt(execTimeStr);

                        accumulator.accumulate(file, INSERT, namespace, execTime);
                        continue;
                    }
                    
                    m = commandPattern.matcher(currentLine);
                    if (m.find()) {
                        int pos = 1;
                        String namespace = m.group(pos++);
                        String command = m.group(pos++);
                        if (command.equals("collStats")) {
                            continue;
                        }
                        //String cmd = m.group(pos++);
                        String json = m.group(pos++);
                        
                        // Fix namespace so that it's not dbname.$cmd
                        if (command.equals("update") || command.equals("delete")) {
                            String c = StringUtils.substringBefore(json, ",");
                            String c2 = c.substring(1, c.length()-1);
                            namespace = namespace.replaceFirst("\\$cmd", c2);
                        }
                        
                        Integer execTime = Integer.parseInt(m.group(pos++));
                        
                        Integer keysExamined = null;
                        Integer docsExamined = null;
                        Integer nReturned = null;
                        
                        // findAndModify, aggregate, getMore, count, find
                        if (command.startsWith("find") || command.equals("getMore") || command.equals("count") || command.equals("aggregate")) {
                            Matcher m2 = jsonPattern.matcher(json);
                            if (m2.find()) {
                                keysExamined = Integer.parseInt(m2.group(1));
                                docsExamined = Integer.parseInt(m2.group(2));
                                
                                String nReturnedStr = m2.group(3);
                                if (nReturnedStr != null) {
                                    nReturned = Integer.parseInt(nReturnedStr);
                                }
                                
                            }
                        }
                        
                        accumulator.accumulate(file, command, namespace, execTime, keysExamined, docsExamined, nReturned);
                        
                        continue;
                    }
                    
                    m = getmorePattern.matcher(currentLine);
                    if (m.find()) {
                        int pos = 1;
                        String namespace = m.group(pos++);
                        Integer keysExamined = Integer.parseInt(m.group(pos++));
                        Integer docsExamined = Integer.parseInt(m.group(pos++));
                        Integer execTime = Integer.parseInt(m.group(pos++));
                        accumulator.accumulate(file, GETMORE, namespace, execTime, keysExamined, docsExamined, null);
                        continue;
                    }
                    
                    if (currentLine.contains("Use of the aggregate command without the")) {
                        continue;
                    }
                    
                    if (!currentLine.endsWith("ms")) {
                        lineBuffer = new StringBuilder(currentLine.trim());
                        continue;
                    }
                    
                    m = queryPattern.matcher(currentLine);
                    if (m.find()) {
                        int pos = 1;
                        String namespace = m.group(pos++);
                        Integer keysExamined = Integer.parseInt(m.group(pos++));
                        Integer docsExamined = Integer.parseInt(m.group(pos++));
                        Integer execTime = Integer.parseInt(m.group(pos++));
                        accumulator.accumulate(file, FIND, namespace, execTime, keysExamined, docsExamined, null);
                        continue;
                    }
                    
                    // odd case put this near the end
                    m = insertPattern2.matcher(currentLine);
                    if (m.find()) {
                        int pos = 1;
                        String namespace = m.group(pos++);
                        //String json = m.group(pos++);
                        //String planType = m.group(pos++);
                        String execTimeStr = m.group(pos++);
                        Integer execTime = Integer.parseInt(execTimeStr);

                        accumulator.accumulate(file, INSERT, namespace, execTime);
                        continue;
                    }
                    
                    logger.warn("Unmatched line in file " + file.getName() + ": "  + currentLine);
                    unmatchedCount++;

                } else if (pre.equals("WRI")) {
                    Matcher m = writePattern.matcher(currentLine);
                    if (m.find()) {
                        int pos = 1;
                        String cmd = m.group(pos++);
                        String namespace = m.group(pos++);
                        
                        String query = m.group(pos++);
                        String planInfo = m.group(pos++);
                        String execTimeStr = m.group(pos++);
                        Integer execTime = Integer.parseInt(execTimeStr);
                        
                        Integer keysExamined = null;
                        Integer docsExamined = null;
                        Matcher m2 = updatePlanInfoPattern.matcher(planInfo);
                        if (m2.find()) {
                            //commands.add(command);
                            String keysStr = m2.group(1);
                            String docsStr = m2.group(2);
                            keysExamined = Integer.parseInt(keysStr);
                            docsExamined = Integer.parseInt(docsStr);
                        }
                        String command = null;
                        if (cmd.equals("update")) {
                            command = UPDATE_W;
                        } else if (cmd.equals("remove")) {
                            command = DELETE_W;
                        }
                        accumulator.accumulate(file, command, namespace, execTime, keysExamined, docsExamined, null);
                        
                        continue;
                    }
                    
                    if (!currentLine.endsWith("ms")) {
                        lineBuffer = new StringBuilder(currentLine.trim());
                        continue;
                    }
                   
                    logger.warn("Unmatched line in file " + file.getName() + ": "  + currentLine);
                    unmatchedCount++;
                }
                
                
            } catch (Exception e) {
                logger.warn(String.format("Error at line %s", lineNum), e);
                //System.out.println(currentLine);
            }

        }
        in.close();
        long end = System.currentTimeMillis();
        long dur = (end - start);
        logger.debug(String.format("Elapsed millis: %s, lineCount: %s, unmatchedCount: %s", dur, lineNum, unmatchedCount));
        
//        for (String command : commands) {
//            System.out.println(command);
//        }
    }


}

