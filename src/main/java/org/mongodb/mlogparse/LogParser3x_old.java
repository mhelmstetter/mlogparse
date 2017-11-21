package org.mongodb.mlogparse;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class LogParser3x_old extends AbstractLogParser implements LogParser {

    Pattern collectionListDbNamePattern = Pattern.compile("^.*'(.*)'");
    
    
    
    Pattern insertPattern = Pattern.compile("^.{28} [A-Z] COMMAND .* command (\\S++).*command: insert \\{.*\\} .* (\\d+)ms$");
    
    //2017-08-11T12:00:27.523-0600 I COMMAND  [conn45040] command pipeline.$cmd command: delete { delete: "queue", deletes: [ { q: { _id: "31bb8851-3dde-4250-8c14-815b727e3ae1" }, limit: 1 } ], ordered: true, shardVersion: [ Timestamp 7000|1, ObjectId('59378d2d394213f2b7f4911e') ] } numYields:0 reslen:221 locks:{ Global: { acquireCount: { r: 2, w: 2 } }, Database: { acquireCount: { w: 2 } }, Collection: { acquireCount: { w: 1 } }, Metadata: { acquireCount: { w: 1 } }, oplog: { acquireCount: { w: 1 } } } protocol:op_command 0ms
    //Pattern commandPattern = Pattern.compile("^.{28} [A-Z] COMMAND .+ command (\\S++).*command: (\\S++) \\{ \\S*: (.*) (\\d+)ms$");
      Pattern commandPattern = Pattern.compile("^.{28} [A-Z] COMMAND .+ command (\\S++).*command: (\\S++) \\{ \\S*:.+?\"?(\\S+?)??\"?,? (.*) (\\d+)ms$");
      //                                        ^.{28} [A-Z] COMMAND .+ command (\\S++).*command: (\\S++) \\{ \\S*:.+?\"??(\\S+)??\"?? (.*)(\\d+)ms$
    
    // 2017-08-11T12:00:30.067-0600 I COMMAND  [conn43331] getmore local.oplog.rs query: { ts: { $gte: Timestamp 1502380885000|3 } } planSummary: COLLSCAN cursorid:18282409613 ntoreturn:0 keysExamined:0 docsExamined:0 numYields:1 nreturned:0 reslen:20 locks:{ Global: { acquireCount: { r: 6 } }, Database: { acquireCount: { r: 3 } }, oplog: { acquireCount: { r: 3 } } } 1000ms
    //Pattern getMorePattern = Pattern.compile("^.{28} [A-Z] COMMAND .+ getmore (\\S++).*");
    Pattern getMorePattern = Pattern.compile("^.{28} [A-Z] COMMAND .+ getmore (\\S++).*query: \\{.+\\} planSummary: (.+)keysExamined:(\\d+) docsExamined:(\\d+).*(\\d+)ms$");
    
    //2017-11-08T15:30:07.465-0800 I WRITE    [conn1] update test.foo appName: "MongoDB Shell" query: { x: { $lte: 1000.0 } } planSummary: COLLSCAN update: { $set: { y: 102.0 } } keysExamined:0 docsExamined:1000 nMatched:1000 nModified:1000 numYields:8 locks:{ Global: { acquireCount: { r: 9, w: 9 } }, Database: { acquireCount: { w: 9 } }, Collection: { acquireCount: { w: 9 } } } 16ms
    //2017-11-08T16:26:54.393-0800 I WRITE    [conn1] remove test.foo appName: "MongoDB Shell" query: { x: { $lte: 1000.0 } } planSummary: COLLSCAN keysExamined:0 docsExamined:1000 ndeleted:1000 keysDeleted:1000 numYields:8 locks:{ Global: { acquireCount: { r: 9, w: 9 } }, Database: { acquireCount: { w: 9 } }, Collection: { acquireCount: { w: 9 } } } 20ms
    // ^.{28} [A-Z] WRITE .+ (update|remove) (\S++).*query: (\{.+\}) (update: .*)?+.* (\d+)ms$
    
    // works
    //Pattern writePattern = Pattern.compile("^.{28} [A-Z] WRITE .+ (update|remove) (\\S++).+query: (\\{.*?\\}+)\\s*(planSummary: .*?)?\\s*(update: .*)?? keysExamined:(\\d+) docsExamined:(\\d+) .* (\\d+)ms$");
    
    Pattern writePattern = Pattern.compile("^.{28} [A-Z] WRITE .+ (update|remove) (\\S++).+query: (\\{.*?\\}+)\\s*(planSummary: .+)?\\s*?(update: .+)?? keysExamined:(\\d+) docsExamined:(\\d+) .+ (\\d+)ms$");
    
    Pattern writePattern2 = Pattern.compile("^.{28} [A-Z] WRITE .+ (update|remove) (\\S++).+query: (\\{.*?\\}+)\\s*(.+) (\\d+)ms$");
    
    Pattern jsonPattern = Pattern.compile(".+keysExamined:(\\d+) docsExamined:(\\d+)");
    
    private StringBuilder lineBuffer = null;
    
    public LogParser3x_old() {
        super();
    }

    public void readFile() throws IOException, ParseException {
        BufferedReader in = new BufferedReader(new FileReader(file));
        JSONParser parser = new JSONParser();
        
        int unmatchedCount = 0;
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
            
            String pre = currentLine.substring(31, 34);

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
                        String s1 = m.group(pos++);
                        String s2 = m.group(pos++);
                        String execTimeStr = m.group(pos++);
                        Integer execTime = Integer.parseInt(execTimeStr);
                        
                        Integer keysExamined = null;
                        Integer docsExamined = null;
                        
                        // Fix namespace so that it's not dbname.$cmd
                        if (command.equals("update") || command.equals("delete")) {
                            namespace = namespace.replaceFirst("\\$cmd", s1);
                        }
                        
                        if (command.startsWith("find") || command.equals("getMore") || command.equals("count") || command.equals("aggregate") || command.equals("update")) {
                            Matcher m2 = jsonPattern.matcher(s2);
                            if (m2.find()) {
                                //commands.add(command);
                                String keysStr = m2.group(1);
                                String docsStr = m2.group(2);
                                keysExamined = Integer.parseInt(keysStr);
                                docsExamined = Integer.parseInt(docsStr);
                            }
                        }
                        
                        accumulator.accumulate(file, command, namespace, execTime, keysExamined, docsExamined, null);
                        
                        continue;
                    }
                    
                    m = getMorePattern.matcher(currentLine);
                    if (m.find()) {
                        int pos = 1;
                        String namespace = m.group(pos++);
                        String planDetails = m.group(pos++);
                        String keysStr = m.group(pos++);
                        String docsStr = m.group(pos++);
                        String execTimeStr = m.group(pos++);
                        Integer execTime = Integer.parseInt(execTimeStr);
                        Integer keysExamined = Integer.parseInt(keysStr);
                        Integer docsExamined = Integer.parseInt(docsStr);
                        accumulator.accumulate(file, GETMORE, namespace, execTime, keysExamined, docsExamined, null);
                        
                        continue;
                    }
                    
                    if (!currentLine.endsWith("ms")) {
                        lineBuffer = new StringBuilder(currentLine.trim());
                        continue;
                    }
                    System.out.println(currentLine);
                    unmatchedCount++;


                } else if (pre.equals("WRI")) {
                    if (writeParse2()) {
                        continue;
                    }
                    
                    if (!currentLine.endsWith("ms")) {
                        lineBuffer = new StringBuilder(currentLine.trim());
                        continue;
                    }
                    System.out.println(currentLine);
                    unmatchedCount++;
                    
                }
                
            } catch (Exception e) {
                logger.warn(String.format("Error at line %s", lineNum), e);
                System.out.println(currentLine);
            }

        }
        in.close();
        long end = System.currentTimeMillis();
        long dur = (end - start);
        System.out.println("Elapsed millis: " + dur);
        
//        for (String command : commands) {
//            System.out.println(command);
//        }
    }
    
    
    public boolean writeParse() {
        Matcher m = writePattern.matcher(currentLine);
        if (m.find()) {
            int pos = 1;
            String cmd = m.group(pos++);
            String namespace = m.group(pos++);
            String query = m.group(pos++);
            String planSummary = m.group(pos++);
            String update = m.group(pos++);
            String keysStr = m.group(pos++);
            String docsStr = m.group(pos++);
            String execTimeStr = m.group(pos++);
            Integer execTime = Integer.parseInt(execTimeStr);
            
            Integer keysExamined = Integer.parseInt(keysStr);
            Integer docsExamined = Integer.parseInt(docsStr);
           
            String command = null;
            if (cmd.equals("update")) {
                command = UPDATE_W;
            } else if (cmd.equals("remove")) {
                command = DELETE_W;
            }
            accumulator.accumulate(file, command, namespace, execTime, keysExamined, docsExamined, null);
            return true;
        }
        return false;
    }
    
    public boolean writeParse2() {
        Matcher m = writePattern2.matcher(currentLine);
        if (m.find()) {
            int pos = 1;
            String cmd = m.group(pos++);
            String namespace = m.group(pos++);
            String query = m.group(pos++);
            String other = m.group(pos++);
            String execTimeStr = m.group(pos++);
            Integer execTime = Integer.parseInt(execTimeStr);
            
            Matcher m2 = jsonPattern.matcher(other);
            Integer keysExamined = null;
            Integer docsExamined = null;
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
            return true;
        }
        return false;
    }


}