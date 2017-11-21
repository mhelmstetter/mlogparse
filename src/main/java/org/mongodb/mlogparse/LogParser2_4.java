package org.mongodb.mlogparse;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.parser.ParseException;

public class LogParser2_4 extends AbstractLogParser implements LogParser {

    Pattern insertPattern = Pattern.compile("^.* \\[.*\\] insert (\\S+) .* (\\d+)ms");
    Pattern updatePattern = Pattern.compile("^.* \\[.*\\] update (\\S+) .* (\\d+)ms");

    public LogParser2_4() {
        super();
    }

    public void readFile() throws IOException, ParseException {
        BufferedReader in = new BufferedReader(new FileReader(file));
        int lineNum = 0;
        while ((currentLine = in.readLine()) != null) {
            lineNum++;
            if (currentLine.length() == 0) {
                continue;
            }

            try {
                Matcher m = insertPattern.matcher(currentLine);
                if (m.find()) {
                    int pos = 1;
                    String namespace = m.group(pos++);
                    String execTimeStr = m.group(pos++);
                    Integer execTime = Integer.parseInt(execTimeStr);
                    accumulator.accumulate(file, INSERT, namespace, execTime);
                    continue;
                }
                m = updatePattern.matcher(currentLine);
                if (m.find()) {
                    int pos = 1;
                    String namespace = m.group(pos++);
                    String execTimeStr = m.group(pos++);
                    Integer execTime = Integer.parseInt(execTimeStr);
                    accumulator.accumulate(file, UPDATE, namespace, execTime);
                    continue;
                }

            } catch (Exception e) {
                logger.warn(String.format("Error at line %s", lineNum), e);
                System.out.println(currentLine);
            }

        }
        in.close();
    }

}
