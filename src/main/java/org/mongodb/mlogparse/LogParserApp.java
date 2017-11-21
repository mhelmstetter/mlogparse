package org.mongodb.mlogparse;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.json.simple.parser.ParseException;

public class LogParserApp {
    
    
    @SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        Options options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(
                OptionBuilder.withArgName("mongod log file").hasArgs().withLongOpt("file").create("f"));
        options.addOption(
                OptionBuilder.withArgName("mongod log directory").hasArgs().withLongOpt("directory").create("d"));
        options.addOption(
                OptionBuilder.withArgName("MongoDB version").hasArgs().withLongOpt("mongoVersion").create("m"));
    
        CommandLineParser parser = new GnuParser();
        CommandLine line = null;
        try {
            line = parser.parse(options, args);
            if (line.hasOption("help")) {
                printHelpAndExit(options);
            }
        } catch (MissingOptionException e) {
            printHelpAndExit(options);
        } catch (Exception e) {
            e.printStackTrace();
            printHelpAndExit(options);
        }
        return line;
    }
    
    private static void printHelpAndExit(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("logParser", options);
        System.exit(-1);
    }

    public static void main(String[] args) throws IOException, ParseException {
        CommandLine line = initializeAndParseCommandLineOptions(args);
        
        String mongoVersion = "3.4";
        
        AbstractLogParser parser = null;
        
        if (line.hasOption("m")) {
            mongoVersion = line.getOptionValue("m");
        }
        if (mongoVersion.startsWith("3.")) {
            parser = new LogParser3x();
        } else if (mongoVersion.startsWith("2.4")) {
            parser = new LogParser2_4();
        } else {
            throw new IllegalArgumentException("Version " + mongoVersion + " not supported");
        }
        
        
        String fileName = line.getOptionValue("f");
        String dirName = line.getOptionValue("d");
        parser.setFileName(fileName);
        parser.setDirName(dirName);
        
        parser.read();
        parser.report();

    }

}
