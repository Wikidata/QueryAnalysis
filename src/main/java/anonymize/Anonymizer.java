package anonymize;

import general.Main;
import general.ParseOneDayWorker;
import input.InputHandler;
import input.InputHandlerTSV;
import logging.LoggingHandler;
import output.OutputHandler;
import output.OutputHandlerTSV;
import query.OpenRDFQueryHandler;
import query.QueryHandler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.log4j.Logger;

/**
 * @author adrian
 *
 */
public class Anonymizer
{

  /**
   * Define a static logger variable.
   */
  private static final Logger logger = Logger.getLogger(Main.class);

  public static void main(String[] args) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException
  {
    Options options = new Options();
    options.addOption("w", "workingDirectory", true, "The directory we should be working on.");
    options.addOption("n", "numberOfThreads", true, "Number of used threads, default 1");

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;

    String workingDirectory;
    String inputFilePrefix;
    String inputFileSuffix = ".tsv.gz";
    String outputFolder;

    String queryParserName = "OpenRDF";

    int numberOfThreads = 1;

    Class inputHandlerClass = InputHandlerTSV.class;
    Class queryHandlerClass = OpenRDFQueryHandler.class;

    try {
      cmd = parser.parse(options, args);
      if (cmd.hasOption("help")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("help", options);
        return;
      }
      if (cmd.hasOption("numberOfThreads")) {
        numberOfThreads = Integer.parseInt(cmd.getOptionValue("numberOfThreads"));
      }
      if (cmd.hasOption("workingDirectory")) {
        workingDirectory = cmd.getOptionValue("workingDirectory").trim();
        if (!workingDirectory.endsWith("/")) {
          workingDirectory += "/";
        }
        inputFilePrefix = workingDirectory + "rawLogData/QueryCnt";

        outputFolder = workingDirectory + "anonymousRawData/";
        File outputFolderFile = new File(outputFolder);
        outputFolderFile.mkdir();
      } else {
        System.out.println("Please specify the directory which we should work on using the option '--workingDirectory DIRECTORY' or '-w DIRECTORY'");
        return;
      }
    }
    catch (UnrecognizedOptionException e) {
      System.out.println("Unrecognized commandline option: " + e.getOption());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("help", options);
      return;
    }
    catch (ParseException e) {
      System.out.println("There was an error while parsing your command line input. Did you rechecked your syntax before running?");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("help", options);
      return;
    }

    LoggingHandler.initConsoleLog();
    LoggingHandler.initFileLog("Anonymizer", "nothing");

    Main.loadStandardPrefixes();

    File lockFile;

    try {
      lockFile = new File(workingDirectory + "locked");

      if (!lockFile.createNewFile()) {
        logger.info("Cannot work on " + workingDirectory + " because another instance is already working on it.");
        return;
      }
    } catch (IOException e) {
      logger.error("Unexpected error while trying to create the lock file.", e);
      return;
    }

    long startTime = System.nanoTime();

    ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

    for (int day = 1; day <= 31; day++) {
      String inputFile = inputFilePrefix + String.format("%02d", day) + inputFileSuffix;
      InputHandler inputHandler;
      try {
        inputHandler = new InputHandlerTSV(inputFile);
      }
      catch (IOException e) {
        logger.warn("File " + inputFile + " could not be found.");
        continue;
      }

      QueryHandler queryHandler = new OpenRDFQueryHandler();

      String outputFile = outputFolder + "/AnonymousQueryCnt" + String.format("%02d", day);
      OutputHandler outputHandler;
      try {
        outputHandler = new OutputHandlerAnonymizer(outputFile, queryHandler.getClass());
      }
      catch (FileNotFoundException e) {
        logger.error("File " + outputFile + "could not be created or written to.", e);
        continue;
      }

      Runnable parseOneMonthWorker = new ParseOneDayWorker(inputHandler, outputHandler, day, false);
      executor.execute(parseOneMonthWorker);
    }
    executor.shutdown();

    while (!executor.isTerminated()) {
      //wait until all workers are finished
    }

    lockFile.delete();

    long stopTime = System.nanoTime();
    long millis = TimeUnit.MILLISECONDS.convert(stopTime - startTime, TimeUnit.NANOSECONDS);
    Date date = new Date(millis);
    System.out.println("Finished executing with all threads: " + new SimpleDateFormat("mm-dd HH:mm:ss:SSSSSSS").format(date));
  }
}
