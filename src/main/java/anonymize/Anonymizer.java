package anonymize;

import general.Main;
import general.ParseOneDayWorker;
import input.InputHandler;
import input.factories.InputHandlerTSVFactory;
import logging.LoggingHandler;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import output.factories.OutputHandlerAnonymizerFactory;
import query.factories.OpenRDFQueryHandlerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author adrian
 */
public class Anonymizer
{

  /**
   * A list containing all datatypes excluded from anonymization.
   */
  public static final List<String> whitelistedDatatypes = new ArrayList<String>();

  /**
   * Define a static logger variable.
   */
  private static final Logger logger = Logger.getLogger(Anonymizer.class);

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

    int numberOfThreads = 1;

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
        File failedQueriesFolder = new File(outputFolder + "failedQueriesFolder/");
        failedQueriesFolder.mkdir();
      } else {
        System.out.println("Please specify the directory which we should work on using the option '--workingDirectory DIRECTORY' or '-w DIRECTORY'");
        return;
      }
    } catch (UnrecognizedOptionException e) {
      System.out.println("Unrecognized commandline option: " + e.getOption());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("help", options);
      return;
    } catch (ParseException e) {
      System.out.println("There was an error while parsing your command line input. Did you rechecked your syntax before running?");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("help", options);
      return;
    }

    LoggingHandler.initConsoleLog();
    LoggingHandler.initFileLog("Anonymizer", "nothing");

    Main.loadStandardPrefixes();
    Anonymizer.loadWhitelistDatatypes();

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

      String outputFile = outputFolder + "AnonymousQueryCnt" + String.format("%02d", day);

      try {
        Runnable parseOneMonthWorker = new ParseOneDayWorker(new InputHandlerTSVFactory(), inputFile, new OutputHandlerAnonymizerFactory(), outputFile, new OpenRDFQueryHandlerFactory(), day, false);
        executor.execute(parseOneMonthWorker);
      } catch (IOException e) {
      }
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

  public static void loadWhitelistDatatypes()
  {
    try (BufferedReader reader = new BufferedReader(new FileReader("anonymization/whitelist"))) {
      String line = null;
      while ((line = reader.readLine()) != null) {
        Anonymizer.whitelistedDatatypes.add(line);
      }
    } catch (FileNotFoundException e) {
      logger.error("Could not read the anonymization whitelist.", e);
    } catch (IOException e) {
      logger.error("Could not read the anonymization whitelist.", e);
    }
  }
}
