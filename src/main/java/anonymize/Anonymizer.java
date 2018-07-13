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
   * A list containing all strings excluded from anonymization.
   */
  public static final List<String> whitelistedStrings = new ArrayList<String>();
  /**
   * Strings of this length or lower should not be anonymized.
   */
  public static int unanonymizedStringLength;

  /**
   * Define a static logger variable.
   */
  private static final Logger logger = Logger.getLogger(Anonymizer.class);

  public static void main(String[] args) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException
  {
    Options options = new Options();
    options.addOption("l", "logging", false, "Enables file logging.");
    options.addOption("w", "workingDirectory", true, "The directory we should be working on.");
    options.addOption("n", "numberOfThreads", true, "Number of used threads, default 1");
    options.addOption("u", "unanonymizedStringLength", true, "Strings of this length or lower should not be anonymized. Default is ten.");

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
      if (cmd.hasOption("logging")) {
        LoggingHandler.initFileLog("Anonymizer", "nothing");
      }
      if (cmd.hasOption("unanonymizedStringLength")) {
        unanonymizedStringLength = Integer.parseInt(cmd.getOptionValue("unanonymizedStringLength"));
      } else {
        unanonymizedStringLength = 10;
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

    Main.queryTypeMapDbLocation = workingDirectory + "queryTypeMap.db";

    LoggingHandler.initConsoleLog();

    Main.loadStandardPrefixes();
    Main.loadUserAgentRegex();
    Main.loadQueryTypeMap();
    Main.loadPreBuildQueryTypes();

    Anonymizer.loadWhitelistDatatypes();
    Anonymizer.loadWhitelistStrings();

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

    Main.mapDb.close();
    new File(Main.queryTypeMapDbLocation).delete();

    long stopTime = System.nanoTime();
    long millis = TimeUnit.MILLISECONDS.convert(stopTime - startTime, TimeUnit.NANOSECONDS);
    Date date = new Date(millis);
    System.out.println("Finished executing with all threads: " + new SimpleDateFormat("mm-dd HH:mm:ss:SSSSSSS").format(date));
  }

  public static void loadWhitelistDatatypes()
  {
    try (BufferedReader reader = new BufferedReader(new FileReader("anonymization/datatypesWhitelist"))) {
      String line = null;
      while ((line = reader.readLine()) != null) {
        Anonymizer.whitelistedDatatypes.add(line);
      }
    } catch (FileNotFoundException e) {
      logger.error("Could not read the anonymization datatypes whitelist.", e);
    } catch (IOException e) {
      logger.error("Could not read the anonymization datatypes whitelist.", e);
    }
  }

  public static void loadWhitelistStrings()
  {
    try (BufferedReader reader = new BufferedReader(new FileReader("anonymization/stringWhitelist"))) {
      String line = null;
      while ((line = reader.readLine()) != null) {
        Anonymizer.whitelistedStrings.add(line);
      }
    } catch (FileNotFoundException e) {
      logger.error("Could not read the anonymization string whitelist.", e);
    } catch (IOException e) {
      logger.error("Could not read the anonymization string whitelist.", e);
    }
  }
}
