package general;
/*-
 * #%L
 * sparqlQueryTester
 * %%
 * Copyright (C) 2016 QueryAnalysis
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.nio.file.Files.readAllBytes;

import input.InputHandlerParquet;
import input.InputHandlerTSV;
import logging.LoggingHandler;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.queryrender.sparql.SPARQLQueryRenderer;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import query.OpenRDFQueryHandler;
import query.StandardizingSPARQLParser;
import scala.Tuple2;


/**
 * @author jgonsior
 */
public final class Main
{
  /**
   * Saves the encountered queryTypes.
   */
  public static Map<ParsedQuery, String> queryTypes = Collections.synchronizedMap(new HashMap<ParsedQuery, String>());
  /**
   * Saves the mapping of query type and user agent to tool name and version.
   */
  public static Map<Tuple2<String, String>, Tuple2<String, String>> queryTypeToToolMapping = new HashMap<Tuple2<String, String>, Tuple2<String, String>>();
  /**
   * Define a static logger variable.
   */
  private static Logger logger = Logger.getLogger(Main.class);

  /**
   * Since this is a utility class, it should not be instantiated.
   */
  private Main()
  {
    throw new AssertionError("Instantiating utility class Main");
  }

  /**
   * Selects the files to be processed and specifies the files to write to.
   *
   * @param args Arguments to specify runtime behavior.
   */
  public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
  {
    //args = new String[] {"-olt", "-file test/test/test/QueryCntDec", "-n1"};

    Options options = new Options();
    options.addOption("l", "logging", false, "enables file logging");
    options.addOption("o", "openrdf", false, "uses the OpenRDF SPARQL Parser");
    options.addOption("f", "file", true, "defines the input file prefix");
    options.addOption("h", "help", false, "displays this help");
    options.addOption("t", "tsv", false, "reads from .tsv-files");
    options.addOption("p", "parquet", false, "read from .parquet-files");
    options.addOption("n", "numberOfThreads", true, "number of used threads, default 1");

    //some parameters which can be changed through parameters
    //QueryHandler queryHandler = new OpenRDFQueryHandler();
    String inputFilePrefix;
    String inputFileSuffix = ".tsv";
    String queryParserName = "OpenRDF";
    Class inputHandlerClass = null;
    Class queryHandlerClass = null;
    int numberOfThreads = 1;

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
      if (cmd.hasOption("help")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("help", options);
        return;
      }
      if (cmd.hasOption("openrdf")) {
        queryHandlerClass = OpenRDFQueryHandler.class;
      }
      if (cmd.hasOption("tsv")) {
        inputFileSuffix = ".tsv";
        inputHandlerClass = InputHandlerTSV.class;
      }
      if (cmd.hasOption("parquet")) {
        inputFileSuffix = ".parquet";
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("SPARQLQueryAnalyzer").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        inputHandlerClass = InputHandlerParquet.class;
      }
      if (inputHandlerClass == null) {
        System.out.println("Please specify which parser to use, either -t for TSV or -p for parquet.");
      }
      if (cmd.hasOption("file")) {
        inputFilePrefix = cmd.getOptionValue("file").trim();
      } else {
        System.out.println("Please specify at least the file which we should work on using the option '--file PREFIX' or 'f PREFIX'");
        return;
      }
      if (cmd.hasOption("logging")) {
        LoggingHandler.initFileLog(queryParserName, inputFilePrefix);
      }
      if (cmd.hasOption("numberOfThreads")) {
        numberOfThreads = Integer.parseInt(cmd.getOptionValue("numberOfThreads"));
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

    loadPreBuildQueryTypes();

    long startTime = System.nanoTime();

    ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

    for (int day = 1; day <= 31; day++) {
      String inputFile = inputFilePrefix + String.format("%02d", day) + inputFileSuffix;
      Runnable parseOneMonthWorker = new ParseOneMonthWorker(inputFile, inputFilePrefix, inputHandlerClass, queryParserName, queryHandlerClass, day);
      executor.execute(parseOneMonthWorker);
    }
    executor.shutdown();

    while (!executor.isTerminated()) {
      //wait until all workers are finished
    }

    writeQueryTypes(inputFilePrefix);

    long stopTime = System.nanoTime();
    long millis = TimeUnit.MILLISECONDS.convert(stopTime - startTime, TimeUnit.NANOSECONDS);
    Date date = new Date(millis);
    System.out.println("Finished executing with all threads: " + new SimpleDateFormat("mm-dd HH:mm:ss:SSSSSSS").format(date));
  }

  /**
   * Loads all pre-build query types.
   */
  private static void loadPreBuildQueryTypes()
  {
    try (DirectoryStream<Path> directoryStream =
        Files.newDirectoryStream(Paths.get("preBuildQueryTypeFiles"))) {
      for (Path filePath : directoryStream) {
        if (Files.isRegularFile(filePath)) {
          if (filePath.toString().endsWith(".preBuildQueryType")) {
            String queryString = new String(readAllBytes(filePath));
            OpenRDFQueryHandler queryHandler = new OpenRDFQueryHandler();
            queryHandler.setValidityStatus(1);
            queryHandler.setQueryString(queryString);
            ParsedQuery normalizedPreBuildQuery = queryHandler.getNormalizedQuery();
            String queryTypeName = filePath.toString().substring(filePath.toString().lastIndexOf("/") + 1, filePath.toString().lastIndexOf("."));
            if (normalizedPreBuildQuery != null) {
              queryTypes.put(normalizedPreBuildQuery, queryTypeName);
            } else {
              logger.info("Pre-build query " + queryTypeName + " could not be parsed.");
            }
          }
          if (filePath.toString().endsWith(".tsv")) {
            TsvParserSettings parserSettings = new TsvParserSettings();
            parserSettings.setLineSeparatorDetectionEnabled(true);
            parserSettings.setHeaderExtractionEnabled(true);
            parserSettings.setSkipEmptyLines(true);
            parserSettings.setReadInputOnSeparateThread(true);

            ObjectRowProcessor rowProcessor = new ObjectRowProcessor()
            {
              @Override
              public void rowProcessed(Object[] row, ParsingContext parsingContext)
              {
                if (row.length <= 1) {
                  logger.warn("Ignoring line without tab while parsing.");
                  return;
                }
                if (row.length == 5) {
                  queryTypeToToolMapping.put(new Tuple2<>(row[0].toString(), row[1].toString()), new Tuple2<>(row[2].toString(), row[3].toString()));
                  return;
                }
                logger.warn("Line with row length " + row.length + " found. Is the formatting of toolMapping.tsv correct?");
                return;
              }

            };

            parserSettings.setProcessor(rowProcessor);

            TsvParser parser = new TsvParser(parserSettings);

            parser.parse(filePath.toFile());
          }
        }

      }

    }
    catch (IOException e) {
      logger.error("Could not read from directory inputData/queryType/premadeQueryTypeFiles", e);
    }
  }

  /**
   * Writes all found query Types to queryType/queryTypeFiles/.
   * @param inputFilePrefix The location of the input data
   */
  private static void writeQueryTypes(String inputFilePrefix)
  {
    String outputFolderName = inputFilePrefix.substring(0, inputFilePrefix.lastIndexOf('/') + 1) + "queryType/";
    new File(outputFolderName).mkdir();
    outputFolderName += "queryTypeFiles/";
    File outputFolderFile = new File(outputFolderName);
    FileUtils.deleteQuietly(outputFolderFile);
    new File(outputFolderName).mkdir();
    SPARQLQueryRenderer renderer = new SPARQLQueryRenderer();
    String currentOutputFolderName = outputFolderName;
    for (ParsedQuery parsedQuery : queryTypes.keySet()) {

      String queryType = queryTypes.get(parsedQuery);
      try (BufferedWriter bw = new BufferedWriter(new FileWriter(currentOutputFolderName + queryType + ".queryType"))) {
        bw.write(renderer.render(parsedQuery));
        bw.write("\n" + parsedQuery.toString());
      } catch (IOException e) {
        logger.error("Could not write the query type " + queryType + ".", e);
      } catch (Exception e) {
        logger.error("Error while rendering query type " + queryType + ".", e);
      }
    }
  }
}

