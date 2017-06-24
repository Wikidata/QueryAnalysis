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


import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import input.InputHandler;
import input.InputHandlerParquet;
import input.InputHandlerTSV;
import logging.LoggingHandler;
import openrdffork.TupleExprWrapper;

import org.apache.commons.cli.*;
import org.apache.commons.collections.BidiMap;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import query.OpenRDFQueryHandler;
import scala.Tuple2;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.nio.file.Files.readAllBytes;


/**
 * @author jgonsior
 */
public final class Main
{
  /**
   * Saves the mapping of properties to groups.
   */
  public static final Map<String, Set<String>> propertyGroupMapping = new HashMap<String, Set<String>>();
  /**
   * Saves the premade queryTypes.
   */
  public static final Map<TupleExprWrapper, String> queryTypes = new HashMap<TupleExprWrapper, String>();
  /**
   * Saves the mapping of query type and user agent to tool name and version.
   */
  public static final Map<Tuple2<String, String>, Tuple2<String, String>> queryTypeToToolMapping = new HashMap<>();
  /**
   * Saves the regular expressions used to identify user queries (via userAgent).
   */
  public static final List<String> userAgentRegex = new ArrayList<String>();
  /**
   * Saves the example queries for query.wikidata.org as strings.
   */
  public static final Map<String, String> exampleQueriesString = new HashMap<String, String>();
  /**
   * Saves the example queries for query.wikidata.org as TupleExpr.
   */
  public static final Map<TupleExprWrapper, String> exampleQueriesTupleExpr = new HashMap<TupleExprWrapper, String>();
  /**
   * Saves the standard prefixes.
   */
  public static final BiMap<String, String> prefixes = HashBiMap.create();
  /**
   * Saves if metrics should be calculated for bot queries.
   */
  public static boolean withBots;
  /**
   * Saves if the input files should be modified with additional prefixes.
   */
  public static boolean readPreprocessed;
  /**
   * Saves if the query types should be generated dynamically.
   */
  public static boolean dynamicQueryTypes;
  /**
   * Define a static logger variable.
   */
  private static final Logger logger = Logger.getLogger(Main.class);
  /**
   * Saves the output folder name for query types.
   */
  private static String outputFolderNameQueryTypes;

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
    Options options = new Options();
    options.addOption("l", "logging", false, "enables file logging");
    options.addOption("o", "outputDirectory", true, "The directory the output should be written to.");
    options.addOption("f", "file", true, "The directory and input file name the program should process (e.g /home/name/data/queryCnt)");
    options.addOption("h", "help", false, "displays this help");
    options.addOption("t", "tsv", false, "reads from .tsv-files");
    options.addOption("n", "numberOfThreads", true, "number of used threads, default 1");
    options.addOption("b", "withBots", false, "enables metric calculation for bot queries+");
    options.addOption("d", "dynamicQueryTypes", false, "enables dynamic generation of query types");


    //some parameters which can be changed through parameters
    //QueryHandler queryHandler = new OpenRDFQueryHandler();
    String inputFilePrefix;
    String inputFileSuffix = ".tsv";
    String outputFolder;
    String queryParserName = "OpenRDF";
    Class inputHandlerClass = InputHandlerTSV.class;
    Class queryHandlerClass = OpenRDFQueryHandler.class;
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
      if (cmd.hasOption("tsv")) {
        inputFileSuffix = ".tsv";
        inputHandlerClass = InputHandlerTSV.class;
      }
      if (cmd.hasOption("file")) {
        inputFilePrefix = cmd.getOptionValue("file").trim();
      } else {
        System.out.println("Please specify the file which we should work on using the option '--file PREFIX' or '-f PREFIX'");
        return;
      }
      if (cmd.hasOption("outputDirectory")) {
        outputFolder = cmd.getOptionValue("outputDirectory").trim();
        if (!outputFolder.endsWith("/")) {
          outputFolder += "/";
        }
      } else {
        System.out.println("Please specify the director to write the processed logs to using the option '--outputDirectry DIRECTORY' or '-o DIRECTORY'");
        return;
      }
      if (cmd.hasOption("logging")) {
        LoggingHandler.initFileLog(queryParserName, inputFilePrefix);
      }
      if (cmd.hasOption("numberOfThreads")) {
        numberOfThreads = Integer.parseInt(cmd.getOptionValue("numberOfThreads"));
      }
      if (cmd.hasOption("withBots")) {
        withBots = true;
      }
      if (cmd.hasOption("readPreprocessed")) {
        readPreprocessed = true;
      }
      if (cmd.hasOption("dynamicQueryTypes")) {
        dynamicQueryTypes = true;
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

    loadStandardPrefixes();
    loadPreBuildQueryTypes();
    loadUserAgentRegex();
    loadPropertyGroupMapping();
    getExampleQueries();

    long startTime = System.nanoTime();

    ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

    prepareWritingQueryTypes(outputFolder);

    for (int day = 1; day <= 31; day++) {
      String inputFile = inputFilePrefix + String.format("%02d", day) + inputFileSuffix;
      Runnable parseOneMonthWorker = new ParseOneDayWorker(inputFile, inputFilePrefix, outputFolder, inputHandlerClass, queryParserName, queryHandlerClass, day);
      executor.execute(parseOneMonthWorker);
    }
    executor.shutdown();

    while (!executor.isTerminated()) {
      //wait until all workers are finished
    }

    // writeQueryTypes(queryTypes);
    writeExampleQueries(outputFolder);

    long stopTime = System.nanoTime();
    long millis = TimeUnit.MILLISECONDS.convert(stopTime - startTime, TimeUnit.NANOSECONDS);
    Date date = new Date(millis);
    System.out.println("Finished executing with all threads: " + new SimpleDateFormat("mm-dd HH:mm:ss:SSSSSSS").format(date));
  }

  /**
   * Loads all standard prefixes.
   */
  private static void loadStandardPrefixes()
  {
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
        if (row.length == 2) {
          try {
            prefixes.put(row[0].toString(), row[1].toString());
          } catch (IllegalArgumentException e) {
            logger.error("Prefix or uri for standard prefixes defined multiple times", e);
          }
          return;
        }
        logger.warn("Line with row length " + row.length + " found. Is the formatting of toolMapping.tsv correct?");
        return;
      }

    };

    parserSettings.setProcessor(rowProcessor);

    TsvParser parser = new TsvParser(parserSettings);

    try {
      parser.parse(new InputStreamReader(new FileInputStream("parserSettings/standardPrefixes.tsv")));
    }
    catch (FileNotFoundException e) {
      logger.error("Could not open configuration file for standard prefixes.", e);
    }
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
            //queryHandler.setValidityStatus(1);
            queryHandler.setQueryString(queryString);
            if (queryHandler.getValidityStatus() != 1) {
              logger.info("The Pre-build query " + filePath + " is no valid SPARQL");
              continue;
            }
            ParsedQuery normalizedPreBuildQuery = queryHandler.getNormalizedQuery();
            String queryTypeName = filePath.toString().substring(filePath.toString().lastIndexOf("/") + 1, filePath.toString().lastIndexOf("."));
            if (normalizedPreBuildQuery != null) {
              queryTypes.put(new TupleExprWrapper(normalizedPreBuildQuery.getTupleExpr()), queryTypeName);
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

    } catch (IOException e) {
      logger.error("Could not read from directory inputData/queryType/premadeQueryTypeFiles", e);
    }
  }

  /**
   * see {@link propertyGroupMapping}.
   */
  private static void loadPropertyGroupMapping()
  {
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
        if (row.length == 2) {
          if (row[1] == null) {
            return;
          }
          propertyGroupMapping.put(row[0].toString(), new HashSet<String>(Arrays.asList(row[1].toString().split(","))));
          return;
        }
        logger.warn("Line with row length " + row.length + " found. Is the formatting of propertyGroupMapping.tsv correct?");
        return;
      }

    };

    parserSettings.setProcessor(rowProcessor);

    TsvParser parser = new TsvParser(parserSettings);

    File file = new File("propertyClassification/propertyGroupMapping.tsv");

    parser.parse(file);
  }

  /**
   * see {@link userAgentRegex}.
   */
  private static void loadUserAgentRegex()
  {
    try (BufferedReader br = new BufferedReader(new FileReader("userAgentClassification/userAgentRegex.dat"))) {
      String line;
      while ((line = br.readLine()) != null) {
        userAgentRegex.add(line);
      }
    } catch (FileNotFoundException e) {
      logger.error("Could not find userAgentRegex.dat", e);
    } catch (IOException e) {
      logger.error("IOError while trying to read userAgentRegex.dat", e);
    }
  }

  /**
   * Reads the example queries from https://www.wikidata.org/wiki/Wikidata:SPARQL_query_service/queries/examples.
   */
  private static void getExampleQueries()
  {
    Document doc;
    // Not an ideal solution since it duplicates code, but I have not yet found a way to set a fallback (no proxy)
    try {

      doc = Jsoup.connect("http://www.wikidata.org/wiki/Wikidata:SPARQL_query_service/queries/examples")
          .header("Accept-Encoding", "gzip, deflate")
          .userAgent("github.com/Wikidata/QueryAnalysis")
          .maxBodySize(0)
          .proxy("webproxy.eqiad.wmnet", 8080)
          .get();
    } catch (IOException e) {
      try {
        logger.warn("Could not connect to wikidata.org via the proxy.", e);
        doc = Jsoup.connect("http://www.wikidata.org/wiki/Wikidata:SPARQL_query_service/queries/examples")
            .header("Accept-Encoding", "gzip, deflate")
            .userAgent("SPARQLQueryAnalyser")
            .maxBodySize(0)
            .get();
      } catch (IOException e2) {
        logger.error("Could not connect to wikidata.org.", e2);
        return;
      }
    }
    Elements links = doc.select("pre");
    for (Element link : links) {

      Element parent = link.parent();
      String name = null;
      while (parent.previousElementSibling() != null) {
        parent = parent.previousElementSibling();
        if (parent.nodeName().matches("h[1-6]")) {
          name = parent.child(0).text();
          break;
        }
      }

      if (name != null) {
        String query = "#" + name + "\n" + link.text();
        exampleQueriesString.put(query, name);
        OpenRDFQueryHandler queryHandler = new OpenRDFQueryHandler();
        queryHandler.setQueryString(query);
        if (queryHandler.getValidityStatus() != 1) {
          logger.warn("The example query " + name + " is no valid SPARQL.");
        } else {
          exampleQueriesTupleExpr.put(new TupleExprWrapper(queryHandler.getParsedQuery().getTupleExpr()), name);
        }
      } else {
        logger.error("Could not find header to: " + link.text());
      }
    }
  }

  /**
   * Creates the output folder for query types (if necessary) and deletes the old files if we're creating new dynamic query types.
   * @param outputFolder The input folder to create the query type subfolder in
   */
  private static void prepareWritingQueryTypes(String outputFolder)
  {
    outputFolderNameQueryTypes = outputFolder + "queryTypeFiles/";
    File outputFolderFile = new File(outputFolderNameQueryTypes);
    if (dynamicQueryTypes) {
      FileUtils.deleteQuietly(outputFolderFile);
    }
    outputFolderFile.mkdir();
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFolderNameQueryTypes + "README.md"))) {
      bw.write("This directory contains one file for each query type found in this month.\n" +
          "The name of the file (<name>.queryType) corresponds to the #QueryType entry in the processed logs.\n" +
          "WARNING: Until unifiyQueryTypes.py has been run on its parent directory this directory contains duplicates as the same query type can have multiple names.");
    } catch (IOException e) {
      logger.error("Could not create the readme for the query type folder.", e);
    }
  }

  /**
   * Writes all found query Types to queryType/queryTypeFiles/.
   *
   * @param queryTypesToWrite The map containing the query types to be written.
   */
  public static void writeQueryTypes(Map<TupleExprWrapper, String> queryTypesToWrite)
  {
    for (Entry<TupleExprWrapper, String> parsedQuery : queryTypesToWrite.entrySet()) {
      String queryType = parsedQuery.getValue();
      try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFolderNameQueryTypes + queryType + ".queryType"))) {
        bw.write(parsedQuery.getKey().toString());
      } catch (IOException e) {
        logger.error("Could not write the query type " + queryType + ".", e);
      }
    }
  }

  /**
   * Writes all the example queries to exampleQueries/.
   *
   * @param outputFolder The location of the input data
   */
  private static void writeExampleQueries(String outputFolder)
  {
    String outputFolderNameExampleQueries = outputFolder + "exampleQueries/";
    File outputFolderFile = new File(outputFolderNameExampleQueries);
    FileUtils.deleteQuietly(outputFolderFile);
    outputFolderFile.mkdir();

    try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFolderNameExampleQueries + "README.md"))) {
      bw.write("This file contains one file for each example query on the wikidata wiki at the time of this run.\n" +
          "The name of the file (<name>.exampleQuery) corresponds to the #ExampleQueryStringComparison and #ExampleQueryParsedComparison entries in the processed logs.");
    } catch (IOException e) {
      logger.error("Could not write the readme for example queries folder.", e);
    }

    for (Entry<String, String> exampleQuery : exampleQueriesString.entrySet()) {

      String fileName = exampleQuery.getValue().replaceAll("/", "SLASH");
      fileName = fileName.replaceAll(" ", "_");

      try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFolderNameExampleQueries + fileName + ".exampleQuery"))) {
        bw.write(exampleQuery.getKey());
      }
      catch (IOException e) {
        logger.error("Could not write the example query " + exampleQuery.getValue() + ".", e);
      }
    }
  }
}

