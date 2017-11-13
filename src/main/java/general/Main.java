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
import input.factories.InputHandlerTSVFactory;
import logging.LoggingHandler;
import openrdffork.TupleExprWrapper;
import org.apache.commons.cli.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.openrdf.query.parser.ParsedQuery;
import output.factories.OutputHandlerTSVFactory;
import query.Cache;
import query.OpenRDFQueryHandler;
import query.QueryHandler;
import query.factories.OpenRDFQueryHandlerFactory;
import scala.Tuple2;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
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
   * Number of disk maps for query types.
   */
  public static final int numberOfQueryTypeDiskMaps = 1600;
  /**
   * Saves the premade queryTypes.
   */
  public static final HTreeMap<byte[], String>[] queryTypes = new HTreeMap[numberOfQueryTypeDiskMaps];
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
   * Saves the standard prefixes. Format is (prefix, uri).
   */
  public static final BiMap<String, String> prefixes = HashBiMap.create();
  /**
   * A list saving the entries from {@link #prefixes} sorted by uri length.
   */
  public static final List<Map.Entry<String, String>> prefixList = new ArrayList<Map.Entry<String, String>>();
  /**
   * Saves if a prefix excludes a query from the simple dataset.
   */
  public static final Set<String> simpleQueryWhitelist = new HashSet<String>();
  /**
   * Saves all user agents that should be in the source category user.
   */
  public static final Set<String> sourceCategoryUserToolName = new HashSet<String>();
  /**
   * Saves if metrics should be calculated for bot queries.
   */
  public static boolean withBots = true;
  /**
   * Saves if the query types should be generated dynamically.
   */
  public static boolean dynamicQueryTypes = true;
  /**
   * If set to true the resulting processed output files are being gzipped.
   */
  public static boolean gzipOutput = true;
  /**
   * The name of the month we're working with - we assume that it's the last part of workingDirectory.
   */
  public static String month;
  /**
   * Saves if example queries should be matched.
   */
  private static boolean exampleQueries = true;
  /**
   * Define a static logger variable.
   */
  private static final Logger logger = Logger.getLogger(Main.class);
  /**
   * The directory we are processing.
   */
  private static String workingDirectory;
  /**
   * If set to true we calculate the OriginalIDs.
   */
  private static boolean withUniqueQueryDetection;
  /**
   * The location of the unique query map db file.
   */
  private static String dbLocation;
  /**
   * The location of the query type map db file.
   */
  public static String queryTypeMapDbLocation;
  /**
   * The query type map.
   */
  public static DB mapDb;

  /**
   * Since this is a utility class, it should not be instantiated.
   */
  private Main()
  {
    throw new AssertionError("Instantiating utility class Main");
  }

  /**
   * @return if unique query detection was enabled.
   */
  public static boolean isWithUniqueQueryDetection()
  {
    return withUniqueQueryDetection;
  }

  /**
   * Selects the files to be processed and specifies the files to write to.
   *
   * @param args Arguments to specify runtime behavior.
   */
  public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
  {
    Options options = new Options();
    options.addOption("l", "logging", false, "Enables file logging.");
    options.addOption("w", "workingDirectory", true, "The directory we should be working on.");
    options.addOption("h", "help", false, "Displays this help.");
    options.addOption("t", "threads", true, "Number of used threads, default 1");
    options.addOption("b", "noBotMetrics", false, "Disables metric calculation for bot queries.");
    options.addOption("d", "noDynamicQueryTypes", false, "Disables dynamic generation of query types.");
    options.addOption("g", "noGzipOutput", false, "Disables gzipping of the output files.");
    options.addOption("e", "noExampleQueriesOutput", false, "Disables the matching of example queries.");
    options.addOption("i", "ignoreLock", false, "Ignores the lock file.");
    options.addOption("u", "withUniqueQueryDetection", false, "Deunify queries.");
    options.addOption("p", "dbLocation", true, "The path of the uniqueQueriesMapDb file. Default is in the working directory.");
    options.addOption("q", "queryTypeMapLocation", true, "The path of the query type map db file. Default is in the working directory.");


    //some parameters which can be changed through parameters
    String inputFilePrefix;
    String inputFileSuffix = ".tsv.gz";
    String outputFolder;
    String queryParserName = "OpenRDF";
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
      if (cmd.hasOption("workingDirectory")) {
        workingDirectory = cmd.getOptionValue("workingDirectory").trim();
        if (!workingDirectory.endsWith("/")) {
          workingDirectory += "/";
        }
        inputFilePrefix = workingDirectory + "rawLogData/QueryCnt";

        outputFolder = workingDirectory + "processedLogData/";

        month = new File(workingDirectory.substring(0, workingDirectory.length() - 1)).getName();

      } else {
        System.out.println("Please specify the directory which we should work on using the option '--workingDirectory DIRECTORY' or '-w DIRECTORY'");
        return;
      }
      if (cmd.hasOption("logging")) {
        LoggingHandler.initFileLog(queryParserName, inputFilePrefix);
      }
      if (cmd.hasOption("threads")) {
        numberOfThreads = Integer.parseInt(cmd.getOptionValue("threads"));
      }
      if (cmd.hasOption("noBotMetrics")) {
        withBots = false;
      }
      if (cmd.hasOption("noDynamicQueryTypes")) {
        dynamicQueryTypes = false;
      }
      if (cmd.hasOption("noGzipOutput")) {
        gzipOutput = false;
      }
      if (cmd.hasOption("noExampleQueriesOutput")) {
        exampleQueries = false;
      }
      if (cmd.hasOption("withUniqueQueryDetection")) {
        withUniqueQueryDetection = true;
      }
      if (cmd.hasOption("dbLocation")) {
        dbLocation = cmd.getOptionValue("dbLocation");
      } else {
        dbLocation = workingDirectory + "uniqueQueryMap.db";
      }
      if (cmd.hasOption("queryTypeMapLocation")) {
        queryTypeMapDbLocation = cmd.getOptionValue("queryTypeMapLocation");
      } else {
        queryTypeMapDbLocation = workingDirectory + "queryTypeMap.db";
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

    loadQueryTypeMap();
    backupQueryTypeMap();
    loadPreBuildQueryTypes();

    loadUserAgentRegex();
    loadToolNamesForUserCategory();

    if (exampleQueries) {
      getExampleQueries();
      writeExampleQueries(outputFolder);
    }
    loadPropertyGroupMapping();

    File lockFile = null;
    if (!cmd.hasOption("ignoreLock")) {
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
    }

    long startTime = System.nanoTime();

    ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

    for (int day = 1; day <= 31; day++) {
      String inputFile = inputFilePrefix + String.format("%02d", day) + inputFileSuffix;

      String outputFile = outputFolder + "QueryProcessed" + queryParserName + String.format("%02d", day);

      try {
        Runnable parseOneMonthWorker = new ParseOneDayWorker(new InputHandlerTSVFactory(), inputFile, new OutputHandlerTSVFactory(), outputFile, new OpenRDFQueryHandlerFactory(), day, true);
        executor.execute(parseOneMonthWorker);
      } catch (IOException e) {
      }
    }
    executor.shutdown();

    while (!executor.isTerminated()) {
      //wait until all workers are finished
    }

    // close MabDb Database - if this goes wrong the file is corrupted the next time we execute this

    if (Main.isWithUniqueQueryDetection()) {
      Cache.mapDb.close();
    }

    mapDb.close();

    if (Main.withUniqueQueryDetection) {
      File file = new File(getDbLocation());
      file.delete();
    }

    if (!cmd.hasOption("ignoreLock")) {
      lockFile.delete();
    }

    long stopTime = System.nanoTime();
    long millis = TimeUnit.MILLISECONDS.convert(stopTime - startTime, TimeUnit.NANOSECONDS);
    Date date = new Date(millis);
    System.out.println("Finished executing with all threads: " + new SimpleDateFormat("mm-dd HH:mm:ss:SSSSSSS").format(date));
  }

  /**
   * Loads the existing query type map or creates one if it is not present.
   */
  public static void loadQueryTypeMap()
  {
    mapDb = DBMaker.fileDB(queryTypeMapDbLocation).fileChannelEnable().fileMmapEnable().make();
    for (int i = 0; i < numberOfQueryTypeDiskMaps; i++) {
      queryTypes[i] = mapDb.hashMap("queryTypeMap" + i, Serializer.BYTE_ARRAY, Serializer.STRING).createOrOpen();
    }
  }

  /**
   * Creates a backup of the query type map db file if it exists.
   */
  private static void backupQueryTypeMap()
  {
    Path currentQueryTypeMapPath = Paths.get(queryTypeMapDbLocation);
    Path backupQueryTypeMapPath = Paths.get(queryTypeMapDbLocation + ".bak");
    try {
      Files.copy(currentQueryTypeMapPath, backupQueryTypeMapPath, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      logger.error("Failed to backup query type map file.", e);
    }
  }

  /**
   * Loads all user agents that should be in the user source category.
   */
  private static void loadToolNamesForUserCategory()
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
        if (row.length < 1) {
          logger.warn("Ignoring line without tab while parsing.");
          return;
        }
        if (row.length == 1) {
          sourceCategoryUserToolName.add(row[0].toString());
          return;
        }
        logger.warn("Line with row length " + row.length + " found. Is the formatting of toolNameForUserCategory.tsv correct?");
        return;
      }

    };

    parserSettings.setProcessor(rowProcessor);

    TsvParser parser = new TsvParser(parserSettings);

    try {
      parser.parse(new InputStreamReader(new FileInputStream("userAgentClassification/toolNameForUserCategory.tsv")));
    } catch (FileNotFoundException e) {
      logger.error("Could not open configuration file for standard prefixes.", e);
    }
  }

  /**
   * Loads all standard prefixes.
   */
  public static void loadStandardPrefixes()
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
        if (row.length == 3) {
          try {
            prefixes.put(row[0].toString(), row[1].toString());
          } catch (IllegalArgumentException e) {
            logger.error("Prefix or uri for standard prefixes defined multiple times", e);
          }
          if (row[2].toString().equals("simple")) {
            simpleQueryWhitelist.add(row[0].toString());
          }
          return;
        }
        logger.warn("Line with row length " + row.length + " found. Is the formatting of standardPrefixes.tsv correct?");
        return;
      }

    };

    parserSettings.setProcessor(rowProcessor);

    TsvParser parser = new TsvParser(parserSettings);

    try {
      parser.parse(new InputStreamReader(new FileInputStream("parserSettings/standardPrefixes.tsv")));
    } catch (FileNotFoundException e) {
      logger.error("Could not open configuration file for standard prefixes.", e);
    }

    prefixList.addAll(Main.prefixes.entrySet());
    Collections.sort(prefixList, new Comparator<Map.Entry<String, String>>()
    {

      @Override
      public int compare(Entry<String, String> arg0, Entry<String, String> arg1)
      {
        return Integer.valueOf(arg1.getValue().length()).compareTo(Integer.valueOf(arg0.getValue().length()));
      }
    });
  }

  /**
   * Loads all pre-build query types.
   */
  public static void loadPreBuildQueryTypes()
  {

    try (DirectoryStream<Path> directoryStream =
             Files.newDirectoryStream(Paths.get("preBuildQueryTypeFiles"))) {
      for (Path filePath : directoryStream) {
        if (Files.isRegularFile(filePath)) {
          if (filePath.toString().endsWith(".preBuildQueryType")) {
            String queryString = new String(readAllBytes(filePath));
            OpenRDFQueryHandler queryHandler = new OpenRDFQueryHandler(QueryHandler.Validity.DEFAULT, -1L, -1, queryString, "preBuildQueryTypes", "", -1);
            if (queryHandler.getValidityStatus() != QueryHandler.Validity.VALID) {
              logger.info("The Pre-build query " + filePath + " is no valid SPARQL");
              continue;
            }
            ParsedQuery normalizedPreBuildQuery = queryHandler.getNormalizedQuery();
            String queryTypeName = filePath.toString().substring(filePath.toString().lastIndexOf("/") + 1, filePath.toString().lastIndexOf("."));
            if (normalizedPreBuildQuery != null) {
              String queryDump = normalizedPreBuildQuery.getTupleExpr().toString();
              byte[] md5 = DigestUtils.md5(queryDump);

              int index = Math.floorMod(queryDump.hashCode(), numberOfQueryTypeDiskMaps);
              queryTypes[index].put(md5, queryTypeName);
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
   * Loads the mapping of property to groups.
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
   * Loads multiple regular expressions that should match all browser user agents.
   */
  public static void loadUserAgentRegex()
  {
    try (BufferedReader br = new BufferedReader(new FileReader("userAgentClassification/userAgentRegex.dat"))) {
      String line;
      while ((line = br.readLine()) != null) {
        userAgentRegex.add(line);
      }
    }
    catch (FileNotFoundException e) {
      logger.error("Could not find userAgentRegex.dat", e);
    }
    catch (IOException e) {
      logger.error("IOError while trying to read userAgentRegex.dat", e);
    }
  }

  /**
   * Reads the example queries from https://www.wikidata.org/wiki/Wikidata:SPARQL_query_service/queries/examples.
   */
  private static void getExampleQueries()
  {
    Document doc;
    Connection connection = Jsoup.connect("http://www.wikidata.org/wiki/Wikidata:SPARQL_query_service/queries/examples")
        .header("Accept-Encoding", "gzip, deflate")
        .userAgent("github.com/Wikidata/QueryAnalysis")
        .maxBodySize(0);
    try {
      doc = connection.get();
    } catch (IOException e) {
      try {
        logger.warn("While trying to download the example queries could not connect directloy to wikidata.org, trying via a proxy now.");
        doc = connection.proxy("webproxy.eqiad.wmnet", 8080).get();
      } catch (IOException e2) {
        logger.error("Could not even connect to wikidata.org via the proxy.", e2);
        return;
      }
    }
    doc.select("span.lineno").remove();
    Elements links = doc.select("pre");
    for (Element link : links) {

      Element previous = link.parent();
      String name = null;
      while (name == null) {
        if (previous.nodeName().matches("h[1-6]")) {
          name = previous.child(0).text();
          break;
        }
        if (previous.previousElementSibling() != null) {
          previous = previous.previousElementSibling();
        } else if (previous.parent() != null) {
          previous = previous.parent();
        } else {
          break;
        }
      }

      if (name != null) {
        String query = link.text();
        exampleQueriesString.put(query, name);
        OpenRDFQueryHandler queryHandler = new OpenRDFQueryHandler(QueryHandler.Validity.DEFAULT, -1L, -1, query, "exampleQueries", "", -1);
        if (queryHandler.getValidityStatus() != QueryHandler.Validity.VALID) {
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
      } catch (IOException e) {
        logger.error("Could not write the example query " + exampleQuery.getValue() + ".", e);
      }
    }
  }

  public static String getDbLocation()
  {
    return dbLocation;
  }
}

