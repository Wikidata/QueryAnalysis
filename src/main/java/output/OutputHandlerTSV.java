package output;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import general.Main;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.log4j.Logger;
import query.Cache;
import query.QueryHandler;
import query.factories.QueryHandlerFactory;



/**
 * @author adrian
 */
public class OutputHandlerTSV extends OutputHandler
{
  /**
   *
   */
  private static final long serialVersionUID = 1L;
  /**
   * Define a static logger variable.
   */
  private static final Logger logger = Logger.getLogger(OutputHandlerTSV.class);

  /**
   * The class of which a queryHandlerObject should be created.
   */
  private QueryHandlerFactory queryHandlerFactory;

  /**
   * The csv-handler.
   */
  private CSVPrinter csvPrinter;

  /**
   * A writer created at object creation to be used in line-by-line writing.
   */
  private BufferedWriter bufferedWriter;

  /**
   * The Caching module object.
   */
  private Cache cache = new Cache();

  /**
   * @param fileToWrite              location of the file to write the received values to
   * @param queryHandlerFactoryToSet The query handler factory to supply the query handler to generate the output with.
   * @throws IOException if the file exists but is a directory
   *                               rather than a regular file, does not exist but cannot be created,
   *                               or cannot be opened for any other reason
   */
  public OutputHandlerTSV(String fileToWrite, QueryHandlerFactory queryHandlerFactoryToSet) throws IOException
  {
    super(fileToWrite, queryHandlerFactoryToSet);
  }

  /**
   * Creates the file specified in the constructor and writes the header.
   *
   * @param fileToWrite              location of the file to write the received values to
   * @param queryHandlerFactoryToSet The query handler factory to supply the query handler to generate the output with.
   * @throws IOException if the output file could not be to
   */
  public void initialize(String fileToWrite, QueryHandlerFactory queryHandlerFactoryToSet) throws IOException
  {
    if (!Main.gzipOutput) {
      outputFile = fileToWrite + ".tsv";
      bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile, false), StandardCharsets.UTF_8));
    } else {
      try {
        outputFile = fileToWrite + ".tsv.gz";
        bufferedWriter = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(outputFile, false)), StandardCharsets.UTF_8));
      } catch (IOException e) {
        logger.error("Somehow we are unable to write the output to " + outputFile, e);
      }
    }

    List<String> header = new ArrayList<>();

    header.add("#Valid");
    header.add("#First");
    header.add("#UniqueId");
    header.add("#OriginalId");
    header.add("#SourceCategory");
    header.add("#ToolName");
    header.add("#ToolVersion");
    header.add("#ExampleQueryStringComparison");
    header.add("#ExampleQueryParsedComparison");
    header.add("#StringLengthWithComments");
    header.add("#QuerySize");
    header.add("#VariableCountHead");
    header.add("#VariableCountPattern");
    header.add("#TripleCountWithService");
    header.add("#TripleCountWithoutService");
    header.add("#QueryType");
    header.add("#QueryComplexity");
    header.add("#NonSimplePropertyPaths");
    header.add("#SubjectsAndObjects");
    header.add("#Predicates");
    header.add("#Categories");
    header.add("#Coordinates");
    header.add("#UsedSparqlFeatures");
    header.add("#PrimaryLanguage");
    header.add("#ServiceCalls");

    header.add("#original_line(filename_line)");

    csvPrinter = new CSVPrinter(bufferedWriter, CSVFormat.newFormat('\t')
        .withHeader(header.toArray(new String[header.size()]))
        .withRecordSeparator('\n')
        .withQuote('"'));
    this.queryHandlerFactory = queryHandlerFactoryToSet;


  }

  /**
   * Takes a query and the additional information from input and writes
   * the available data to the active .tsv.
   *
   * @param queryToAnalyze The query that should be analyzed and written.
   * @param validityStatus The validity status which was the result of the decoding process of the URI
   * @param userAgent      The user agent the query was being executed by.
   * @param currentLine    The line from which the data to be written originates.
   * @param currentFile    The file from which the data to be written originates.
   * @throws IOException If there was an error during writing.
   */
  @Override
  public final void writeLine(String queryToAnalyze, QueryHandler.Validity validityStatus, String userAgent, String timeStamp, long currentLine, int day, String currentFile) throws IOException
  {
    QueryHandler queryHandler = cache.getQueryHandler(validityStatus, queryToAnalyze, currentLine, day, userAgent, currentFile, threadNumber, queryHandlerFactory);

    // the order in which fields are being written to this list is important - it needs to be the same as the one for the header above!
    List<Object> line = new ArrayList<>();
    line.add(queryHandler.getValidityStatus());
    if (queryHandler.isFirst()) {
      line.add("FIRST");
    } else {
      line.add("COPY");
    }
    line.add(queryHandler.getUniqeId());
    line.add(queryHandler.getOriginalId());
    line.add(queryHandler.getSourceCategory());
    line.add(queryHandler.getToolName());
    line.add(queryHandler.getToolVersion());
    if (Main.withBots || queryHandler.getToolName().equals("UNKNOWN") || queryHandler.getToolName().equals("USER")) {
      line.add(queryHandler.getExampleQueryStringMatch());
      line.add(queryHandler.getExampleQueryTupleMatch());
      line.add(queryHandler.getStringLength());
      line.add(queryHandler.getQuerySize());
      line.add(queryHandler.getVariableCountHead());
      line.add(queryHandler.getVariableCountPattern());
      line.add(queryHandler.getTripleCountWithService());
      line.add(queryHandler.getTripleCountWithoutService());
      line.add(queryHandler.getQueryType());
      line.add(queryHandler.getSimpleOrComplex());
      line.add(queryHandler.getNonSimplePropertyPathsString());
      line.add(queryHandler.getqIDString());
      line.add(queryHandler.getpIDString());
      line.add(queryHandler.getCategoriesString());
      line.add(queryHandler.getCoordinatesString());

      //add all sparqlStatisticNodes
      Map<String, Integer> sparqlStatistics = queryHandler.getSparqlStatistics();
      String sparqlStatisticsLine = "";
      for (Map.Entry<String, Integer> sparqlStatisticFeature : sparqlStatistics.entrySet()) {
        if (sparqlStatisticFeature.getValue() != 0) {
          sparqlStatisticsLine += sparqlStatisticFeature.getKey() + ", ";

        }
      }
      line.add(sparqlStatisticsLine);
      line.add(queryHandler.getPrimaryLanguage());
      line.add(queryHandler.getServiceCallsString());

    } else {
      for (int i = 0; i < 13; i++) {
        line.add(-1);
      }
    }
    line.add(currentFile + "_" + currentLine);

    csvPrinter.printRecord(line);
  }
  
  /**
   * Closes the writer this object was writing to.
   */
  public final void closeFiles()
  {
    try {
      csvPrinter.close();
    } catch (IOException e) {
      logger.error("Unable to close the outputStream ", e);
    }
  }
}
