package output;

import com.univocity.parsers.tsv.TsvWriter;
import com.univocity.parsers.tsv.TsvWriterSettings;
import general.Main;
import org.apache.log4j.Logger;
import query.Cache;
import query.QueryHandler;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

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
  private Class queryHandlerClass;

  /**
   * A writer created at object creation to be used in line-by-line writing.
   */
  private TsvWriter writer;

  /**
   * The outputStream object we are writing too.
   */
  private OutputStream outputStream;

  /**
   * The Caching module object.
   */
  private Cache cache = new Cache();

  /**
   * @param fileToWrite            location of the file to write the received values to
   * @param queryHandlerClassToSet handler class used to analyze the query string that will be written
   * @throws FileNotFoundException if the file exists but is a directory
   *                               rather than a regular file, does not exist but cannot be created,
   *                               or cannot be opened for any other reason
   */
  public OutputHandlerTSV(String fileToWrite, Class queryHandlerClassToSet) throws FileNotFoundException
  {
    super(fileToWrite, queryHandlerClassToSet);
  }

  /**
   * Creates the file specified in the constructor and writes the header.
   *
   * @param fileToWrite       location of the file to write the received values to
   * @param queryHandlerClass handler class used to analyze the query string that will be written
   * @throws FileNotFoundException if the file exists but is a directory
   *                               rather than a regular file, does not exist but cannot be created,
   *                               or cannot be opened for any other reason
   */
  public void initialize(String fileToWrite, Class queryHandlerClass) throws FileNotFoundException
  {
    if (!Main.gzipOutput) {
      outputStream = new FileOutputStream(fileToWrite + ".tsv");
    } else {
      try {
        outputStream = new GZIPOutputStream(new FileOutputStream(new File(fileToWrite + ".tsv.gz")));
      } catch (IOException e) {
        logger.error("Somehow we are unable to write the output to " + fileToWrite + ".tsv.gz", e);
      }
    }
    writer = new TsvWriter(outputStream, new TsvWriterSettings());
    this.queryHandlerClass = queryHandlerClass;

    List<String> header = new ArrayList<>();
    header.add("#Valid");
    header.add("#UniqueId");
    header.add("#OriginalId");
    header.add("#ToolName");
    header.add("#ToolVersion");
    header.add("#ExampleQueryStringComparison");
    header.add("#ExampleQueryParsedComparison");
    header.add("#StringLengthWithComments");
    header.add("#QuerySize");
    header.add("#VariableCountHead");
    header.add("#VariableCountPattern");
    header.add("#TripleCountWithService");
    header.add("#QueryType");
    header.add("#QueryComplexity");
    header.add("#SubjectsAndObjects");
    header.add("#Predicates");
    header.add("#Categories");
    header.add("#Coordinates");
    header.add("#UsedSparqlFeatures");

    header.add("#original_line(filename_line)");
    writer.writeHeaders(header);
  }

  /**
   * Closes the writer this object was writing to.
   */
  public final void closeFiles()
  {
    writer.close();
    try {
      outputStream.close();
    } catch (IOException e) {
      logger.error("Unable to close the outputStream ", e);
    }
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
   */
  @Override
  public final void writeLine(String queryToAnalyze, QueryHandler.Validity validityStatus, String userAgent, long currentLine, int day, String currentFile)
  {
    QueryHandler queryHandler = cache.getQueryHandler(validityStatus, queryToAnalyze, currentLine, day, queryHandlerClass);

    queryHandler.setUserAgent(userAgent);
    queryHandler.setCurrentFile(currentFile);
    queryHandler.setThreadNumber(threadNumber);
    queryHandler.setQueryTypes(queryTypes);

    // the order in which fields are being written to this list is important - it needs to be the same as the one for the header above!
    List<Object> line = new ArrayList<>();
    line.add(queryHandler.getValidityStatus());
    line.add(queryHandler.getUniqeId());
    line.add(queryHandler.getOriginalId());
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
      line.add(queryHandler.getQueryType());
      line.add(queryHandler.getSimpleOrComplex());
      line.add(queryHandler.getqIDString());
      line.add(queryHandler.getpIDString());
      line.add(queryHandler.getCategoriesString());
      line.add(queryHandler.getCoordinatesString());

      Map<String, Integer> sparqlStatistics = queryHandler.getSparqlStatistics();
      //add all sparqlStatisticNodes
      String sparqlStatisticsLine = "";
      for (Map.Entry<String, Integer> sparqlStatisticFeature : sparqlStatistics.entrySet()) {
        if (sparqlStatisticFeature.getValue() != 0) {
          sparqlStatisticsLine += sparqlStatisticFeature.getKey() + ", ";

        }
      }
      line.add(sparqlStatisticsLine);

    } else {
      for (int i = 0; i < 13; i++) {
        line.add(-1);
      }
    }
    line.add(currentFile + "_" + currentLine);

    writer.writeRow(line);
  }
}
