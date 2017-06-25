package output;

import com.univocity.parsers.tsv.TsvWriter;
import com.univocity.parsers.tsv.TsvWriterSettings;
import general.Main;
import org.apache.log4j.Logger;
import query.Cache;
import query.QueryHandler;
import query.SparqlStatisticsCollector;

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
  private final Class queryHandlerClass;

  /**
   * A writer created at object creation to be used in line-by-line writing.
   */
  private TsvWriter writer;

  /**
   * The outputStream object we are writing too.
   */
  private OutputStream outputStream = null;

  private Cache cache = Cache.getInstance();

  /**
   * Creates the file specified in the constructor and writes the header.
   *
   * @param fileToWrite       location of the file to write the received values to
   * @param queryHandlerClass handler class used to analyze the query string that will be written
   * @throws FileNotFoundException if the file exists but is a directory
   *                               rather than a regular file, does not exist but cannot be created,
   *                               or cannot be opened for any other reason
   */
  public OutputHandlerTSV(String fileToWrite, Class queryHandlerClass) throws FileNotFoundException
  {
    if (Main.noGzipOutput) {
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
    header.add("#SubjectsAndObjects");
    header.add("#Predicates");
    header.add("#Categories");

    //add all sparqlStatisticNodes
    for (String sparqlStatisticFeature : SparqlStatisticsCollector.getDefaultMap().keySet()) {
      header.add("#" + sparqlStatisticFeature);
    }

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
  public final void writeLine(String queryToAnalyze, Integer validityStatus, String userAgent, long currentLine, String currentFile)
  {
    QueryHandler queryHandler = cache.getQueryHandler(validityStatus, queryToAnalyze, queryHandlerClass);

    queryHandler.setUserAgent(userAgent);
    queryHandler.setCurrentLine(currentLine);
    queryHandler.setCurrentFile(currentFile);
    queryHandler.setThreadNumber(threadNumber);
    queryHandler.setQueryTypes(queryTypes);

    List<Object> line = new ArrayList<>();
    line.add(queryHandler.getValidityStatus());
    line.add(queryHandler.getToolName());
    line.add(queryHandler.getToolVersion());
    if (Main.withBots || queryHandler.getToolName().equals("0") || queryHandler.getToolName().equals("USER")) {
      line.add(queryHandler.getExampleQueryStringMatch());
      line.add(queryHandler.getExampleQueryTupleMatch());
      line.add(queryHandler.getStringLength());
      line.add(queryHandler.getQuerySize());
      line.add(queryHandler.getVariableCountHead());
      line.add(queryHandler.getVariableCountPattern());
      line.add(queryHandler.getTripleCountWithService());
      line.add(queryHandler.getQueryType());
      line.add(queryHandler.getqIDString());
      line.add(queryHandler.getpIDString());
      line.add(queryHandler.getCategoriesString());

      Map<String, Integer> sparqlStatistics = queryHandler.getSparqlStatistics();
      //add all sparqlStatisticNodes
      for (String sparqlStatisticFeature : sparqlStatistics.keySet()) {
        line.add(sparqlStatistics.get(sparqlStatisticFeature));
      }

    } else {
      for (int i = 0; i < 10 + SparqlStatisticsCollector.getDefaultMap().size(); i++) {
        line.add(-1);
      }
    }
    line.add(currentFile + "_" + currentLine);

    for (int i = 0; i < line.size(); i++) {
      line.set(i, makeStringMoreReadable(line.get(i).toString()));
    }

    writer.writeRow(line);
  }

  /**
   * Converts the given string to UNKNOWN if it is -1 and to NULL if the object is null, returns the original in all other cases.
   * @param inputString The string to be converted.
   * @return UNKNOWN for '-1', NULL for null-objects, the original string in all other cases
   */
  final String makeStringMoreReadable(String inputString)
  {
    if (inputString == null) {
      return "NULL";
    }

    if (inputString.equals("-1")) {
      return "UNKNOWN";
    }
    return inputString;
  }
}
