package output;

import com.univocity.parsers.tsv.TsvWriter;
import com.univocity.parsers.tsv.TsvWriterSettings;

import general.Main;

import org.apache.log4j.Logger;
import query.QueryHandler;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
   * the class of wich a queryHandlerObject should be created
   */
  private final Class queryHandlerClass;

  /**
   * A writer created at object creation to be used in line-by-line writing.
   */
  private TsvWriter writer;

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
    FileOutputStream outputWriter = new FileOutputStream(fileToWrite + ".tsv");
    writer = new TsvWriter(outputWriter, new TsvWriterSettings());
    for (int i = 0; i < hourly_user.length; i++) {
      hourly_user[i] = 0L;
      hourly_spider[i] = 0L;
    }

    this.queryHandlerClass = queryHandlerClass;

    List<String> header = new ArrayList<>();
    header.add("#Valid");
    header.add("#ToolName");
    header.add("#ToolVersion");
    header.add("#StringLengthWithComments");
    header.add("#QuerySize");
    header.add("#VariableCountHead");
    header.add("#VariableCountPattern");
    header.add("#TripleCountWithService");
    header.add("#TripleCountNoService");
    header.add("#QueryType");
    header.add("#uri_path");
    header.add("#user_agent");
    header.add("#ts");
    header.add("#agent_type");
    header.add("#hour");
    header.add("#httpStatusCode");
    header.add("#original_line(filename_line)");
    writer.writeHeaders(header);
  }

  /**
   * Closes the writer this object was writing to.
   */
  public final void closeFiles()
  {
/*    try {
      FileOutputStream outputHourly = new FileOutputStream(file + "HourlyAgentCount.tsv");
      TsvWriter hourlyWriter = new TsvWriter(outputHourly, new TsvWriterSettings());
      hourlyWriter.writeHeaders("hour", "user_count", "spider_count");
      for (int i = 0; i < hourly_user.length; i++) {
        hourlyWriter.writeRow(new String[] {String.valueOf(i), String.valueOf(hourly_user[i]), String.valueOf(hourly_spider[i])});
      }
      hourlyWriter.close();
    } catch (FileNotFoundException e) {
      logger.error("Could not write the hourly agent_type count.", e);
    }*/

    writer.close();
  }

  /**
   * Takes a query and the additional information from input and writes
   * the available data to the active .tsv.
   *
   * @param queryToAnalyze The query that should be analyzed and written.
   * @param validityStatus The validity status which was the result of the decoding process of the URI
   * @param row            The input data to be written to this line.
   * @param currentLine    The line from which the data to be written originates.
   * @param currentFile    The file from which the data to be written originates.
   */
  @Override
  public final void writeLine(String queryToAnalyze, Integer validityStatus, Object[] row, long currentLine, String currentFile)
  {
    QueryHandler queryHandler = null;
    try {
      queryHandler = (QueryHandler) queryHandlerClass.getConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      logger.error("Failed to create query handler object" + e);
    }
    queryHandler.setValidityStatus(validityStatus);
    queryHandler.setUserAgent(row[2].toString());
    queryHandler.setQueryString(queryToAnalyze);
    queryHandler.setCurrentLine(currentLine);
    queryHandler.setCurrentFile(currentFile);


    List<Object> line = new ArrayList<>();
    line.add(queryHandler.getValidityStatus());
    line.add(queryHandler.getToolName());
    line.add(queryHandler.getToolVersion());
    if (Main.withBots || queryHandler.getToolName().equals("0")) {
      line.add(queryHandler.getStringLength());
      line.add(queryHandler.getQuerySize());
      line.add(queryHandler.getVariableCountHead());
      line.add(queryHandler.getVariableCountPattern());
      line.add(queryHandler.getTripleCountWithService());
      line.add(-1);
      line.add(queryHandler.getQueryType());
    } else {
      for (int i = 0; i < 7; i++) {
        line.add(-1);
      }
    }
    //add existing lines
    line.addAll(Arrays.asList(row));
    line.add(currentFile + "_" + currentLine);

    writer.writeRow(line);
  }
}
