package output;

import com.univocity.parsers.tsv.TsvWriter;
import com.univocity.parsers.tsv.TsvWriterSettings;
import org.apache.log4j.Logger;
import query.QueryHandler;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
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
  private static Logger logger = Logger.getLogger(OutputHandlerTSV.class);
  /**
   * The handler used to process the rows to output.
   */
  private QueryHandler queryHandler;
  /**
   * A writer created at object creation to be used in line-by-line writing.
   */
  private TsvWriter writer;
  /**
   * The file to write the data to.
   */
  private String file;

  /**
   * Creates the file specified in the constructor and writes the header.
   *
   * @param queryHandlerToUse The handler used to analyze the query string that will be written
   * @param fileToWrite       location of the file to write the received values to
   * @throws FileNotFoundException if the file exists but is a directory
   *                               rather than a regular file, does not exist but cannot be created,
   *                               or cannot be opened for any other reason
   */
  public OutputHandlerTSV(String fileToWrite, QueryHandler queryHandlerToUse) throws FileNotFoundException
  {
    this.queryHandler = queryHandlerToUse;
    this.file = fileToWrite;
    FileOutputStream outputWriter = new FileOutputStream(fileToWrite + ".tsv");
    writer = new TsvWriter(outputWriter, new TsvWriterSettings());
    for (int i = 0; i < hourly_user.length; i++) {
      hourly_user[i] = 0L;
      hourly_spider[i] = 0L;
    }

    List<String> header = new ArrayList<String>();
    header.add("#Valid");
    header.add("#StringLengthWithComments");
    header.add("#StringLengthNoComments");
    header.add("#VariableCountHead");
    header.add("#VariableCountPattern");
    header.add("#TripleCountWithService");
    header.add("#TripleCountNoService");
    header.add("#uri_path");
    header.add("#user_agent");
    header.add("#ts");
    header.add("#agent_type");
    header.add("#hour");
    header.add("#original_line(filename_line)");
    writer.writeHeaders(header);
  }

  /**
   * Closes the writer this object was writing to.
   */
  public final void closeFiles()
  {
    try {
      FileOutputStream outputHourly = new FileOutputStream(file + "HourlyAgentCount.tsv");
      TsvWriter hourlyWriter = new TsvWriter(outputHourly, new TsvWriterSettings());
      hourlyWriter.writeHeaders("hour", "user_count", "spider_count");
      for (int i = 0; i < hourly_user.length; i++) {
        hourlyWriter.writeRow(new String[] {String.valueOf(i), String.valueOf(hourly_user[i]), String.valueOf(hourly_spider[i])});
      }
      hourlyWriter.close();
    } catch (FileNotFoundException e) {
      logger.error("Could not write the hourly agent_type count.", e);
    }
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
    queryHandler.setValidityStatus(validityStatus);
    queryHandler.setQueryString(queryToAnalyze);
    queryHandler.setCurrentLine(currentLine);
    queryHandler.setCurrentFile(currentFile);


    List<Object> line = new ArrayList<Object>();
    line.add(queryHandler.getValidityStatus());

    line.add(queryHandler.getStringLength());
    line.add(queryHandler.getStringLengthNoComments());
    line.add(queryHandler.getVariableCountHead());
    line.add(queryHandler.getVariableCountPattern());
    line.add(queryHandler.getTripleCountWithService());
    line.add(-1);
    for (int i = 1; i < row.length; i++) {
      line.add(row[i]);
    }
    line.add(currentFile + "_" + currentLine);
    int hour = -1;

    try {
      hour = Integer.parseInt(row[5].toString());
    } catch (NumberFormatException e) {
      logger.error("Hour field is not parsable as integer.", e);
    }
    if (0 <= hour && hour < 24) {
      if (row[4].toString().equals("user")) {
        hourly_user[Integer.parseInt(row[5].toString())] += 1L;
      }
      if (row[4].toString().equals("spider")) {
        hourly_spider[Integer.parseInt(row[5].toString())] += 1L;
      }
    } else {
      logger.error("Hour field " + hour + " is not between 0 and 24.");
    }
    writer.writeRow(line);
  }
}
