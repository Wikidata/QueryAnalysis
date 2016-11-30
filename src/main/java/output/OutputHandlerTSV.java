package output;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

import com.univocity.parsers.tsv.TsvWriter;
import com.univocity.parsers.tsv.TsvWriterSettings;
import query.QueryHandler;

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
   * The handler used to process the rows to output.
   */
  private QueryHandler queryHandler;
  /**
   * A writer created at object creation to be used in line-by-line writing.
   */
  private TsvWriter writer;

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
    FileOutputStream outputWriter = new FileOutputStream(fileToWrite);
    writer = new TsvWriter(outputWriter, new TsvWriterSettings());
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
    writer.close();
  }

  /**
   * Takes a query and the additional information from input and writes
   * the available data to the active .tsv.
   *
   * @param queryToAnalyze The query that should be analyzed and written.
   * @param row            The input data to be written to this line.
   * @param currentLine    The line from which the data to be written originates.
   * @param currentFile    The file from which the data to be written originates.
   */
  @Override
  public final void writeLine(String queryToAnalyze, Object[] row, long currentLine, String currentFile)
  {
    queryHandler.setQueryString(queryToAnalyze);
    queryHandler.setCurrentLine(currentLine);
    queryHandler.setCurrentFile(currentFile);

    List<Object> line = new ArrayList<Object>();
    if (queryHandler.isValid()) {
      line.add("1");
    } else {
      line.add("-1");
    }

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
    writer.writeRow(line);
  }
}
