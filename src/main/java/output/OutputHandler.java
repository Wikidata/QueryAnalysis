package output;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.tsv.TsvWriter;
import com.univocity.parsers.tsv.TsvWriterSettings;

import query.QueryHandler;

/**
 *
 * @author adrian
 *
 */
public class OutputHandler
{
  /**
   * A writer created at object creation to be used in line-by-line writing.
   */
  private TsvWriter writer;
  /**
   * Creates the file specified in the constructor and writes the header.
   * @param fileToWrite location of the file to write the received values to
   * @throws FileNotFoundException if the file exists but is a directory
   * rather than a regular file, does not exist but cannot be created,
   * or cannot be opened for any other reason
   */
  public OutputHandler(String fileToWrite) throws FileNotFoundException
  {
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
    header.add("#original_line(filename_line)");
    /*
    header.add("#uri_path");
    header.add("#user_agent");
    header.add("#ts");
    header.add("#agent_type");
    header.add("#hour");
    */
    writer.writeHeaders(header);
  }

  /**
   * Closes the writer this object was writing to.
   */
  public final void closeFile()
  {
    writer.close();
  }

  /**
   * Takes a query and the additional information from input and writes
   * the available data to the active .tsv.
   * @param queryHandler The queryHandler whose data should be written.
   * @param row The input data to be written to this line.
   * @param currentLine The line from which the data to be written originates.
   * @param currentFile The file from which the data to be written originates.
   */
  public final void writeLine(QueryHandler queryHandler, Object[] row,
      long currentLine, String currentFile)
  {
    List<Object> line = new ArrayList<Object>();
    if (queryHandler.isValid()) {
      line.add("1");
    } else {
      line.add("0");
    }
    line.add(queryHandler.getStringLength());
    line.add(queryHandler.getStringLengthNoComments());
    line.add(queryHandler.getVariableCountHead());
    line.add(queryHandler.getVariableCountPattern());
    line.add(queryHandler.getTripleCountWithService());
    line.add(-1);
    line.add(currentFile + "_" + currentLine);
    /*
    for (int i = 1; i < row.length; i++) {
      line.add(row[i]);
    }
    */
    writer.writeRow(line);
  }
}
