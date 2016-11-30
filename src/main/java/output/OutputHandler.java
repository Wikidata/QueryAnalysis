package output;

import java.io.Serializable;
import java.util.List;

/**
 * @author adrian
 *
 */
public abstract class OutputHandler implements Serializable
{
  /**
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * Takes a query and the additional information from input and writes
   * the available data to the active file.
   *
   * @param queryToAnalyze The query that should be analyzed and written.
   * @param row            The input data to be written to this line.
   * @param currentLine    The line from which the data to be written originates.
   * @param currentFile    The file from which the data to be written originates.
   */
  public abstract void writeLine(String queryToAnalyze, Object[] row, long currentLine, String currentFile);

  /**
   * Closes any files that might have been opened.
   */
  public abstract void closeFiles();
}
