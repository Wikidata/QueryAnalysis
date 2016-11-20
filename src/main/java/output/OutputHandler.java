package output;

import java.util.List;

/**
 * @author adrian
 *
 */
public abstract class OutputHandler
{
  /**
   * Takes a query and the additional information from input and writes
   * the available data to the active file.
   *
   * @param queryToAnalyze The query that should be analyzed and written.
   * @param row            The input data to be written to this line.
   * @param currentLine    The line from which the data to be written originates.
   * @param currentFile    The file from which the data to be written originates.
   */
  public abstract List<Object> getLine(String queryToAnalyze, Object[] row, long currentLine, String currentFile);
  
  /**
   * @param line Line to write.
   */
  public abstract void writeLine(List<Object> line);

  /**
   * Closes any files that might have been opened.
   */
  public abstract void closeFiles();
}
