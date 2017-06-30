package output;

import openrdffork.TupleExprWrapper;
import query.QueryHandler;

import java.io.Serializable;
import java.util.Map;

/**
 * @author adrian
 */
public abstract class OutputHandler implements Serializable
{
  /**
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * The number of the thread (needs to be unique for one run).
   */
  protected int threadNumber = -1;
  /**
   * The map holding the query types.
   */
  protected Map<TupleExprWrapper, String> queryTypes;

  /**
   * @param threadNumberToSet {@link #threadNumber}
   */
  public void setThreadNumber(int threadNumberToSet)
  {
    this.threadNumber = threadNumberToSet;
  }

  /**
   * @param queryTypesToSet {@link #queryTypes}
   */
  public void setQueryTypes(Map<TupleExprWrapper, String> queryTypesToSet)
  {
    this.queryTypes = queryTypesToSet;
  }

  /**
   * Takes a query and the additional information from input and writes
   * the available data to the active file.
   *
   * @param queryToAnalyze The query that should be analyzed and written.
   * @param validityStatus The validity status which was the result of the decoding process of the URI
   * @param userAgent      The user agent with which the query was being executed as.
   * @param currentLine    The line from which the data to be written originates.
   * @param currentFile    The file from which the data to be written originates.
   */
  public abstract void writeLine(String queryToAnalyze, QueryHandler.Validity validityStatus, String userAgent, long currentLine, String currentFile);

  /**
   * Closes any files that might have been opened.
   */
  public abstract void closeFiles();
}
