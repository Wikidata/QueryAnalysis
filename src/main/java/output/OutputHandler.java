package output;

import java.io.Serializable;

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
   * Saves the amount of queries put by agent_type user.
   */
  protected final Long[] hourly_user = new Long[24];
  /**
   * Saves the amount of queries put by agent_type spider.
   */
  protected final Long[] hourly_spider = new Long[24];

  /**
   * Takes a query and the additional information from input and writes
   * the available data to the active file.
   *
   * @param queryToAnalyze The query that should be analyzed and written.
   * @param validityStatus The validity status which was the result of the decoding process of the URI
   * @param row            The input data to be written to this line.
   * @param currentLine    The line from which the data to be written originates.
   * @param currentFile    The file from which the data to be written originates.
   */
  public abstract void writeLine(String queryToAnalyze, Integer validityStatus, String userAgent, long currentLine, String currentFile);

  /**
   * Closes any files that might have been opened.
   */
  public abstract void closeFiles();
}
