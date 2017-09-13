package output;

import openrdffork.TupleExprWrapper;
import query.QueryHandler;

import java.io.FileNotFoundException;
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
   * The output file this handler writes to.
   */
  protected String outputFile;

  /**
   * @return The output file this handler writes to.
   */
  public String getOutputFile()
  {
    return outputFile;
  }

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
   * @param fileToWrite            The file to write the output to.
   * @param queryHandlerClassToSet The query Handler class to use for generating the output.
   * @throws FileNotFoundException If there was some error concerning the file to write to.
   */
  public OutputHandler(String fileToWrite, Class queryHandlerClassToSet) throws FileNotFoundException
  {
    this.outputFile = fileToWrite;
    this.initialize(fileToWrite, queryHandlerClassToSet);
  }

  /**
   * Method to be called before using writeLine().
   *
   * @param fileToWrite            The file to write the output to.
   * @param queryHandlerClassToSet The query Handler class to use for generating the output.
   * @throws FileNotFoundException If there was some error concerning the file to write to.
   */
  public abstract void initialize(String fileToWrite, Class queryHandlerClassToSet) throws FileNotFoundException;

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
  public abstract void writeLine(String queryToAnalyze, QueryHandler.Validity validityStatus, String userAgent, long currentLine, int currentDay, String currentFile);

  /**
   * Closes any files that might have been opened.
   */
  public abstract void closeFiles();
}
