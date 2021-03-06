package output;

import openrdffork.TupleExprWrapper;
import query.QueryHandler;
import query.factories.QueryHandlerFactory;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.apache.commons.csv.CSVPrinter;

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
   * The csv-handler.
   */
  protected CSVPrinter csvPrinter;

  /**
   * A writer created at object creation to be used in line-by-line writing.
   */
  protected BufferedWriter bufferedWriter;

  /**
   * @param fileToWrite            The file to write the output to.
   * @param queryHandlerFactoryToSet The query Handler factory to use for generating the output.
   * @throws IOException If there was some error concerning the file to write to.
   */
  public OutputHandler(String fileToWrite, QueryHandlerFactory queryHandlerFactoryToSet) throws IOException
  {
    this.outputFile = fileToWrite;
    this.initialize(fileToWrite, queryHandlerFactoryToSet);
  }

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
   * Method to be called before using writeLine().
   *
   * @param fileToWrite              The file to write the output to.
   * @param queryHandlerFactoryToSet The query handler factory to supply the query handler for generating the output.
   * @throws IOException If there was some error concerning the file to write to.
   */
  public abstract void initialize(String fileToWrite, QueryHandlerFactory queryHandlerFactoryToSet) throws IOException;

  /**
   * Takes a query and the additional information from input and writes
   * the available data to the active file.
   *
   * @param queryToAnalyze The query that should be analyzed and written.
   * @param validityStatus The validity status which was the result of the decoding process of the URI
   * @param userAgent      The user agent with which the query was being executed as.
   * @param timeStamp      The time stamp of the query.
   * @param currentLine    The line from which the data to be written originates.
   * @param currentDay     The day from which the data to be written originates.
   * @param currentFile    The file from which the data to be written originates.
   * @throws IOException If there was an error during writing.
   */
  public abstract void writeLine(String queryToAnalyze, QueryHandler.Validity validityStatus, String userAgent, String timeStamp, long currentLine, int currentDay, String currentFile) throws IOException;

  /**
   * Closes any files that might have been opened.
   */
  public abstract void closeFiles();
}
