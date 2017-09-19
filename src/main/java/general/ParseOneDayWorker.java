package general;


import input.InputHandler;
import input.factories.InputHandlerFactory;
import openrdffork.TupleExprWrapper;
import org.apache.log4j.Logger;
import output.OutputHandler;
import output.factories.OutputHandlerFactory;
import query.factories.QueryHandlerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Julius Gonsior
 */
public class ParseOneDayWorker implements Runnable
{
  /**
   * Define a static logger variable.
   */
  private static final Logger logger = Logger.getLogger(ParseOneDayWorker.class);
  /**
   * The inputHandler to process from.
   */
  private InputHandler inputHandler;
  /**
   * The output handler to process to.
   */
  private OutputHandler outputHandler;
  /**
   * The day this worker is processing.
   */
  private int day;
  /**
   * If the query types should be written out.
   */
  private Boolean writeQueryTypes;
  /**
   * A map for holding all query types found on this day.
   */
  private HashMap<TupleExprWrapper, String> queryTypes = new HashMap<TupleExprWrapper, String>();

  /**
   * @param inputHandlerFactory  The input handler factory to supply the input handler.
   * @param inputFile            The file to read from.
   * @param outputHandlerFactory The output handler factory to supply the output handler.
   * @param outputFile           The file to write to.
   * @param queryHandlerFactory  The query handler factory to supply the query handler.
   * @param dayToSet             The day being processed.
   * @param writeQueryTypes      If the query types should be written or not.
   * @throws IOException If the input or output handler could not be created.
   */
  public ParseOneDayWorker(InputHandlerFactory inputHandlerFactory, String inputFile, OutputHandlerFactory outputHandlerFactory, String outputFile, QueryHandlerFactory queryHandlerFactory, int dayToSet, Boolean writeQueryTypes) throws IOException
  {
    try {
      this.inputHandler = inputHandlerFactory.getInputHandler(inputFile);
    } catch (IOException e) {
      logger.warn("File " + inputFile + " could not be found.");
      throw e;
    }
    try {
      this.outputHandler = outputHandlerFactory.getOutputHandler(outputFile, queryHandlerFactory);
    } catch (FileNotFoundException e) {
      logger.error("File " + outputFile + "could not be created or written to.", e);
      throw e;
    }
    this.day = dayToSet;
    this.writeQueryTypes = writeQueryTypes;
    for (Map.Entry<TupleExprWrapper, String> entry : Main.queryTypes.entrySet()) {
      this.queryTypes.put(entry.getKey(), entry.getValue());
    }
  }


  @Override
  public void run()
  {
    logger.info("Start processing " + inputHandler.getInputFile());
    outputHandler.setThreadNumber(day);
    outputHandler.setQueryTypes(queryTypes);
    inputHandler.parseTo(outputHandler, day);
    logger.info("Done processing " + inputHandler.getInputFile() + " to " + outputHandler.getOutputFile() + ".");

    if (writeQueryTypes) {
      Main.writeQueryTypes(queryTypes);
    }
  }
}
