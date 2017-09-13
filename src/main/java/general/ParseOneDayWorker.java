package general;


import input.InputHandler;
import openrdffork.TupleExprWrapper;
import org.apache.log4j.Logger;
import output.OutputHandler;
import output.OutputHandlerTSV;
import query.OpenRDFQueryHandler;

import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
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
  private InputHandler inputHandler;
  private OutputHandler outputHandler;
  private int day;
  private Boolean writeQueryTypes;
  private HashMap<TupleExprWrapper, String> queryTypes = new HashMap<TupleExprWrapper, String>();

  public ParseOneDayWorker(InputHandler inputHandlerToSet, int day, String outputFile, Boolean writeQueryTypes) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException
  {

    try {
      outputHandler = new OutputHandlerTSV(outputFile, OpenRDFQueryHandler.class);
    } catch (FileNotFoundException e) {
      logger.error("File " + outputFile + "could not be created or written to.", e);
    }

    this.inputHandler = inputHandlerToSet;
    this.day = day;
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
