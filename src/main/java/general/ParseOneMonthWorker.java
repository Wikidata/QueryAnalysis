package general;


import input.InputHandler;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import output.OutputHandlerTSV;
import query.QueryHandler;

import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;

/**
 * @author: Julius Gonsior
 */
public class ParseOneMonthWorker implements Runnable
{
  /**
   * Define a static logger variable.
   */
  private static Logger logger = Logger.getLogger(ParseOneMonthWorker.class);
  private String inputFile;
  private String inputFilePrefix;
  private InputHandler inputHandler;
  private String queryParserName;
  private QueryHandler queryHandler;
  private int day;

  public ParseOneMonthWorker(String inputFile, String inputFilePrefix, Class inputHandlerClass, String queryParserName, Class queryHandlerClass, int day) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException
  {
    this.inputFile = inputFile;
    this.inputFilePrefix = inputFilePrefix;
    this.inputHandler = (InputHandler) inputHandlerClass.getConstructor().newInstance();
    this.queryParserName = queryParserName;
    this.queryHandler = (QueryHandler) queryHandlerClass.getConstructor().newInstance();
    this.day = day;
  }


  @Override
  public void run()
  {
    //create directory for the output
    String outputFolderName = inputFilePrefix.substring(0, inputFilePrefix.lastIndexOf('/'));
    String outputFile = outputFolderName + "/QueryProcessed" + queryParserName + String.format("%02d", day);
    try {
      inputHandler.setInputFile(inputFile);

      logger.info("Start processing " + inputFile);
      try {
        OutputHandlerTSV outputHandler = new OutputHandlerTSV(outputFile, queryHandler);
        //try {
        inputHandler.parseTo(outputHandler);
        logger.info("Done processing " + inputFile + " to " + outputFile + ".");
        //   } catch (Exception e) {
        //    logger.error("Unexpected error while parsing " + inputFile + ".", e);
        //  }
      } catch (FileNotFoundException e) {
        logger.error("File " + outputFile + "could not be created or written to.", e);
      }
    } catch (FileNotFoundException e) {
      logger.warn("File " + inputFile + " could not be found.");
    } catch (AnalysisException e) {
      logger.warn("File " + inputFile + " could not be found.");
    }
  }

}
