package output.factories;

import output.OutputHandler;
import output.OutputHandlerTSV;
import query.factories.QueryHandlerFactory;

import java.io.FileNotFoundException;

/**
 * @author adrian
 */
public class OutputHandlerTSVFactory implements OutputHandlerFactory
{

  /**
   * @param outputFile          The file the output should be written to.
   * @param queryHandlerFactory The factory supplying the query handler to generate the output with.
   * @return An output handler tsv based on the parameters.
   * @throws FileNotFoundException If the file could not be created or written to.
   */
  @Override
  public OutputHandler getOutputHandler(String outputFile, QueryHandlerFactory queryHandlerFactory) throws FileNotFoundException
  {
    return new OutputHandlerTSV(outputFile, queryHandlerFactory);
  }

}
