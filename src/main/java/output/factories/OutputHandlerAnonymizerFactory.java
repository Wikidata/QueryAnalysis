/**
 *
 */
package output.factories;

import output.OutputHandler;
import output.OutputHandlerAnonymizer;
import query.factories.QueryHandlerFactory;

import java.io.FileNotFoundException;

/**
 * @author adrian
 */
public class OutputHandlerAnonymizerFactory implements OutputHandlerFactory
{
  /**
   * @param outputFile          The file the output should be written to.
   * @param queryHandlerFactory The factory supplying the query handler to generate the output with.
   * @return An output handler anonymizer based on the parameters.
   * @throws FileNotFoundException If the file could not be created or written to.
   */
  @Override
  public OutputHandler getOutputHandler(String outputFile, QueryHandlerFactory queryHandlerFactory) throws FileNotFoundException
  {
    return new OutputHandlerAnonymizer(outputFile, queryHandlerFactory);
  }

}
