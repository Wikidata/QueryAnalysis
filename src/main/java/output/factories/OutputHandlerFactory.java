package output.factories;

import java.io.FileNotFoundException;

import output.OutputHandler;
import query.factories.QueryHandlerFactory;

/**
 * @author adrian
 *
 */
public interface OutputHandlerFactory
{
  /**
   * @param outputFile The file the output should be written to.
   * @param queryHandlerFactory The factory supplying the query handler to generate the output with.
   * @return An output handler corresponding to this factory based on the parameters.
   * @throws FileNotFoundException If the file could not be created or written to.
   */
  OutputHandler getOutputHandler(String outputFile, QueryHandlerFactory queryHandlerFactory) throws FileNotFoundException;
}
