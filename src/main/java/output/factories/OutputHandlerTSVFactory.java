package output.factories;

import output.OutputHandler;
import output.OutputHandlerTSV;
import query.factories.QueryHandlerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author adrian
 */
public class OutputHandlerTSVFactory implements OutputHandlerFactory
{

  /**
   * @param outputFile          The file the output should be written to.
   * @param queryHandlerFactory The factory supplying the query handler to generate the output with.
   * @return An output handler tsv based on the parameters.
   * @throws IOException If the necessary files could not be created.
   */
  @Override
  public OutputHandler getOutputHandler(String outputFile, QueryHandlerFactory queryHandlerFactory) throws IOException
  {
    return new OutputHandlerTSV(outputFile, queryHandlerFactory);
  }

}
