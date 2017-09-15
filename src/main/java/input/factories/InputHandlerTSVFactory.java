/**
 * 
 */
package input.factories;

import java.io.IOException;

import input.InputHandler;
import input.InputHandlerTSV;

/**
 * @author adrian
 *
 */
public class InputHandlerTSVFactory implements InputHandlerFactory
{

  /**
   * @param inputFile The input file to read from.
   * @return An input handler tsv based on the parameters.
   * @throws IOException If the file specified in inputFile could not be found or read from.
   */
  @Override
  public InputHandler getInputHandler(String inputFile) throws IOException
  {
    return new InputHandlerTSV(inputFile);
  }

}
