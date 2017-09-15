package input.factories;

import java.io.IOException;

import input.InputHandler;

/**
 * @author adrian
 *
 */
public interface InputHandlerFactory
{
  /**
   * @param inputFile The input file to read from.
   * @return An input handler corresponding to this factory based on the parameters.
   * @throws IOException If the file specified in inputFile could not be found or read from.
   */
  InputHandler getInputHandler(String inputFile) throws IOException;
}
