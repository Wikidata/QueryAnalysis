package general;
/*-
 * #%L
 * sparqlQueryTester
 * %%
 * Copyright (C) 2016 QueryAnalysis
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.FileNotFoundException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import input.InputHandler;
import output.OutputHandler;



/**
 * @author jgonsior
 */
public class Main
{
  /**
   * Define a static logger variable.
   */
  private static Logger logger = Logger.getLogger(Main.class);

  /**
   * Selects the files to be processed and specifies the files to write to.
   * @param args Arguments to specify runtime behavior (non so far).
   */
  public static void main(String[] args)
  {
    PropertyConfigurator.configure("log4j.properties");
    for (String argument : args) {
      if (argument.equals("-logging")) {
        logger.removeAllAppenders();
      }
    }

    //should be changed when it is being run on the server
    for (int i = 1; i <= 30; i++) {
      String inputFile = "QueryCutSept" + String.format("%02d", i) + ".tsv";
      String outputFile = "QueryProcessedSept" +
          String.format("%02d", i) + ".tsv";
      try {
        InputHandler inputHandler = new InputHandler(inputFile);
        try {
          OutputHandler outputHandler = new OutputHandler(outputFile);
          try {
            inputHandler.parseTo(outputHandler);
          } catch (Exception e) {
            logger.error("Unexpected error while parsing " +
                inputFile + ".", e);
          }
          logger.info("Processed " + inputFile + " to " + outputFile + ".");
        } catch (FileNotFoundException e) {
          logger.error("File " + outputFile +
              " could not be created or written to.", e);
        }
      }
      catch (FileNotFoundException e) {
        logger.warn("File " + inputFile + " could not be found.");
      }
    }
  }
}
