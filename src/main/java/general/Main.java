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

import input.InputHandler;
import logging.LoggingHandler;
import org.apache.log4j.Logger;
import output.OutputHandler;
import query.JenaQueryHandler;
import query.OpenRDFQueryHandler;
import query.QueryHandler;

import java.io.FileNotFoundException;


/**
 * @author jgonsior
 */
public final class Main
{
  /**
   * Define a static logger variable.
   */
  private static Logger logger = Logger.getLogger(Main.class);

  /**
   * Since this is a utility class, it should not be instantiated.
   */
  private Main()
  {
    throw new AssertionError("Instantiating utility class Main");
  }

  /**
   * Selects the files to be processed and specifies the files to write to.
   *
   * @param args Arguments to specify runtime behavior.
   */
  public static void main(String[] args)
  {
    QueryHandler queryHandler = new OpenRDFQueryHandler();
    for (String argument : args) {
      if (argument.equals("-logging")) {
        LoggingHandler.initFileLog();
      }
      if (argument.equals("-jena")) queryHandler = new JenaQueryHandler();
      if (argument.equals("-openrdf")) queryHandler = new OpenRDFQueryHandler();
    }

    LoggingHandler.initConsoleLog();

    for (int i = 1; i <= 30; i++) {
      String inputFile = "QueryCnt" + String.format("%02d", i) + ".tsv";
      String outputFile = "QueryProcessedSept" +
          String.format("%02d", i) + ".tsv";
      try {
        InputHandler inputHandler = new InputHandler(inputFile);
        try {
          OutputHandler outputHandler = new OutputHandler(outputFile, queryHandler);
          try {
            inputHandler.parseTo(outputHandler);
          } catch (Exception e) {
            logger.error("Unexpected error while parsing " + inputFile + ".", e);
          }
          logger.info("Processed " + inputFile + " to " + outputFile + ".");
        } catch (FileNotFoundException e) {
          logger.error("File " + outputFile + "could not be created or written to.", e);
        }
      } catch (FileNotFoundException e) {
        logger.warn("File " + inputFile + " could not be found.");
      }
    }
  }
}