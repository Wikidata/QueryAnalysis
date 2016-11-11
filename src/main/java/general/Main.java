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

import input.InputHandler;
import logging.TimestampFileAppender;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.varia.LevelRangeFilter;

import output.OutputHandler;
import query.JenaQueryHandler;
import query.OpenRDFQueryHandler;
import query.QueryHandler;


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
   *
   * @param args Arguments to specify runtime behavior (non so far).
   */
  public static void main(String[] args)
  {
    QueryHandler queryHandler = new OpenRDFQueryHandler();
    for (String argument : args) {
      if (argument.equals("-logging")) {
        initFileLog();
      }
      if (argument.equals("-jena")) queryHandler = new JenaQueryHandler();
      if (argument.equals("-openrdf")) queryHandler = new OpenRDFQueryHandler();
    }
    initConsoleLog();

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

  /**
   * Defines an appender that writes all log messages to the file general.%timestamp.log.
   */
  public static void initFileLog()
  {
    TimestampFileAppender fileAppender = new TimestampFileAppender();
    fileAppender.setName("FileLogger");
    fileAppender.setLayout(new PatternLayout("%-4r [%t] %-5p %c %x - %m%n"));
    fileAppender.setFile("logs/general.%timestamp.log");
    fileAppender.activateOptions();
    Logger.getRootLogger().addAppender(fileAppender);
  }

  /**
   * Defines an appender that writes INFO log messages to the console.
   */
  public static void initConsoleLog()
  {
    ConsoleAppender consoleAppender = new ConsoleAppender();
    consoleAppender.setName("ConsoleLogger");
    consoleAppender.setLayout(new PatternLayout("%-4r [%t] %-5p %c %x - %m%n"));
    LevelRangeFilter levelRangeFilter = new LevelRangeFilter();
    levelRangeFilter.setLevelMax(Level.INFO);
    levelRangeFilter.setLevelMin(Level.INFO);
    consoleAppender.addFilter(levelRangeFilter);
    consoleAppender.activateOptions();
    Logger.getRootLogger().addAppender(consoleAppender);
  }
}
