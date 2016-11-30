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
import input.InputHandlerTSV;
import logging.LoggingHandler;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import output.OutputHandlerTSV;
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
    //args = new String[] {"-jl", "-file test/test/test/QueryCntSept"};

    Options options = new Options();
    options.addOption("l", "logging", false, "enables file logging");
    options.addOption("j", "jena", false, "uses the Jena SPARQL Parser");
    options.addOption("o", "openrdf", false, "uses the OpenRDF SPARQL Parser");
    options.addOption("f", "file", true, "defines the input file prefix");
    options.addOption("h", "help", false, "displays this help");
    options.addOption("t", "tsv", false, "reads from .tsv-files");

    //some parameters which can be changed through parameters
    QueryHandler queryHandler = new OpenRDFQueryHandler();
    String inputFilePrefix;
    String inputFileSuffix = ".tsv";
    String queryParserName = "OpenRDF";

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
      if (cmd.hasOption("help")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("help", options);
        return;
      }
      if (cmd.hasOption("logging")) {
        LoggingHandler.initFileLog();
      }
      if (cmd.hasOption("jena")) {
        queryHandler = new JenaQueryHandler();
        queryParserName = "Jena";
      }
      if (cmd.hasOption("openrdf")) {
        queryHandler = new OpenRDFQueryHandler();
      }
      if (cmd.hasOption("tsv")) {
        inputFileSuffix = ".tsv";
      }
      if (cmd.hasOption("file")) {
        inputFilePrefix = cmd.getOptionValue("file").trim();
      } else {
        System.out.println("Please specify at least the file which we should work on using the option '--file PREFIX' or 'f PREFIX'");
        return;
      }
    } catch (UnrecognizedOptionException e) {
      System.out.println("Unrecognized commandline option: " + e.getOption());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("help", options);
      return;

    } catch (ParseException e) {
      System.out.println("There was an error while parsing your command line input. Did you rechecked your syntax before running?");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("help", options);
      return;
    }

    LoggingHandler.initConsoleLog();

    for (int i = 1; i <= 30; i++) {
      String inputFile = inputFilePrefix + String.format("%02d", i) + inputFileSuffix;

      //create directory for the output
      String outputFolderName = inputFilePrefix.substring(0, inputFilePrefix.lastIndexOf('/'));
      String outputFile = outputFolderName + "/QueryProcessed" + queryParserName + String.format("%02d", i) + ".tsv";
      try {
        InputHandler inputHandler;
        inputHandler = new InputHandlerTSV(inputFile);
        logger.info("Start processing " + inputFile);
        try {
          OutputHandlerTSV outputHandler = new OutputHandlerTSV(outputFile, queryHandler);
          try {
            inputHandler.parseTo(outputHandler);
          } catch (Exception e) {
            logger.error("Unexpected error while parsing " + inputFile + ".", e);
          }
          logger.info("Done processing " + inputFile + " to " + outputFile + ".");
        } catch (FileNotFoundException e) {
          logger.error("File " + outputFile + "could not be created or written to.", e);
        }
      } catch (FileNotFoundException e) {
        logger.warn("File " + inputFile + " could not be found.");
      }
    }
  }
}