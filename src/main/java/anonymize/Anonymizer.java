/**
 * 
 */
package anonymize;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.Files.readAllBytes;

import openrdffork.StandardizingSPARQLParser;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.queryrender.sparql.SPARQLQueryRenderer;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import general.Main;
import logging.LoggingHandler;

/**
 * @author adrian
 *
 */
public class Anonymizer
{

  /**
   * Define a static logger variable.
   */
  private static final Logger logger = Logger.getLogger(Main.class);

  /**
   * @param args
   */
  public static void main(String[] args)
  {
    loadStandardPrefixes();

    LoggingHandler.initFileLog("Anonymizer", "nothing");

    int worked = 0;
    int failed = 0;
    int failedToParse = 0;

    try (DirectoryStream<Path> directoryStream =
        Files.newDirectoryStream(Paths.get("/home/adrian/workspace/java/months/inputData/processedLogData/exampleQueries/"))) {
      for (Path filePath : directoryStream) {
        if (Files.isRegularFile(filePath)) {
          String queryString = new String(readAllBytes(filePath));
          String renderedQueryString;
          try {
            ParsedQuery parsedQuery = new StandardizingSPARQLParser().parseQuery(queryString, query.OpenRDFQueryHandler.BASE_URI);
            renderedQueryString = new SPARQLQueryRenderer().render(parsedQuery);
          }
          catch (MalformedQueryException e) {
            failedToParse += 1;
            if (!e.toString().matches(".*projection alias .* was previously used.*")) {
              String filePathString = filePath.toString();
              System.out.println(filePathString.substring(filePathString.lastIndexOf("/"), filePathString.length()));
              System.out.println(e.getMessage());
            }
            continue;
          }
          catch (Exception e) {
            // e.printStackTrace();
            continue;
          }
          try {
            ParsedQuery parsedRenderedQuery = new StandardizingSPARQLParser().parseQuery(renderedQueryString, query.OpenRDFQueryHandler.BASE_URI);
            worked += 1;
          }
          catch (MalformedQueryException e) {
            failed += 1;
          }
        }
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }

    System.out.println("Worked: " + worked + " Failed: " + failed + " Failed to parse: " + failedToParse);
  }

  /**
   * Loads all standard prefixes.
   */
  private static void loadStandardPrefixes()
  {
    TsvParserSettings parserSettings = new TsvParserSettings();
    parserSettings.setLineSeparatorDetectionEnabled(true);
    parserSettings.setHeaderExtractionEnabled(true);
    parserSettings.setSkipEmptyLines(true);
    parserSettings.setReadInputOnSeparateThread(true);

    ObjectRowProcessor rowProcessor = new ObjectRowProcessor()
    {
      @Override
      public void rowProcessed(Object[] row, ParsingContext parsingContext)
      {
        if (row.length <= 1) {
          logger.warn("Ignoring line without tab while parsing.");
          return;
        }
        if (row.length == 3) {
          try {
            Main.prefixes.put(row[0].toString(), row[1].toString());
          } catch (IllegalArgumentException e) {
            logger.error("Prefix or uri for standard prefixes defined multiple times", e);
          }
          return;
        }
        logger.warn("Line with row length " + row.length + " found. Is the formatting of toolMapping.tsv correct?");
        return;
      }

    };

    parserSettings.setProcessor(rowProcessor);

    TsvParser parser = new TsvParser(parserSettings);

    try {
      parser.parse(new InputStreamReader(new FileInputStream("parserSettings/standardPrefixes.tsv")));
    } catch (FileNotFoundException e) {
      logger.error("Could not open configuration file for standard prefixes.", e);
    }
  }
}