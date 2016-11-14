package input;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import org.apache.log4j.Logger;
import output.OutputHandler;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;

/**
 * @author adrian
 */
public class InputHandler
{
  /**
   * Define a static logger variable so that it references the
   * Logger instance named "InputHandler".
   */
  private static Logger logger = Logger.getLogger(InputHandler.class);
  /**
   * The name of the input file for referencing in the output file.
   */
  private String inputFile;
  /**
   * The reader the parse()-method should read from.
   */
  private Reader reader;

  /**
   * @param fileToRead The file the parse()-method should read from.
   * @throws FileNotFoundException If the file does not exist,
   *                               is a directory rather than a regular file,
   *                               or for some other reason cannot be opened for reading.
   */
  public InputHandler(String fileToRead) throws FileNotFoundException
  {
    this.reader = new InputStreamReader(new FileInputStream(fileToRead));
    this.inputFile = fileToRead;
  }

  /**
   * Read the file given by reader and hands the data to the outputHandler.
   *
   * @param outputHandler Handles the data that should be written.
   */
  public final void parseTo(final OutputHandler outputHandler)
  {
    //read in queries from .tsv
    TsvParserSettings parserSettings = new TsvParserSettings();
    parserSettings.setLineSeparatorDetectionEnabled(true);
    parserSettings.setHeaderExtractionEnabled(true);

    ObjectRowProcessor rowProcessor = new ObjectRowProcessor()
    {
      @Override
      public void rowProcessed(Object[] row, ParsingContext parsingContext)
      {
        String queryString = "";
        try {
          // the url needs to be transformed first into a URL and then later into a URI because the charachter ^
          // which is included in some Queries is apparently an illegal charachter which needs to be encoded
          // differently (which the creation of a URL object first is dealing with)
          URL url = new URL("https://query.wikidata.org/" + row[0]);

          //parse url
          String temp = url.getQuery();
          if (temp == null) return;
          String[] pairs = temp.split("&");
          for (String pair : pairs) {
            int idx = pair.indexOf("=");
            String key = idx > 0 ? URLDecoder.decode(pair.substring(0, idx), "UTF-8") : pair;
            if (key.equals("query")) {
              //find out the query parameter
              queryString = idx > 0 && pair.length() > idx + 1 ? URLDecoder.decode(pair.substring(idx + 1), "UTF-8") : null;
            }
          }

        } catch (MalformedURLException e) {
          logger.error("There was a syntax error in the following URL: " + row[0] + " /nFound at " + inputFile + ", line " + parsingContext.currentLine() + "\n" + e.getMessage());
        } catch (UnsupportedEncodingException e) {
          logger.error("Your system apperently doesn't supports UTF-8 encoding. Please fix this before running this software again.");
        }
        outputHandler.writeLine(queryString, row, parsingContext.currentLine(), inputFile);
      }

    };

    parserSettings.setProcessor(rowProcessor);

    TsvParser parser = new TsvParser(parserSettings);
    parser.parse(reader);
    outputHandler.closeFile();
  }
}
