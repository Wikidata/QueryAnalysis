package input;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.log4j.Logger;

import output.OutputHandler;
import query.QueryHandler;

/**
 *
 * @author adrian
 *
 */
public class InputHandler
{
  /** Define a static logger variable so that it references the
   * Logger instance named "Main".
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
   * is a directory rather than a regular file,
   * or for some other reason cannot be opened for reading.
   */
  public InputHandler(String fileToRead) throws FileNotFoundException
  {
    this.reader = new InputStreamReader(new FileInputStream(fileToRead));
    this.inputFile = fileToRead;
  }

  /**
   * Read the file given by reader and hands the data to the outputHandler.
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
        String queryString = null;
        try {
          //parse url
          List<NameValuePair> params = URLEncodedUtils.parse(
              new URI((String) row[0]), "UTF-8");

          //find out the query parameter
          for (NameValuePair param : params) {
            if (param.getName().equals("query")) {
              queryString = param.getValue();
            }
          }
        } catch (URISyntaxException e) {
          logger.warn("There was a syntax error in the following URI: " +
              row[0]);
        }
        QueryHandler queryHandler = new QueryHandler(queryString);

        outputHandler.writeLine(queryHandler, row,
            parsingContext.currentLine(), inputFile);
      }

    };

    parserSettings.setProcessor(rowProcessor);

    TsvParser parser = new TsvParser(parserSettings);
    parser.parse(reader);
    outputHandler.closeFile();
  }
}
