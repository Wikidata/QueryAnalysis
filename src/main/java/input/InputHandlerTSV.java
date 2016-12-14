package input;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import org.apache.log4j.Logger;
import output.OutputHandler;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * @author adrian
 */
public class InputHandlerTSV extends InputHandler
{
  /**
   * Define a static logger variable so that it references the
   * Logger instance named "InputHandler".
   */
  private static Logger logger = Logger.getLogger(InputHandlerTSV.class);
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
  public InputHandlerTSV(String fileToRead) throws FileNotFoundException
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
        try {
          String queryString = decode(row[0].toString(), inputFile, parsingContext.currentLine());
          outputHandler.writeLine(queryString, row, parsingContext.currentLine(), inputFile);
        } catch (IllegalArgumentException e) {
          logger.error("There was an error while parsing the following URL: " + row[0].toString() + " /nFound at " + inputFile + ", line " + parsingContext.currentLine() + "\n" + e.getMessage());
        }
      }

    };

    parserSettings.setProcessor(rowProcessor);

    TsvParser parser = new TsvParser(parserSettings);

    parser.parse(reader);


    outputHandler.closeFiles();
  }
}
