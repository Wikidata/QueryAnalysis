package input;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import org.apache.log4j.Logger;

import output.OutputHandler;
import output.OutputHandlerTSV;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;

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
   * {@inheritDoc}
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
        String queryString = decode(row[0].toString(), inputFile, String.valueOf(parsingContext.currentLine()));
        outputHandler.writeLine(outputHandler.getLine(queryString, row, parsingContext.currentLine(), inputFile));
      }

    };

    parserSettings.setProcessor(rowProcessor);

    TsvParser parser = new TsvParser(parserSettings);
    parser.parse(reader);
    outputHandler.closeFiles();
  }

  @Override
  public void setInputFile(String fileToRead) throws FileNotFoundException
  {
    this.inputFile = fileToRead;
    this.reader = new InputStreamReader(new FileInputStream(fileToRead));
  }
}
