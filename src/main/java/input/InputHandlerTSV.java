package input;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import com.univocity.parsers.tsv.TsvWriter;
import org.apache.log4j.Logger;
import output.OutputHandler;
import query.QueryHandler;
import scala.Tuple2;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * @author adrian
 */
public class InputHandlerTSV extends InputHandler
{
  /**
   * Define a static logger variable so that it references the
   * Logger instance named "InputHandler".
   */
  private static final Logger logger = Logger.getLogger(InputHandlerTSV.class);

  /**
   * The reader the parse()-method should read from.
   */
  private Reader reader;

  /**
   * The file for writing the pre-processed data.
   */
  private TsvWriter preprocessedWriter;

  /**
   * @param fileToRead The file the parse()-method should read from.
   * @throws IOException           if an I/O error has occurred
   * @throws FileNotFoundException If the file does not exist,
   *                               is a directory rather than a regular file,
   *                               or for some other reason cannot be opened for reading.
   */
  public void setInputFile(String fileToRead) throws IOException
  {
    this.inputFile = fileToRead;
    this.reader = new InputStreamReader(new GZIPInputStream(new FileInputStream(fileToRead)));
  }

  /**
   * Read the file given by reader and hands the data to the outputHandler.
   *
   * @param outputHandler Handles the data that should be written.
   */
  public final void parseTo(final OutputHandler outputHandler, int day)
  {
    //read in queries from .tsv
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
        Tuple2<String, QueryHandler.Validity> queryTuple = decode(row[0].toString(), inputFile, parsingContext.currentLine());

        String queryString = queryTuple._1;
        QueryHandler.Validity validity = queryTuple._2;
        String userAgent = row[2].toString();
        Long line = parsingContext.currentLine();

        try {
          outputHandler.writeLine(queryString, validity, userAgent, line, day, inputFile);
        } catch (NullPointerException e) {
          outputHandler.writeLine("", QueryHandler.Validity.INTERNAL_ERROR, userAgent, line, day, inputFile);
          logger.error("Unexpected Null Pointer Exception in writeLine.", e);
        }
      }
    };

    parserSettings.setProcessor(rowProcessor);

    TsvParser parser = new TsvParser(parserSettings);

    parser.parse(reader);

    outputHandler.closeFiles();

    if (preprocessedWriter != null) {
      preprocessedWriter.close();
    }
  }
}
