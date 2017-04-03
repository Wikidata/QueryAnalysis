package input;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import com.univocity.parsers.tsv.TsvWriter;
import com.univocity.parsers.tsv.TsvWriterSettings;
import general.Main;
import org.apache.log4j.Logger;
import output.OutputHandler;
import query.QueryHandler;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

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
   * @throws FileNotFoundException If the file does not exist,
   *                               is a directory rather than a regular file,
   *                               or for some other reason cannot be opened for reading.
   */
  public void setInputFile(String fileToRead) throws FileNotFoundException
  {
    this.inputFile = fileToRead;
    this.reader = new InputStreamReader(new FileInputStream(fileToRead));
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
    parserSettings.setSkipEmptyLines(true);
    parserSettings.setReadInputOnSeparateThread(true);

    if (!Main.readPreprocessed) {
      try {
        String file = inputFile.substring(0, inputFile.lastIndexOf("/") + 1) + "Preprocessed" + inputFile.substring(inputFile.lastIndexOf("/") + 1);
        FileOutputStream tempFile = new FileOutputStream(file);
        preprocessedWriter = new TsvWriter(tempFile, new TsvWriterSettings());
      } catch (FileNotFoundException e) {
        logger.error("Could not create temporary file for adding prefixes", e);
      }
    }

    ObjectRowProcessor rowProcessor = new ObjectRowProcessor()
    {

      private boolean headersWritten;

      @Override
      public void rowProcessed(Object[] row, ParsingContext parsingContext)
      {
        if (row.length <= 1) {
          logger.warn("Ignoring line without tab while parsing.");
          return;
        }

        if (Main.readPreprocessed) {
          String preprocessedQuery = row[0].toString();
          Integer preprocessedValidity;
          try {
            preprocessedValidity = Integer.valueOf(row[1].toString());
          } catch (NumberFormatException e) {
            logger.warn("Could not parse temp_validity, is the file preprocessed?", e);
            return;
          }
          outputHandler.writeLine(preprocessedQuery, preprocessedValidity, row[3].toString(), parsingContext.currentLine(), inputFile);
        } else {
          Tuple2<String, Integer> queryTuple = decode(row[0].toString(), inputFile, parsingContext.currentLine());

          if (preprocessedWriter != null) {

            if (!headersWritten) {
              ArrayList<String> headers = new ArrayList<String>();
              headers.add("preprocessed_query");
              headers.add("temp_validity");
              String[] parsedHeaders = parsingContext.parsedHeaders();
              headers.addAll(Arrays.asList(parsedHeaders).subList(1, parsedHeaders.length));
              preprocessedWriter.writeHeaders(headers);
              headersWritten = true;
            }

            ArrayList<Object> rowToWrite = new ArrayList<Object>();
            rowToWrite.add(QueryHandler.addMissingPrefixesToQuery(queryTuple._1));
            rowToWrite.add(queryTuple._2);
            rowToWrite.addAll(Arrays.asList(row).subList(1, row.length));
            preprocessedWriter.writeRow(rowToWrite);
          }
          outputHandler.writeLine(queryTuple._1, queryTuple._2, row[2].toString(), parsingContext.currentLine(), inputFile);
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
