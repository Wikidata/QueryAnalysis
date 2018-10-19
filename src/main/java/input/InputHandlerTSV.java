package input;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.zip.GZIPInputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import output.OutputHandler;
import query.QueryHandler;
import scala.Tuple2;

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
   * The parser to read the .tsv-Files.
   */
  private CSVParser csvParser;

  /**
   * @param fileToRead The file to read.
   * @throws IOException           If another error occurred.
   */
  public InputHandlerTSV(String fileToRead) throws IOException
  {
    super(fileToRead);
  }

  /**
   * @param fileToRead The file the parse()-method should read from.
   * @throws IOException           if an I/O error has occurredd
   */
  public void setInputFile(String fileToRead) throws IOException
  {
    this.inputFile = fileToRead;
    this.reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(fileToRead))));
    csvParser = new CSVParser(reader,
        CSVFormat.newFormat('\t')
        .withFirstRecordAsHeader()
        .withIgnoreHeaderCase()
        .withIgnoreEmptyLines()
        .withTrim());
  }

  /**
   * Read the file given by reader and hands the data to the outputHandler.
   * @param day The day currently being processed.
   * @param outputHandler Handles the data that should be written.
   * @throws IOException If there was an error during writing.
   */
  public final void parseTo(final OutputHandler outputHandler, int day) throws IOException
  {
    for (CSVRecord csvRecord : csvParser) {
      if (csvRecord.size() <= 1) {
        logger.warn("Ignoring line without tab while parsing.");
        return;
      }
      Long line = csvRecord.getRecordNumber();
      Tuple2<String, QueryHandler.Validity> queryTuple = decode(csvRecord.get("uri_query").toString(), inputFile, line);

      String queryString = queryTuple._1;
      QueryHandler.Validity validity = queryTuple._2;
      String userAgent = "null";
      String possibleUserAgent = csvRecord.get("user_agent");
      if (possibleUserAgent != null)
        userAgent = possibleUserAgent;
      String timeStamp = "null";
      String possibleTimeStamp = csvRecord.get("ts");
      if (possibleTimeStamp != null)
        timeStamp = possibleTimeStamp;
      try {
        outputHandler.writeLine(queryString, validity, userAgent, timeStamp, line, day, inputFile);
      } catch (NullPointerException e) {
        outputHandler.writeLine("", QueryHandler.Validity.INTERNAL_ERROR, userAgent, timeStamp, line, day, inputFile);
        logger.error("Unexpected Null Pointer Exception in writeLine.", e);
      }
    }

    outputHandler.closeFiles();
  }
}
