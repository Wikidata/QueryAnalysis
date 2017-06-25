package input;

import org.apache.log4j.Logger;
import output.OutputHandler;
import query.QueryHandler;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;

/**
 * @author adrian
 */
public abstract class InputHandler
{
  /**
   * Define a static logger variable.
   */
  private static final Logger logger = Logger.getLogger(InputHandler.class);
  /**
   * The name of the input file for referencing in the output file.
   */
  protected String inputFile;

  /**
   * Read the file given by reader and hands the data to the outputHandler.
   *
   * @param outputHandler Handles the data that should be written.
   */
  public abstract void parseTo(OutputHandler outputHandler);

  public abstract void setInputFile(String fileToRead) throws FileNotFoundException;

  /**
   * @param uriQuery  The uri_query entry from the logs.
   * @param inputFile The file this uri_query was read from.
   * @return The query as a Tuple2 Object, ._1 containing the pure string ._2 the validity code
   */
  public final Tuple2<String, QueryHandler.Validity> decode(String uriQuery, String inputFile, long line)
  {

    String queryString = "";
    QueryHandler.Validity validityStatus = QueryHandler.Validity.INVALID;
    try {
      // the url needs to be transformed first into a URL and then later into a URI because the charachter ^
      // which is included in some Queries is apparently an illegal charachter which needs to be encoded
      // differently (which the creation of a URL object first is dealing with)
      URL url = new URL("https://query.wikidata.org/" + uriQuery);

      //parse url
      String temp = url.getQuery();
      if (temp == null) {
        return new Tuple2<>("", QueryHandler.Validity.INVALID_EMTPY_QUERY_STRING);
      }
      String[] pairs = temp.split("&");
      for (String pair : pairs) {
        int idx = pair.indexOf("=");
        String key = idx > 0 ? URLDecoder.decode(pair.substring(0, idx), "UTF-8") : pair;
        if (key.equals("query")) {
          //find out the query parameter
          queryString = idx > 0 && pair.length() > idx + 1 ? URLDecoder.decode(pair.substring(idx + 1), "UTF-8") : null;
        }
      }
      if (queryString == null) {
        queryString = "";
      }
      validityStatus = QueryHandler.Validity.VALID;

    } catch (MalformedURLException e) {
      validityStatus = QueryHandler.Validity.INVALID_SYNTAX;
      queryString = "INVALID"; //needs to be something != null
      logger.info("MAL length: " + uriQuery.length());
      logger.warn("There was a syntax error in the following URL: " + uriQuery + " \tFound at " + inputFile + ", line " + line + "\t" + e.getMessage());
    } catch (UnsupportedEncodingException e) {
      logger.error("Your system apperently doesn't supports UTF-8 encoding. Please fix this before running this software again.");
    } catch (IllegalArgumentException e) {
      //increment counter for truncated queries
      validityStatus = QueryHandler.Validity.INVALID_URL_TRUNCATED;
      queryString = "INVALID"; //needs to be something != null
      logger.debug("ILL length: " + uriQuery.length());
      logger.warn("There was an error while parsing the following URL, probably caused by a truncated URI: " + uriQuery + " \tFound at " + inputFile + ", line " + line + "\t" + e.getMessage());
    }
    return new Tuple2<>(queryString, validityStatus);
  }
}
