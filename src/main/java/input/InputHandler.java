package input;

import org.apache.log4j.Logger;
import output.OutputHandler;

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
  private static Logger logger = Logger.getLogger(InputHandler.class);

  /**
   * Read the file given by reader and hands the data to the outputHandler.
   *
   * @param outputHandler Handles the data that should be written.
   */
  public abstract void parseTo(OutputHandler outputHandler);

  /**
   * @param uriQuery  The uri_query entry from the logs.
   * @param inputFile The file this uri_query was read from.
   * @return The query as a pure string if possible, else an empty string.
   */
  public final String decode(String uriQuery, String inputFile, long line)
  {
    String queryString = "";
    try {
      // the url needs to be transformed first into a URL and then later into a URI because the charachter ^
      // which is included in some Queries is apparently an illegal charachter which needs to be encoded
      // differently (which the creation of a URL object first is dealing with)
      URL url = new URL("https://query.wikidata.org/" + uriQuery);

      //parse url
      String temp = url.getQuery();
      if (temp == null) return queryString;
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
      logger.info("MAL length: " + uriQuery.length());
      logger.warn("There was a syntax error in the following URL: " + uriQuery + " \tFound at " + inputFile + ", line " + line + "\t" + e.getMessage());
    } catch (UnsupportedEncodingException e) {
      logger.error("Your system apperently doesn't supports UTF-8 encoding. Please fix this before running this software again.");
    } catch (IllegalArgumentException e) {
      //increment counter for truncated queries
      logger.info("ILL length: " + uriQuery.length());
      logger.warn("There was an error while parsing the following URL, probably caused by a truncated URI: " + uriQuery + " \tFound at " + inputFile + ", line " + line +"\t" + e.getMessage());
    }
    return queryString;
  }
}
