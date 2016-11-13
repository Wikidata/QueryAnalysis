package query;

import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;

/**
 * @author adrian
 */
public abstract class QueryHandler
{
  /**
   * Define a static logger variable.
   */
  private static Logger logger = Logger.getLogger(JenaQueryHandler.class);
  /**
   * Saves the query-string handed to the constructor.
   */
  private String queryString;
  /**
   * Saves if queryString is a valid query.
   */
  private boolean valid;

  /**
   * saves the current line the query was from
   */
  private long currentLine;

  /**
   * saves the current file the query was from
   */
  private String currentFile;


  /**
   *
   */
  public QueryHandler()
  {
    this.queryString = "";
    this.valid = false;
  }

  /**
   * Updated the handler to represent the string in queryString.
   */
  public abstract void update();

  /**
   * @return The logger to write messages to.
   */
  public final Logger getLogger()
  {
    return logger;
  }

  /**
   * @return Returns the query-string represented by this handler.
   */
  public final String getQueryString()
  {
    return queryString;
  }

  /**
   * @param queryStringToSet query to set the variable queryString to
   */
  public final void setQueryString(String queryStringToSet)
  {
    if (queryString == null) {
      this.queryString = "";
      this.valid = false;
    } else {
      this.queryString = this.addMissingPrefixesToQuery(queryStringToSet);
      update();
    }
    return;
  }

  /**
   * https://query.wikidata.org/ automatically adds some prefixes to all queried queries
   * therefore the queries in the log files are missing mostly these prefixes and therefore
   * we need to add them manually (but only if they aren't already inside of the queries)
   * -> this method is here to achieve exactly this
   *
   * @param queryWithoutPrefixes
   * @return
   */
  public final String addMissingPrefixesToQuery(String queryWithoutPrefixes)
  {
    String toBeAddedPrefixes = "";
    List<String> prefixes = new LinkedList<>();
    prefixes.add("PREFIX wd: <http://www.wikidata.org/entity/>");
    prefixes.add("PREFIX wdt: <http://www.wikidata.org/prop/direct/>");
    prefixes.add("PREFIX wikibase: <http://wikiba.se/ontology#>");
    prefixes.add("PREFIX p: <http://www.wikidata.org/prop/>");
    prefixes.add("PREFIX ps: <http://www.wikidata.org/prop/statement/>");
    prefixes.add("PREFIX pq: <http://www.wikidata.org/prop/qualifier/>");
    prefixes.add("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>");
    prefixes.add("PREFIX bd: <http://www.bigdata.com/rdf#>");

    for (String prefix : prefixes) {
      if (!queryWithoutPrefixes.toLowerCase().contains(prefix.toLowerCase())) {
        toBeAddedPrefixes += prefix + "\n";
      }
    }

    return toBeAddedPrefixes + queryWithoutPrefixes;
  }

  /**
   * @return Returns true if the query is valid.
   */
  public final boolean isValid()
  {
    return valid;
  }

  /**
   * @param validToSet validity to set the variable valid to
   */
  public final void setValid(boolean validToSet)
  {
    this.valid = validToSet;
  }

  /**
   * @return Returns the length of the query with comments and even if invalid.
   * '/n' counts as one character.
   */
  public final Integer getStringLength()
  {
    if (queryString == null) return -1;
    return queryString.length();
  }

  /**
   * The function returns the length of the query as a string
   * without comments and formatting.
   *
   * @return Returns the length of the query without comments (-1 if invalid).
   */
  public abstract Integer getStringLengthNoComments();

  /**
   * @return Returns the number of variables in the query head.
   */
  public abstract Integer getVariableCountHead();

  /**
   * @return Returns the number of variables in the query pattern.
   */
  public abstract Integer getVariableCountPattern();

  /**
   * @return Returns the number of triples in the query pattern
   * (including triples in SERIVCE blocks).
   */
  public abstract Integer getTripleCountWithService();

  /**
   * @param currentLine the current line the query was from
   */
  public void setCurrentLine(long currentLine)
  {
    this.currentLine = currentLine;
  }

  /**
   * @param currentFile the current file the query originated from
   */
  public void setCurrentFile(String currentFile)
  {
    this.currentFile = currentFile;
  }

  /**
   * @return the line the query originated from
   */
  public long getCurrentLine()
  {
    return currentLine;
  }

  /**
   * @return the file the query originated from
   */
  public String getCurrentFile()
  {
    return currentFile;
  }
}
