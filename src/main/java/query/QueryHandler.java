package query;

import org.apache.log4j.Logger;

/**
 * @author adrian
 *
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
      this.queryString = queryStringToSet;
      update();
    }
    return;
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
}
