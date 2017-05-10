package query;

import general.Main;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * @author adrian
 */
public abstract class QueryHandler
{
  /**
   * Define a static logger variable.
   */
  private static final Logger logger = Logger.getLogger(QueryHandler.class);

  /**
   * A pattern of a SPARQL query where all "parameter" information is removed from
   * the query
   * Helpful for similarity comparisons between two Queries
   * The query type as a number referencing a file containing the queryTypePattern.
   * <p>
   * -1 means uninitialized or that the query is or that the queryType could not
   * be computed
   */
  protected String queryType = "-1";

  /**
   * The Q-IDs used in this query.
   */
  protected Set<String> qIDs;
  /**
   * Kind of the complexity of the query
   */
  protected Integer querySize = null;
  /**
   * The number of variables in the query head
   */
  protected Integer variableCountHead = null;
  /**
   * The number of variables in the pattern.
   */
  protected Integer variableCountPattern = null;
  /**
   * The number of triples in the query pattern
   * (including triples in SERIVCE blocks).
   */
  protected Integer tripleCountWithService = null;
  /**
   * Contains the sparql statistics. Needs to be a linked map in order to
   * ensure consistency in the order of the keys (the OutputHandler first iterates
   * over the keys for the output TSV headers, and LATER over the values)
   */
  protected LinkedHashMap<String, Integer> sparqlStatistics;
  /**
   * Saves the query-string with added prefixes.
   */
  private String queryString;
  /**
   * Saves the query-string without prefixes.
   */
  private String queryStringWithoutPrefixes;
  /**
   * Saves if queryString is a valid query, and if not, why
   * 2 -> valid, but empty query
   * 1 -> valid
   * 0 -> default internal value -> should always be changed, if it is 0 then there was an internal error!
   * -1 -> invalid for unknown reasons
   * -3 -> Not a valid (absolute URI):
   * -5 -> BIND clause alias '{}' was previously used
   * -6 ->	Multiple prefix declarations for prefix 'p'
   * -9 -> There was a syntax error in the URL
   * -10 -> The url was truncated and was therefore not decodable
   * -11 -> The query string was empty and was therefore not being parsed
   */
  private int validityStatus;
  /**
   * Saves the current line the query was from.
   */
  private long currentLine;
  /**
   * Saves the current file the query was from.
   */
  private String currentFile;
  /**
   * Contains the length of the Query without added prefixes.
   */
  private int lengthNoAddedPrefixes;
  /**
   * The name of the tool which created the query.
   * 0 for user querys
   * -1 for unknown tool
   */
  private String toolName;
  /**
   * The version of the tool which created the query.
   * -1 for unknown tool
   */
  private String toolVersion;
  /**
   * True if the tool information got already computed.
   * useful for lazy-loading of tool information
   */
  private boolean toolComputed = false;
  /**
   * The userAgent string which executed this query.
   */
  private String userAgent;
  /**
   * The Q-IDs as a string of comma separated values.
   */
  private String qIDString = null;

  /**
   *
   */
  public QueryHandler()
  {
    this.queryString = "";
    this.validityStatus = 0;
  }

  /**
   * Update the handler to represent the string in queryString.
   */
  protected abstract void update();

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
    if (queryStringToSet.equals("")) {
      this.validityStatus = 2;
    } else if (validityStatus > -1) {
      this.queryStringWithoutPrefixes = queryStringToSet;
      this.lengthNoAddedPrefixes = queryStringToSet.length();
      this.queryString = queryStringToSet;
      update();
    }
  }

  /**
   * @return Returns the original query-string represented by this handler.
   */
  public final String getQueryStringWithoutPrefixes()
  {
    return queryStringWithoutPrefixes;
  }

  /**
   * @return Returns the validity status of the query
   */
  public final int getValidityStatus()
  {
    return validityStatus;
  }

  /**
   * @param validityStatus validityStatus to set the variable valid to
   */
  public final void setValidityStatus(int validityStatus)
  {
    this.validityStatus = validityStatus;
  }

  /**
   * @return Returns the length of the query with comments and even if invalid.
   * '/n' counts as one character.
   */
  public final Integer getStringLength()
  {
    if (queryString == null) {
      return -1;
    }
    return queryString.length();
  }

  /**
   * Computes the number of variables in the query head.
   * Useful for caching.
   */
  protected abstract void computeVariableCountHead();

  /**
   * @return Returns the number of variables in the query head.
   */
  public Integer getVariableCountHead()
  {
    if (this.variableCountHead == null) {
      this.computeVariableCountHead();
    }
    return this.variableCountHead;
  }

  /**
   * Computes the sparqlStatistics.
   * Useful for caching.
   */
  protected abstract void computeSparqlStatistics();

  /**
   * @return a map containing the number of occurrences of each sparql features
   * existing in the AST tupleExpr tree
   */
  public LinkedHashMap<String, Integer> getSparqlStatistics()
  {
    if (this.sparqlStatistics == null) {
      this.computeSparqlStatistics();
    }
    return this.sparqlStatistics;
  }


  /**
   * Computes the number of variables in the query pattern.
   * Useful for caching.
   */
  protected abstract void computeVariableCountPattern();

  /**
   * @return Returns the number of variables in the query pattern.
   */
  public Integer getVariableCountPattern()
  {
    if (this.variableCountPattern == null) {
      this.computeVariableCountPattern();
    }
    return this.variableCountPattern;
  }

  /**
   * Computes the number of triples in the query pattern
   * (including triples in SERIVCE blocks).
   * Useful for caching.
   */
  protected abstract void computeTripleCountWithService();

  /**
   * @return Returns the number of triples in the query pattern
   * (including triples in SERIVCE blocks).
   */
  public Integer getTripleCountWithService()
  {
    if (this.tripleCountWithService == null) {
      this.computeTripleCountWithService();
    }
    return this.tripleCountWithService;
  }


  /**
   * Computes the query type.
   *
   * @throws IllegalStateException
   */
  protected abstract void computeQueryType() throws IllegalStateException;


  /**
   * @return Returns the query type as a number referencing a file containing the queryTypePattern.
   */
  public final String getQueryType()
  {
    //lazy loading of queryType
    if (queryType.equals("-1")) {
      try {
        this.computeQueryType();
      } catch (IllegalStateException e) {
        return "-1";
      }
    }
    return this.queryType;
  }

  /**
   * @return the line the query originated from
   */
  public final long getCurrentLine()
  {
    return currentLine;
  }

  /**
   * @param currentLine the current line the query was from
   */
  public final void setCurrentLine(long currentLine)
  {
    this.currentLine = currentLine;
  }

  /**
   * @return the file the query originated from
   */
  public final String getCurrentFile()
  {
    return currentFile;
  }

  /**
   * @param currentFile the current file the query originated from
   */
  public final void setCurrentFile(String currentFile)
  {
    this.currentFile = currentFile;
  }

  /**
   * @return the length of the query without the added prefixes
   */
  public int getLengthNoAddedPrefixes()
  {
    return lengthNoAddedPrefixes;
  }

  /**
   * Computes kind of the query complexity.
   * Useful for caching.
   */
  protected abstract void computeQuerySize();

  /**
   * @return kind of the complexity of the SPARQL query
   */
  public Integer getQuerySize()
  {
    if (this.querySize == null) {
      this.computeQuerySize();
    }
    return this.querySize;
  }

  /**
   * Sets the toolName and version.
   */
  private void computeTool()
  {
    this.toolComputed = true;

    //default values in case we don't find anything for computation
    this.toolName = "0";
    this.toolVersion = "0";

    if (validityStatus != 1) {
      return;
    }

    if (queryStringWithoutPrefixes.equals("prefix schema: <http://schema.org/> SELECT * WHERE {<http://www.wikidata.org> schema:dateModified ?y}")) {
      toolName = "wikidataLastModified";
      toolVersion = "1.0";
      return;
    }

    //first check if there is a toolComment, if so we don't need to use
    // queryTypes and userAgents
    //assuming that if there is a tool comment at all, it is before the query,
    // but can be after the namespace the first comment
    // and that it start with #TOOL: or #Tool: or #tool: and that the tool name is then
    // everything until the end of that line
    int toolIndex = this.queryStringWithoutPrefixes.indexOf("#TOOL:");
    if (toolIndex == -1) {
      toolIndex = this.queryStringWithoutPrefixes.indexOf("#Tool:");
    }
    if (toolIndex == -1) {
      toolIndex = this.queryStringWithoutPrefixes.indexOf("#tool:");
    }
    if (toolIndex != -1) {
      int toolCommentLineEndIndex = this.queryStringWithoutPrefixes.indexOf("\n", toolIndex + 6);

      //in case the index is at the end of the query, looking at you developer of Histropedia-WQT !!!
      if (toolCommentLineEndIndex == -1) {
        toolCommentLineEndIndex = queryStringWithoutPrefixes.length();
      }
      this.toolName = this.queryStringWithoutPrefixes.substring(toolIndex + 6, toolCommentLineEndIndex);
      this.toolVersion = "0.1";
      return;
    }

    if (userAgent.equals("Mozilla/5.0")) {
      toolName = "feb20descriptions";
      toolVersion = "0.1";
      return;
    }

    Tuple2<String, String> key = new Tuple2<>(this.getQueryType(), this.getUserAgent());
    if (Main.queryTypeToToolMapping.containsKey(key)) {
      Tuple2<String, String> value = Main.queryTypeToToolMapping.get(key);
      this.toolName = value._1;
      this.toolVersion = value._2;
      return;
    }

    for (String regex : Main.userAgentRegex) {
      if (this.userAgent.matches(regex)) {
        this.toolName = "USER";
        this.toolVersion = "1.0";
      }
    }

    if (this.toolName.equals("0")) {
      logger.debug("Tool found which is neither user nor bot - is it really not a bot or a user?: \n" + this.queryString);
    }

  }

  /**
   * @return The name of the tool that posed this query (if any)
   */
  public final String getToolName()
  {
    if (!toolComputed) {
      this.computeTool();
    }
    return toolName;
  }

  /**
   * @return The version of the tool that posed this query (if any)
   */
  public final String getToolVersion()
  {
    if (!toolComputed) {
      this.computeTool();
    }
    return toolVersion;
  }

  /**
   * @return The user agent that posed this query.
   */
  private String getUserAgent()
  {
    return userAgent;
  }

  /**
   * @param userAgent The user agent that posed this query.
   */
  public final void setUserAgent(String userAgent)
  {
    this.userAgent = userAgent;
  }

  /**
   * @return The Q-IDs contained in this query
   */
  public Set<String> getqIDs()
  {
    if (queryType.equals("-1") && qIDs == null) {
      try {
        this.computeQueryType();
      } catch (IllegalStateException e) {
        return null;
      }
    }
    return qIDs;
  }

  /**
   * Sets the Q-IDs, removing http://www.wikidata.org/entity/ if necessary.
   *
   * @param qIDstoSet the Q-IDs to set
   */
  protected void setqIDs(Set<String> qIDstoSet)
  {
    qIDs = new HashSet<String>();
    for (String qID : qIDstoSet) {
      qIDs.add(qID.replaceAll("http://www.wikidata.org/entity/", ""));
    }
  }

  /**
   * Computes the Q-IDs as a string of comma separated values.
   * Useful for caching.
   */
  private void computeqIDString()
  {
    if (qIDs == null) {
      this.qIDString = "D";
      return;
    }
    if (qIDs.size() == 0) {
      this.qIDString = "D";
      return;
    }
    String qIDString = "";
    for (String qID : qIDs) {
      qIDString += qID + ",";
    }
    this.qIDString = qIDString.substring(0, qIDString.lastIndexOf(","));
  }

  /**
   * @return the Q-IDs as a string of comma separated values.
   */
  public String getqIDString()
  {
    if (this.qIDString == null) {
      this.computeqIDString();
    }
    return this.qIDString;
  }
}
