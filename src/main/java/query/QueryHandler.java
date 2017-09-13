package query;

import general.Main;
import openrdffork.TupleExprWrapper;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;

/**
 * @author adrian
 */
public abstract class QueryHandler implements Serializable
{

  public QueryHandler(Validity validity, Long line, Integer day, String queryString)
  {
    this.day = day;
    this.line = line;
    this.validityStatus = validity;

    this.setUniqeId(this.day, this.line, queryString);
    this.setOriginalId(this.getUniqeId());

    if (queryString.equals("")) {
      this.validityStatus = Validity.EMPTY;
    } else if (validityStatus == null || validityStatus.getValue() > -1) {
      this.queryStringWithoutPrefixes = queryString;
      this.lengthNoAddedPrefixes = queryString.length();
      this.queryString = queryString;
      update();
    }
  }

  /**
   * Define a static logger variable.
   */
  private static final Logger logger = Logger.getLogger(QueryHandler.class);

  /**
   * The number of the thread (needs to be unique for one run).
   */
  protected int threadNumber = -1;
  /**
   * The map holding the query types.
   */
  protected Map<TupleExprWrapper, String> queryTypes;

  /**
   * A pattern of a SPARQL query where all "parameter" information is removed from
   * the query
   * Helpful for similarity comparisons between two Queries
   * The query type as a number referencing a file containing the queryTypePattern.
   * <p>
   * -1 means uninitialized or that the query is or that the queryType could not
   * be computed
   */
  protected String queryType = null;

  /**
   * Saves if this the query represented by this handler falls into the complex or simple category.
   * A query is simple if it uses no variable and only simple prefixes in property positions.
   */
  protected Complexity simpleOrComplex = null;


  /**
   * The Q-IDs used in this query.
   */
  protected Set<String> qIDs;
  /**
   * The P-IDs used in this query.
   */
  protected Set<String> pIDs;
  /**
   * The geocoordinates used in this query.
   */
  protected Set<String> coordinates;
  /**
   * Kind of the complexity of the query.
   */
  protected Integer querySize = null;
  /**
   * The number of variables in the query head.
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
   * Contains the sparql statistics.
   */
  protected HashMap<String, Integer> sparqlStatistics;
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
   */
  private Validity validityStatus;
  /**
   * Saves the current line the query was from.
   */
  protected long line;
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
  private boolean toolComputed;
  /**
   * The userAgent string which executed this query.
   */
  private String userAgent;
  /**
   * The Q-IDs as a string of comma separated values.
   */
  private String qIDString;
  /**
   * The P-IDs as a string of comma separated values.
   */
  private String pIDString;
  /**
   * The geo coordinates as a string of comma separated values.
   */
  private String coordinatesString = null;
  /**
   * The categories as a string of comma separated values.
   */
  private String categories = null;

  /**
   * The day at which the Query was being created;
   * Used for the calculation of the uniqueId.
   */
  protected int day;

  public String getUniqeId()
  {
    return uniqeId;
  }

  public String getOriginalId()
  {
    return originalId;
  }

  public void setUniqeId(int day, long line, String queryString)
  {
    this.uniqeId = QueryHandler.generateId(day, line, queryString);
  }

  /**
   * Generates an id based on the parameters.
   *
   * @param day
   * @param line
   * @param queryString
   * @return
   */
  public static String generateId(int day, long line, String queryString)
  {
    return day + "_" + line + "_" + queryString.hashCode();
  }

  /**
   * The unique id consists of the hash of the QueryString combined with the line it was being executed.
   */
  private String uniqeId;

  protected void setOriginalId(String originalId)
  {
    this.originalId = originalId;
  }

  /**
   * The id of the first queryHandler with this queryString.
   */
  private String originalId;


  /**
   *
   */
  public QueryHandler()
  {
    this.queryString = "";
    this.validityStatus = Validity.DEFAULT;
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
   * @param threadNumberToSet {@link #threadNumber}
   */
  public void setThreadNumber(int threadNumberToSet)
  {
    threadNumber = threadNumberToSet;
  }

  /**
   * @param queryTypesToSet {@link #queryTypes}
   */
  public void setQueryTypes(Map<TupleExprWrapper, String> queryTypesToSet)
  {
    this.queryTypes = queryTypesToSet;
  }

  /**
   * @return Returns the query-string represented by this handler.
   */
  public final String getQueryString()
  {
    return queryString;
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
  public final Validity getValidityStatus()
  {
    return validityStatus;
  }

  /**
   * @param validityStatus validityStatus to set the variable valid to
   */
  public final void setValidityStatus(Validity validityStatus)
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
  public HashMap<String, Integer> getSparqlStatistics()
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
    if (queryType == null) {
      try {
        this.computeQueryType();
      } catch (IllegalStateException e) {
        return "INVALID";
      }
    }
    return this.queryType;
  }

  /**
   * @return the line the query originated from
   */
  public final long getLine()
  {
    return line;
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
    this.toolName = "UNKNOWN";
    this.toolVersion = "UNKNOWN";

    if (validityStatus != Validity.VALID) {
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
   * @return The name of the example query this query equals according to string comparison, or -1 if it does not
   */
  public final String getExampleQueryStringMatch()
  {
    if (this.getValidityStatus() != Validity.VALID) {
      return "INVALID";
    }
    String name = Main.exampleQueriesString.get(this.queryString);
    if (name == null) {
      return "NONE";
    }
    return name;
  }

  /**
   * @return The name of the example query this query equals according to comparison of the parsed query, or -1 if it does not
   */
  public abstract String getExampleQueryTupleMatch();

  /**
   * @param anyIDstoSet A set with IDs with explicit URIs
   * @return The set with all URIs from Main.prefixes replaced by the prefixes, all others will be added as-is.
   */
  private Set<String> setAnyIDs(Set<String> anyIDstoSet)
  {
    List<Map.Entry<String, String>> prefixList = new ArrayList<Map.Entry<String, String>>(Main.prefixes.entrySet());
    Collections.sort(prefixList, new Comparator<Map.Entry<String, String>>()
    {

      @Override
      public int compare(Entry<String, String> arg0, Entry<String, String> arg1)
      {
        return Integer.valueOf(arg1.getValue().length()).compareTo(Integer.valueOf(arg0.getValue().length()));
      }
    });

    Set<String> anyIDs = new HashSet<String>();
    for (String anyID : anyIDstoSet) {
      boolean wasAdded = false;
      for (Map.Entry<String, String> entry : prefixList) {
        if (anyID.startsWith(entry.getValue())) {
          anyIDs.add(anyID.replaceFirst(entry.getValue(), entry.getKey() + ":"));
          wasAdded = true;
          break;
        }
      }
      if (!wasAdded) anyIDs.add(anyID);
    }
    return anyIDs;
  }

  /**
   * @param anyIDstoString The set that should be converted into a string.
   * @return The IDs as a string of comma separated values.
   */
  private String computeAnyIDString(Set<String> anyIDstoString)
  {
    if (anyIDstoString == null) {
      return "";
    }
    if (anyIDstoString.size() == 0) {
      return "";
    }
    String anyIDStringToReturn = "";
    for (String anyID : anyIDstoString) {
      anyIDStringToReturn += anyID + ",";
    }
    return anyIDStringToReturn.substring(0, anyIDStringToReturn.lastIndexOf(","));
  }

  /**
   * @return The Q-IDs contained in this query
   */
  public Set<String> getqIDs()
  {
    if (queryType == null && qIDs == null) {
      try {
        this.computeQueryType();
      } catch (IllegalStateException e) {
        return null;
      }
    }
    return qIDs;
  }

  /**
   * Sets the Q-IDs, replacing URIs with default prefixes.
   *
   * @param qIDstoSet the Q-IDs to set
   */
  protected void setqIDs(Set<String> qIDstoSet)
  {
    this.qIDs = setAnyIDs(qIDstoSet);
  }

  /**
   * Computes the Q-IDs as a string of comma separated values.
   * Useful for caching.
   */
  private void computeqIDString()
  {
    this.qIDString = computeAnyIDString(getqIDs());
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

  /**
   * @return The P-IDs contained in this query
   */
  public Set<String> getpIDs()
  {
    if (queryType == null && pIDs == null) {
      try {
        this.computeQueryType();
      } catch (IllegalStateException e) {
        return null;
      }
    }
    return pIDs;
  }

  /**
   * Sets the P-IDs, replacing URIs with default prefixes.
   *
   * @param pIDstoSet the P-IDs to set
   */
  protected void setpIDs(Set<String> pIDstoSet)
  {
    this.pIDs = setAnyIDs(pIDstoSet);
  }

  /**
   * Computes the P-IDs as a string of comma separated values.
   * Useful for caching.
   */
  private void computepIDString()
  {
    this.pIDString = computeAnyIDString(getpIDs());
  }

  /**
   * @return the P-IDs as a string of comma separated values.
   */
  public String getpIDString()
  {
    if (this.pIDString == null) {
      this.computepIDString();
    }
    return this.pIDString;
  }

  /**
   * Sets the categories string to contain all categories it should have according to its predicates and the propertyGroupMapping.
   */
  private void computeCategoriesString()
  {
    if (queryType == null && qIDs == null) {
      try {
        this.computeQueryType();
      } catch (IllegalStateException e) {
        this.categories = "";
      }
    }

    Set<String> categoriesFound = new HashSet<String>();

    Set<String> pids = this.getpIDs();

    if (pids == null) {
      this.categories = "";
      return;
    }

    for (String predicate : pids) {
      for (Map.Entry<String, Set<String>> entry : Main.propertyGroupMapping.entrySet()) {
        if (predicate.endsWith(entry.getKey())) {
          for (Map.Entry<String, String> entryPrefixes : Main.prefixes.entrySet()) {
            if (predicate.startsWith(entryPrefixes.getKey())) {
              categoriesFound.addAll(entry.getValue());
            }
          }
        }
      }
    }
    this.categories = this.computeAnyIDString(categoriesFound);
  }

  /**
   * @return {@link #categories}
   */
  public String getCategoriesString()
  {
    if (this.categories == null) {
      this.computeCategoriesString();
    }
    return categories;
  }

  /**
   * Computes the coordinates as a string of comma separated values.
   */
  private void computeCoordinatesString()
  {
    if (coordinates == null) {
      this.computeCoordinates();
    }

    this.coordinatesString = this.computeAnyIDString(coordinates);
  }

  /**
   * Extracts the geo coordinates from the query and stores them in {@link #coordinates}.
   */
  public abstract void computeCoordinates();

  /**
   * @return {@link #coordinates}
   */
  public String getCoordinatesString()
  {
    if (this.coordinatesString == null) {
      this.computeCoordinatesString();
    }
    return coordinatesString;
  }

  /**
   * @return {@link #simpleOrComplex}
   */
  public Complexity getSimpleOrComplex()
  {
    if (this.simpleOrComplex == null) {
      this.computeSimpleOrComplex();
    }
    return this.simpleOrComplex;
  }

  /**
   * Computes if this query is simple or complex.
   */
  public abstract void computeSimpleOrComplex();

  /**
   * @author adrian
   * An enumeration specifying the complexity of the query.
   */
  public enum Complexity
  {
    /**
     * The complexity has not been set.
     */
    NONE,
    /**
     * There was an error while determining the complexity.
     */
    ERROR,
    /**
     * The query is of simple complexity.
     */
    SIMPLE,
    /**
     * The query is of complex complexity.
     */
    COMPLEX
  }

  /**
   * @author adrian
   * An enumeration specifying the different validity states.
   */
  public enum Validity
  {
    /**
     * Valid, but empty query.
     */
    EMPTY(2),
    /**
     * Valid query.
     */
    VALID(1),
    /**
     * default internal value -> should always be changed, if it is 0 then there was an internal error!
     */
    DEFAULT(0),
    /**
     * Invalid for unknown reasons.
     */
    INVALID(-1),
    /**
     * Not a valid (absolute) URI.
     */
    INVALID_URI(-2),
    /**
     * BIND clause alias '{}' was previously used.
     */
    INVALID_BIND_PREVIOUSLY_USED(-5),
    /**
     * Multiple prefix declarations for prefix.
     */
    INVALID_PREFIX_DECLARATION_MULTIPLE_TIMES(-6),
    /**
     * There was a syntax error in the URL.
     */
    INVALID_SYNTAX(-9),
    /**
     * The url was truncated and was therefore not decodable.
     */
    INVALID_URL_TRUNCATED(-10),
    /**
     * The query string was empty and was therefore not being parsed.
     */
    INVALID_EMTPY_QUERY_STRING(-11),
    /**
     * If some internal error occured.
     */
    INTERNAL_ERROR(-12);

    /**
     * The value representing the validity.
     */
    private int value;

    /**
     * @param valueToSet The integer value of the validity state.
     */
    Validity(int valueToSet)
    {
      this.value = valueToSet;
    }

    /**
     * @return the validity code of this validity object.
     */
    public int getValue()
    {
      return this.value;
    }
  }
}
