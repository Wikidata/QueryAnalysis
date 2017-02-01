package query;

import org.apache.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author adrian
 */
public abstract class QueryHandler
{
  /**
   * Define a static logger variable.
   */
  protected static Logger logger = Logger.getLogger(QueryHandler.class);

  /**
   * A pattern of a SPARQL query where all "parameter" information is removed from
   * the query
   * Helpful for similarity comparisons between two Queries
   * The query type as a number referencing a file containing the queryTypePattern.
   * <p>
   * -1 means uninitialized or that the query is or that the queryType could not
   * be computed
   */

  protected Integer queryType = -1;

  /**
   * Saves the query-string with added prefixes
   */
  private String queryString;

  /**
   * Saves the query-string without prefixes
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
   * saves the current file the query was from
   */
  private String currentFile;

  /**
   * contains the length of the Query without added prefixes
   */
  private int lengthNoAddedPrefixes;

  /**
   * the name of the tool which created the query
   * 0 for user querys
   * -1 for unknown tool
   */
  private String toolName = "0";

  /**
   * the version of the tool which created the query
   * 0 for unknown
   * -1 for unknown tool
   */
  private String toolVersion = "0";

  /**
   * the tool name from the query comment
   * 0 if undefined
   * -1 for unknown tool
   */
  private String toolCommentInfo = "0";

  /**
   * true if the tool information got already computed
   * useful for lazy-loading of tool information
   */
  private boolean toolComputed = false;

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
    if (queryStringToSet.equals("")) {
      this.validityStatus = 2;
    } else if (validityStatus > -1) {
      this.queryStringWithoutPrefixes = queryStringToSet;
      this.queryString = this.addMissingPrefixesToQuery(queryStringToSet);
      update();
    }
  }

  /**
   * https://query.wikidata.org/ automatically adds some prefixes to all queried queries
   * therefore the queries in the log files are missing mostly these prefixes and therefore
   * we need to add them manually (but only if they aren't already inside of the queries)
   * -> this method is here to achieve exactly this.
   *
   * @param queryWithoutPrefixes
   * @return
   */
  public final String addMissingPrefixesToQuery(String queryWithoutPrefixes)
  {
    this.lengthNoAddedPrefixes = queryWithoutPrefixes.length();
    String toBeAddedPrefixes = "";
    Map<String, String> prefixes = new LinkedHashMap<>();

    prefixes.put("PREFIX hint: <http://www.bigdata.com/queryHints#>", "prefix\\s+hint:");
    prefixes.put("PREFIX gas: <http://www.bigdata.com/rdf/gas#>", "prefix\\s+gas:");
    prefixes.put("PREFIX bds: <http://www.bigdata.com/rdf/search#>", "prefix\\s+bds:");
    prefixes.put("PREFIX bd: <http://www.bigdata.com/rdf#>", "prefix\\s+bd:");
    prefixes.put("PREFIX schema: <http://schema.org/>", "prefix\\s+schema:");
    prefixes.put("PREFIX cc: <http://creativecommons.org/ns#>", "prefix\\s+cc:");
    prefixes.put("PREFIX geo: <http://www.opengis.net/ont/geosparql#>", "prefix\\s+geo:");
    prefixes.put("PREFIX prov: <http://www.w3.org/ns/prov#>", "prefix\\s+prov:");
    prefixes.put("PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>", "prefix\\s+xsd:");
    prefixes.put("PREFIX skos: <http://www.w3.org/2004/02/skos/core#>", "prefix\\s+skos:");
    prefixes.put("PREFIX owl: <http://www.w3.org/2002/07/owl#>", "prefix\\s+owl:");
    prefixes.put("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>", "prefix\\s+rdf:");
    prefixes.put("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>", "prefix\\s+rdfs:");
    prefixes.put("PREFIX wdata: <http://www.wikidata.org/wiki/Special:EntityData/>", "prefix\\s+wdata:");
    prefixes.put("PREFIX wdno: <http://www.wikidata.org/prop/novalue/>", "prefix\\s+wdno:");
    prefixes.put("PREFIX prn: <http://www.wikidata.org/prop/reference/value-normalized/>", "prefix\\s+prn:");
    prefixes.put("PREFIX prv: <http://www.wikidata.org/prop/reference/value/>", "prefix\\s+prv:");
    prefixes.put("PREFIX pr: <http://www.wikidata.org/prop/reference/>", "prefix\\s+pr:");
    prefixes.put("PREFIX pqn: <http://www.wikidata.org/prop/qualifier/value-normalized/>", "prefix\\s+pqn:");
    prefixes.put("PREFIX pqv: <http://www.wikidata.org/prop/qualifier/value/>", "prefix\\s+pqv:");
    prefixes.put("PREFIX pq: <http://www.wikidata.org/prop/qualifier/>", "prefix\\s+pq:");
    prefixes.put("PREFIX psn: <http://www.wikidata.org/prop/statement/value-normalized/>", "prefix\\s+psn:");
    prefixes.put("PREFIX psv: <http://www.wikidata.org/prop/statement/value/>", "prefix\\s+psv:");
    prefixes.put("PREFIX ps: <http://www.wikidata.org/prop/statement/>", "prefix\\s+ps:");
    prefixes.put("PREFIX wdv: <http://www.wikidata.org/value/>", "prefix\\s+wdv:");
    prefixes.put("PREFIX wdref: <http://www.wikidata.org/reference/>", "prefix\\s+wdref:");
    prefixes.put("PREFIX p: <http://www.wikidata.org/prop/>", "prefix\\s+p:");
    prefixes.put("PREFIX wds: <http://www.wikidata.org/entity/statement/>", "prefix\\s+wds:");
    prefixes.put("PREFIX wdt: <http://www.wikidata.org/prop/direct/>", "prefix\\s+wdt:");
    prefixes.put("PREFIX wd: <http://www.wikidata.org/entity/>", "prefix\\s+wd:");
    prefixes.put("PREFIX wikibase: <http://wikiba.se/ontology#>", "prefix\\s+wikibase:");

    String queryWithoutPrefixesLowerCase = queryWithoutPrefixes.toLowerCase();

    for (Map.Entry<String, String> entry : prefixes.entrySet()) {
      //prevents prefixes from being added twice
      if (!Pattern.compile(entry.getValue()).matcher(queryWithoutPrefixesLowerCase).find()) {
        toBeAddedPrefixes += entry.getKey() + "\n";
      }
    }

    return toBeAddedPrefixes + queryWithoutPrefixes;
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
   * Computes the query type
   *
   * @throws IllegalStateException
   */
  public abstract void computeQueryType() throws IllegalStateException;


  /**
   * @return Returns the query type as a number referencing a file containing the queryTypePattern.
   */
  public Integer getQueryType()
  {
    //lazy loading of queryType
    if (queryType == -1) {
      try {
        this.computeQueryType();
      } catch (IllegalStateException e) {
        return -1;
      }
    }
    return this.queryType;
  }

  /**
   * @return the line the query originated from
   */
  public long getCurrentLine()
  {
    return currentLine;
  }

  /**
   * @param currentLine the current line the query was from
   */
  public void setCurrentLine(long currentLine)
  {
    this.currentLine = currentLine;
  }

  /**
   * @return the file the query originated from
   */
  public String getCurrentFile()
  {
    return currentFile;
  }

  /**
   * @param currentFile the current file the query originated from
   */
  public void setCurrentFile(String currentFile)
  {
    this.currentFile = currentFile;
  }

  public int getLengthNoAddedPrefixes()
  {
    return lengthNoAddedPrefixes;
  }

  /**
   * assuming that, if there is a tool comment at all, it is the first comment
   * in the query and that it start with a #TOOL: and that the tool name is then
   * everything until the end of that line
   */
  private void computeTool() {
    if(validityStatus != 1) {
      return;
    }
    if (this.queryStringWithoutPrefixes.startsWith("#TOOL:")) {
      this.toolCommentInfo = this.queryStringWithoutPrefixes.substring(6, this.queryStringWithoutPrefixes.indexOf("\n"));
      this.toolName = this.toolCommentInfo;
    }

    this.toolComputed = true;
  }

  public String getToolName()
  {
    if(!toolComputed) {
      this.computeTool();
    }
    return toolName;
  }

  public String getToolVersion()
  {
    if(!toolComputed) {
      this.computeTool();
    }
    return toolVersion;
  }

  public String getToolCommentInfo()
  {
    if(!toolComputed) {
      this.computeTool();
    }
    return toolCommentInfo;
  }
}
