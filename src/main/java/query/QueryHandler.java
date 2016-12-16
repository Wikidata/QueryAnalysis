package query;

import org.apache.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
   * contains the length of the Query without added prefixes
   */
  private int lengthNoAddedPrefixes;

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
    if (queryStringToSet == null) {
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
    this.lengthNoAddedPrefixes = queryWithoutPrefixes.length();
    String toBeAddedPrefixes = "";
    Map<String, String> prefixes = new LinkedHashMap<>();

    prefixes.put("PREFIX hint: <http://www.bigdata.com/queryHints#>", "<http://www.bigdata.com/queryHints#>");
    prefixes.put("PREFIX gas: <http://www.bigdata.com/rdf/gas#>", "<http://www.bigdata.com/rdf/gas#>");
    prefixes.put("PREFIX bds: <http://www.bigdata.com/rdf/search#>", "<http://www.bigdata.com/rdf/search#>");
    prefixes.put("PREFIX bd: <http://www.bigdata.com/rdf#>", "<http://www.bigdata.com/rdf#>");
    prefixes.put("PREFIX schema: <http://schema.org/>", "<http://schema.org/>");
    prefixes.put("PREFIX cc: <http://creativecommons.org/ns#>", "<http://creativecommons.org/ns#>");
    prefixes.put("PREFIX geo: <http://www.opengis.net/ont/geosparql#>", "<http://www.opengis.net/ont/geosparql#>");
    prefixes.put("PREFIX prov: <http://www.w3.org/ns/prov#>", "<http://www.w3.org/ns/prov#>");
    prefixes.put("PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>", "<http://www.w3.org/2001/XMLSchema#>");
    prefixes.put("PREFIX skos: <http://www.w3.org/2004/02/skos/core#>", "<http://www.w3.org/2004/02/skos/core#>");
    prefixes.put("PREFIX owl: <http://www.w3.org/2002/07/owl#>", "<http://www.w3.org/2002/07/owl#>");
    prefixes.put("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#>");
    prefixes.put("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>", "<http://www.w3.org/2000/01/rdf-schema#>");
    prefixes.put("PREFIX wdata: <http://www.wikidata.org/wiki/Special:EntityData/>", "<http://www.wikidata.org/wiki/Special:EntityData/>");
    prefixes.put("PREFIX wdno: <http://www.wikidata.org/prop/novalue/>", "<http://www.wikidata.org/prop/novalue/>");
    prefixes.put("PREFIX prn: <http://www.wikidata.org/prop/reference/value-normalized/>", "<http://www.wikidata.org/prop/reference/value-normalized/>");
    prefixes.put("PREFIX prv: <http://www.wikidata.org/prop/reference/value/>", "<http://www.wikidata.org/prop/reference/value/>");
    prefixes.put("PREFIX pr: <http://www.wikidata.org/prop/reference/>", "<http://www.wikidata.org/prop/reference/>");
    prefixes.put("PREFIX pqn: <http://www.wikidata.org/prop/qualifier/value-normalized/>", "<http://www.wikidata.org/prop/qualifier/value-normalized/>");
    prefixes.put("PREFIX pqv: <http://www.wikidata.org/prop/qualifier/value/>", "<http://www.wikidata.org/prop/qualifier/value/>");
    prefixes.put("PREFIX pq: <http://www.wikidata.org/prop/qualifier/>", "<http://www.wikidata.org/prop/qualifier/>");
    prefixes.put("PREFIX psn: <http://www.wikidata.org/prop/statement/value-normalized/>", "<http://www.wikidata.org/prop/statement/value-normalized/>");
    prefixes.put("PREFIX psv: <http://www.wikidata.org/prop/statement/value/>", "<http://www.wikidata.org/prop/statement/value/>");
    prefixes.put("PREFIX ps: <http://www.wikidata.org/prop/statement/>", "<http://www.wikidata.org/prop/statement/>");
    prefixes.put("PREFIX wdv: <http://www.wikidata.org/value/>", "<http://www.wikidata.org/value/>");
    prefixes.put("PREFIX wdref: <http://www.wikidata.org/reference/>", "<http://www.wikidata.org/reference/>");
    prefixes.put("PREFIX p: <http://www.wikidata.org/prop/>", "<http://www.wikidata.org/prop/>");
    prefixes.put("PREFIX wds: <http://www.wikidata.org/entity/statement/>", "<http://www.wikidata.org/entity/statement/>");
    prefixes.put("PREFIX wdt: <http://www.wikidata.org/prop/direct/>", "<http://www.wikidata.org/prop/direct/>");
    prefixes.put("PREFIX wd: <http://www.wikidata.org/entity/>", "<http://www.wikidata.org/entity/>");
    prefixes.put("PREFIX wikibase: <http://wikiba.se/ontology#>", "<http://wikiba.se/ontology#>");

    for (Map.Entry<String, String> entry : prefixes.entrySet()) {
      //prevents prefixes from being added twice
      if (!queryWithoutPrefixes.contains(entry.getValue())) {
        toBeAddedPrefixes += entry.getKey() + "\n";
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
}
