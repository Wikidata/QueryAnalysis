package query;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParserUtil;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author jgonsior
 */
public class BlazedQueryHandler
{
  /**
   * Define a static logger variable.
   */
  private static Logger logger = Logger.getLogger(BlazedQueryHandler.class);

  /**
   * Saves if queryString is a valid query.
   */
  private boolean valid;
  /**
   * The query object created from query-string.
   */
  private ParsedQuery query;
  /**
   * Saves the number of triples in the pattern.
   *
   * @todo Should be local variable of getTripleCount() but cannot because of:
   * Local variable triplesCount defined in an enclosing scope must be final or
   * effectively final.
   */
  private int triplesCount;

  /**
   * @param queryToAnalyze String which (if possible) will be parsed
   *                       for further analysis
   */
  public BlazedQueryHandler(String queryToAnalyze)
  {
    try {
      this.query = this.parseQuery(queryToAnalyze);
      this.valid = true;
    } catch (MalformedQueryException e) {
      this.valid = false;
    }
  }

  /**
   * parses a given SPARQL 1.1 query into an OpenRDF ParsedQuery object
   *
   * @param queryToParse
   * @return
   * @throws MalformedQueryException
   */
  private ParsedQuery parseQuery(String queryToParse) throws MalformedQueryException
  {
    //the third argument is the basURI to resolve any relative URIs that are in
    //the query against, but it can be NULL as well
    return QueryParserUtil.parseQuery(QueryLanguage.SPARQL, queryToParse, null);
  }

  /**
   * @return Returns the query-string represented by this handler.
   */
  public final String getQueryString()
  {
    return this.query.getSourceString();
  }

  /**
   * @return Returns true if the query is valid.
   */
  public final boolean isValid()
  {
    return valid;
  }

  /**
   * @return Returns the length of the query with comments and even if invalid.
   * '/n' counts as one character.
   */
  public final Integer getStringLength()
  {
    if (this.getQueryString() == null) return -1;
    return this.getQueryString().length();
  }

  /**
   * The function returns the length of the query as a string
   * without comments and formatting.
   * <p>
   * Unfortunately I couldn't find any OpenRDF method for removing the comments
   * --> it needs to be done in a cumbersome manual way…
   *
   * @return Returns the length of the query without comments (-1 if invalid).
   * and make sure it cannot break the query.
   */
  public final Integer getStringLengthNoComments()
  {
    if (!valid) {
      return -1;
    }
    String sourceQuery = query.getSourceString();
    String uncommented = "";

    //if there is a < or a " then there can't be a comment anymore until we reach a > or another "
    boolean canFindComments = true;
    boolean commentFound = false;

    //ignore all # that are inside <> or ""
    for (int i = 0; i < sourceQuery.length(); i++) {
      Character character = sourceQuery.charAt(i);

      if (character == '#' && canFindComments) {
        commentFound = true;
      } else if (character == '\n') {
        // in a new line everything is possible again
        commentFound = false;
        canFindComments = true;
      } else if (canFindComments && (character == '<' || character == '"')) {
        canFindComments = false;
      } else if (character == '>' || character == '"') {
        // now we can find comments again
        canFindComments = true;
      }

      //finally keep only charachters that are NOT inside a comment
      if (!commentFound) {
        uncommented = uncommented + character;
      }
    }

    try {
      this.parseQuery(uncommented);
    } catch (MalformedQueryException e) {
      logger.warn("Tried to remove formatting from a valid string " + "but broke it while doing so.\n" + e.getLocalizedMessage() + "\n\n" + e.getMessage());
      return -1;
    }
    return uncommented.length();
  }


  /**
   * @return Returns the number of variables in the query pattern.
   */
  public final Integer getVariableCountPattern()
  {
    if (!this.valid) {
      return -1;
    }

    final Set<Var> variables = new HashSet<>();

    TupleExpr expr = this.query.getTupleExpr();
    StatementPatternCollector collector = new StatementPatternCollector();
    expr.visit(collector);
    List<StatementPattern> statementPatterns = collector.getStatementPatterns();

    for (StatementPattern statementPattern : statementPatterns) {
      List<Var> statementVariables = statementPattern.getVarList();
      for (Var statementVariable : statementVariables) {
        if (!statementVariable.isConstant()) {
          variables.add(statementVariable);
        }
      }

    }

    return variables.size();
  }

  /**
   * @return Returns the number of variables in the query head.
   */
  public final Integer getVariableCountHead()
  {
    if (!this.valid) {
      return -1;
    }

    return this.query.getTupleExpr().getBindingNames().size();
  }

  /**
   * @return Returns the number of triples in the query pattern
   * (including triples in SERIVCE blocks).
   */
  public final Integer getTripleCountWithService()
  {
    if (!this.valid) {
      return -1;
    }
    TupleExpr expr = this.query.getTupleExpr();
    StatementPatternCollector collector = new StatementPatternCollector();
    expr.visit(collector);
    return collector.getStatementPatterns().size();
  }
}
