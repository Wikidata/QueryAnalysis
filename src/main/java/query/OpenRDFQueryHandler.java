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
public class OpenRDFQueryHandler extends QueryHandler
{
  /**
   * Define a static logger variable.
   */
  protected static Logger logger = Logger.getLogger(OpenRDFQueryHandler.class);

  /**
   * The query object created from query-string.
   */
  private ParsedQuery query;

  /**
   * {@inheritDoc}
   */
  public final void update()
  {
    try {
      this.query = this.parseQuery(getQueryString());
      this.setValidityStatus(1);
    } catch (MalformedQueryException e) {
      String message = e.getMessage();
      if (message != null) {
        if (message.contains("\n")) {
          message = message.substring(0, message.indexOf("\n"));
        }

        if (message.contains("Not a valid (absolute) URI:")) {
          setValidityStatus(-3);
        } else if (message.contains("BIND clause alias '{}' was previously used")) {
          setValidityStatus(-5);
        } else if (message.contains("Multiple prefix declarations for prefix")) {
          setValidityStatus(-6);
        } else {
          setValidityStatus(-1);
        }
      } else {
        setValidityStatus(-1);
      }
      logger.debug("Invalid query: \t" + getQueryString() + "\t->\t" + e.getMessage());
      logger.debug("QUE length:" + this.getLengthNoAddedPrefixes() + "\t" + message);
    }
  }

  /**
   * Parses a given SPARQL 1.1 query into an OpenRDF ParsedQuery object.
   *
   * @param queryToParse SPARQL-query as string that should be parsed to OpenRDF
   * @return Returns the query as an OpenRDF ParsedQuery object
   * @throws MalformedQueryException if the supplied query was malformed
   */
  private ParsedQuery parseQuery(String queryToParse) throws MalformedQueryException
  {
    //the third argument is the basURI to resolve any relative URIs that are in
    //the query against, but it can be NULL as well
    try {
      return QueryParserUtil.parseQuery(QueryLanguage.SPARQL, queryToParse, "https://query.wikidata.org/bigdata/namespace/wdq/sparql");
    } catch (Throwable e) { // kind of a dirty hack to catch an java.lang.error which occurs when trying to parse a query which contains f.e. the following string: "jul\ius" where the \ is an invalid escape charachter
      //because this error is kind of an MalformedQueryException we will just throw it as one
      throw new MalformedQueryException(e.getMessage());
    }
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
    if (getValidityStatus() != 1) {
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

      //finally keep only characters that are NOT inside a comment
      if (!commentFound) {
        uncommented = uncommented + character;
      }
    }

    try {
      this.parseQuery(uncommented);
    } catch (MalformedQueryException e) {
      getLogger().warn("Tried to remove formatting from a valid string " + "but broke it while doing so.\n" + e.getLocalizedMessage() + "\n\n" + e.getMessage());
      return -1;
    }
    return uncommented.length();
  }

  @Override
  public Integer getQuerySize()
  {
    if (getValidityStatus() != 1) {
      return -1;
    }

    OpenRDFQuerySizeCalculatorVisitor openRDFQueryLengthVisitor = new OpenRDFQuerySizeCalculatorVisitor();

    logger.info("ast tree: \n" + this.query.getTupleExpr());

    try {
      this.query.getTupleExpr().visit(openRDFQueryLengthVisitor);
    } catch (Exception e) {
      logger.error("An unknown error occured while calculating the query size: ", e);
    }

    return openRDFQueryLengthVisitor.getSize();
  }

  /**
   * @return Returns the number of variables in the query pattern.
   */
  public final Integer getVariableCountPattern()
  {
    if (getValidityStatus() != 1) {
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
    if (getValidityStatus() != 1) {
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
    if (getValidityStatus() != 1) {
      return -1;
    }
    TupleExpr expr = this.query.getTupleExpr();
    StatementPatternCollector collector = new StatementPatternCollector();
    expr.visit(collector);
    return collector.getStatementPatterns().size();
  }
}
