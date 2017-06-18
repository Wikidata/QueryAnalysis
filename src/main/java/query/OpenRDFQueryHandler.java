package query;

import general.Main;
import openrdffork.StandardizingSPARQLParser;
import openrdffork.TupleExprWrapper;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.ArbitraryLengthPath;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.BaseDeclProcessor;
import org.openrdf.query.parser.sparql.PrefixDeclProcessor;
import org.openrdf.query.parser.sparql.StringEscapesProcessor;
import org.openrdf.query.parser.sparql.ast.*;

import java.util.*;

/**
 * @author jgonsior
 */
public class OpenRDFQueryHandler extends QueryHandler
{
  /**
   * The base URI to resolve any possible relative URIs against.
   */
  private static final String BASE_URI = "https://query.wikidata.org/bigdata/namespace/wdq/sparql";
  /**
   * Define a static logger variable.
   */
  private static final Logger logger = Logger.getLogger(OpenRDFQueryHandler.class);
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
          logger.warn("This shoud not happen anymore: " + e.getMessage());
          setValidityStatus(-3);
        } else if (message.contains("BIND clause alias '{}' was previously used")) {
          logger.warn("This shoud not happen anymore: " + e.getMessage());
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
    //the third argument is the baseURI to resolve any relative URIs that are in
    //the query against, but it can be NULL as well
    StandardizingSPARQLParser parser = new StandardizingSPARQLParser();
/*  try {
      queryAST = SyntaxTreeBuilder.parseQuery(queryToParse);
    }
    catch (TokenMgrError | ParseException e1) {
      throw new MalformedQueryException(e1.getMessage());
    }
    parser.normalize(queryAST);*/

    try {
      ParsedQuery parsedQuery = parser.parseQuery(queryToParse, BASE_URI);
      //QueryParserUtil.parseQuery(QueryLanguage.SPARQL, queryToParse, "https://query.wikidata.org/bigdata/namespace/wdq/sparql");
      return parsedQuery;
    } catch (Throwable e) {
      // kind of a dirty hack to catch an java.lang.error which occurs when trying to parse a query which contains f.e. the following string: "jul\ius" where the \ is an invalid escape charachter
      //because this error is kind of an MalformedQueryException we will just throw it as one
      throw new MalformedQueryException(e.getMessage());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void computeQuerySize()
  {
    if (getValidityStatus() != 1) {
      this.querySize = -1;
      return;
    }

    OpenRDFQuerySizeCalculatorVisitor openRDFQueryLengthVisitor = new OpenRDFQuerySizeCalculatorVisitor();

    try {
      this.query.getTupleExpr().visit(openRDFQueryLengthVisitor);
    } catch (Exception e) {
      logger.error("An unknown error occured while calculating the query size: ", e);
    }

    this.querySize = openRDFQueryLengthVisitor.getSize();
  }

  @Override
  protected void computeSparqlStatistics()
  {

    if (getValidityStatus() != 1) {
      this.sparqlStatistics = SparqlStatisticsCollector.getDefaultMap();
      return;
    }

    SparqlStatisticsCollector sparqlStatisticsCollector = new SparqlStatisticsCollector();
    try {
      this.query.getTupleExpr().visitChildren(sparqlStatisticsCollector);
    } catch (Exception e) {
      logger.error("An unknown error occured while computing the sparql statistics: ", e);
    }

    this.sparqlStatistics = sparqlStatisticsCollector.getStatistics();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected final void computeVariableCountPattern()
  {
    if (getValidityStatus() != 1) {
      this.variableCountPattern = -1;
      return;
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

    this.variableCountPattern = variables.size();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected final void computeVariableCountHead()
  {
    if (getValidityStatus() != 1) {
      this.variableCountHead = -1;
      return;
    }

    this.variableCountHead = this.query.getTupleExpr().getBindingNames().size();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected final void computeTripleCountWithService()
  {
    if (getValidityStatus() != 1) {
      this.tripleCountWithService = -1;
      return;
    }
    TupleExpr expr = this.query.getTupleExpr();
    StatementPatternCollector collector = new StatementPatternCollector();
    expr.visit(collector);
    this.tripleCountWithService = collector.getStatementPatterns().size();
  }

  /**
   * {@inheritDoc}
   */
  public final void computeQueryType() throws IllegalStateException
  {
    if (this.getValidityStatus() != 1) {
      throw new IllegalStateException();
    }
    ParsedQuery normalizedQuery;
    try {
      normalizedQuery = normalize(query);
    } catch (MalformedQueryException | VisitorException e) {
      logger.error("Unexpected error while normalizing " + getQueryString(), e);
      throw new IllegalStateException();
    }

    String result = queryTypes.get(new TupleExprWrapper(normalizedQuery.getTupleExpr()));

    if (result != null) {
      this.queryType = result;
      return;
    }

    if (Main.dynamicQueryTypes) {
      String newQueryType = String.valueOf(threadNumber) + "_" + String.valueOf(queryTypes.size());
      queryTypes.put(new TupleExprWrapper(normalizedQuery.getTupleExpr()), newQueryType);
      this.queryType = newQueryType;
    } else {
      this.queryType = "-1";
    }
  }

  /**
   * @return the represented query normalized or null if the represented query was not valid
   */
  public final ParsedQuery getNormalizedQuery()
  {
    if (this.getValidityStatus() != 1) {
      throw new IllegalStateException();
    }
    try {
      return normalize(this.query);
    } catch (MalformedQueryException | VisitorException e) {
      return null;
    }
  }

  /**
   * @return the represented query as a ParsedQuery-Object or null if the query was not valid
   */
  public final ParsedQuery getParsedQuery()
  {
    if (this.getValidityStatus() != 1) {
      return null;
    } else {
      return this.query;
    }
  }

  /**
   * Normalizes a given query by:
   * - replacing all wikidata uris at subject and object positions with sub1, sub2 ... (obj1, obj2 ...).
   *
   * @param queryToNormalize the query to be normalized
   * @return the normalized query
   * @throws MalformedQueryException If the query was malformed (would be a bug since the input was a parsed query)
   * @throws VisitorException        If there is an error during normalization
   */
  private ParsedQuery normalize(ParsedQuery queryToNormalize) throws MalformedQueryException, VisitorException
  {
    ParsedQuery normalizedQuery = new StandardizingSPARQLParser().parseNormalizeQuery(queryToNormalize.getSourceString(), BASE_URI);

    final Map<String, Integer> strings = new HashMap<>();
    final Map<String, Integer> pIDs = new HashMap<String, Integer>();

    normalizedQuery.getTupleExpr().visit(new QueryModelVisitorBase<VisitorException>()
    {

      @Override
      public void meet(StatementPattern statementPattern)
      {
        statementPattern.setSubjectVar(normalizeHelper(statementPattern.getSubjectVar(), strings));
        statementPattern.setObjectVar(normalizeHelper(statementPattern.getObjectVar(), strings));

        normalizeHelper(statementPattern.getPredicateVar(), pIDs);
      }
    });
    normalizedQuery.getTupleExpr().visit(new QueryModelVisitorBase<VisitorException>()
    {

      @Override
      public void meet(ArbitraryLengthPath arbitraryLengthPath)
      {
        arbitraryLengthPath.setSubjectVar(normalizeHelper(arbitraryLengthPath.getSubjectVar(), strings));
        arbitraryLengthPath.setObjectVar(normalizeHelper(arbitraryLengthPath.getObjectVar(), strings));
      }
    });
    this.setqIDs(strings.keySet());
    this.setpIDs(pIDs.keySet());
    return normalizedQuery;
  }

  /**
   * A helper function to find the fitting replacement value for wikidata uri normalization.
   *
   * @param var        The variable to be normalized
   * @param foundNames The list of already found names
   * @return the normalized name (if applicable)
   */
  private Var normalizeHelper(Var var, Map<String, Integer> foundNames)
  {
    if (var != null) {
      Value value = var.getValue();
      if (value != null) {
        if (value.getClass().equals(URIImpl.class)) {
          String subjectString = value.stringValue();
          if (!foundNames.containsKey(subjectString)) {
            foundNames.put(subjectString, foundNames.size() + 1);
          }
          String uri = subjectString.substring(0, subjectString.lastIndexOf("/")) + "/QName" + foundNames.get(subjectString);
          String name = "-const-" + uri + "-uri";
          return new Var(name, new URIImpl(uri));
        }
      }
    }
    return var;
  }

  @Override
  public final String getExampleQueryTupleMatch()
  {
    if (this.getValidityStatus() != 1) {
      return "-1";
    }
    String name = Main.exampleQueriesTupleExpr.get(new TupleExprWrapper(this.query.getTupleExpr()));
    if (name == null) {
      return "-1";
    }
    return name;
  }

}
