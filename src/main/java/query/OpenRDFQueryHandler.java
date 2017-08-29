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
import org.openrdf.query.parser.sparql.ASTVisitorBase;
import org.openrdf.query.parser.sparql.ast.ASTLimit;
import org.openrdf.query.parser.sparql.ast.ASTNumericLiteral;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ASTRDFLiteral;
import org.openrdf.query.parser.sparql.ast.ASTString;
import org.openrdf.query.parser.sparql.ast.ASTVar;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.openrdf.query.parser.sparql.ast.TokenMgrError;
import org.openrdf.query.parser.sparql.ast.VisitorException;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
      this.setValidityStatus(QueryHandler.Validity.VALID);
    } catch (MalformedQueryException e) {
      String message = e.getMessage();
      if (message != null) {
        if (message.contains("\n")) {
          message = message.substring(0, message.indexOf("\n"));
        }

        if (message.contains("Not a valid (absolute) URI:")) {
          logger.warn("This shoud not happen anymore: " + e.getMessage());
          setValidityStatus(QueryHandler.Validity.INVALID_URI);
        } else if (message.contains("BIND clause alias '{}' was previously used")) {
          logger.warn("This shoud not happen anymore: " + e.getMessage());
          setValidityStatus(QueryHandler.Validity.INVALID_BIND_PREVIOUSLY_USED);
        } else if (message.contains("Multiple prefix declarations for prefix")) {
          setValidityStatus(QueryHandler.Validity.INVALID_PREFIX_DECLARATION_MULTIPLE_TIMES);
        } else {
          setValidityStatus(QueryHandler.Validity.INVALID);
        }
      } else {
        setValidityStatus(QueryHandler.Validity.INVALID);
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

    try {
      ParsedQuery parsedQuery = parser.parseQuery(queryToParse, BASE_URI);
      return parsedQuery;
    } catch (Throwable e) {
      // kind of a dirty hack to catch an java.lang.error which occurs when trying to parse a query which contains f.e. the following string: "jul\ius" where the \ is an invalid escape character
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
    if (getValidityStatus() != QueryHandler.Validity.VALID) {
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

    if (getValidityStatus() != QueryHandler.Validity.VALID) {
      this.sparqlStatistics = SparqlStatisticsCollector.getDefaultMap();
      return;
    }

    SparqlStatisticsCollector sparqlStatisticsCollector = new SparqlStatisticsCollector();
    try {
      System.out.println("\n\n\n\n");
      System.out.println(this.query.getSourceString());
      System.out.println(this.query.getTupleExpr());
      this.query.getTupleExpr().visitChildren(sparqlStatisticsCollector);
      this.query.getTupleExpr().visit(sparqlStatisticsCollector);
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
    if (getValidityStatus() != QueryHandler.Validity.VALID) {
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
    if (getValidityStatus() != QueryHandler.Validity.VALID) {
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
    if (getValidityStatus() != QueryHandler.Validity.VALID) {
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
    if (this.getValidityStatus() != QueryHandler.Validity.VALID) {
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
      String newQueryType = "qt:" + String.valueOf(threadNumber) + "_" + String.valueOf(queryTypes.size());
      queryTypes.put(new TupleExprWrapper(normalizedQuery.getTupleExpr()), newQueryType);
      this.queryType = newQueryType;
    } else {
      this.queryType = "UNKNOWN";
    }
  }

  /**
   * @return the represented query normalized or null if the represented query was not valid
   */
  public final ParsedQuery getNormalizedQuery()
  {
    if (this.getValidityStatus() != QueryHandler.Validity.VALID) {
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
    if (this.getValidityStatus() != QueryHandler.Validity.VALID) {
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

    normalizedQuery.getTupleExpr().visit(new QueryModelVisitorBase<VisitorException>() {

      @Override
      public void meet(StatementPattern statementPattern) {
        statementPattern.setSubjectVar(normalizeHelper(statementPattern.getSubjectVar(), strings));
        statementPattern.setObjectVar(normalizeHelper(statementPattern.getObjectVar(), strings));

        normalizeHelper(statementPattern.getPredicateVar(), pIDs);
      }
    });
    normalizedQuery.getTupleExpr().visit(new QueryModelVisitorBase<VisitorException>() {

      @Override
      public void meet(ArbitraryLengthPath arbitraryLengthPath) {
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
          String lastIndexOf;
          if (subjectString.contains("/")) {
            lastIndexOf = "/";
          } else if (subjectString.contains(":")) {
            lastIndexOf = ":";
          } else {
            logger.error("Variable " + var.toString() + " could not be normalized because the urn formatting is not recognized.\n" +
                         "Query was: " + this.getQueryStringWithoutPrefixes());
            return var;
          }
          String uri = subjectString.substring(0, subjectString.lastIndexOf(lastIndexOf)) + lastIndexOf + "QName" + foundNames.get(subjectString);
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
    if (this.getValidityStatus() != QueryHandler.Validity.VALID) {
      return "INVALID";
    }
    String name = Main.exampleQueriesTupleExpr.get(new TupleExprWrapper(this.query.getTupleExpr()));
    if (name == null) {
      return "NONE";
    }
    return name;
  }

  @Override
  public void computeCoordinates()
  {
    this.coordinates = new HashSet<String>();
    if (getValidityStatus() != QueryHandler.Validity.VALID) {
      this.coordinates.add("NONE");
      return;
    }

    try {
      ASTQueryContainer qc = SyntaxTreeBuilder.parseQuery(getQueryString());
      qc.jjtAccept(new ASTVisitorBase() {
        @Override
        public Object visit(ASTString string, Object data) throws VisitorException {
          Pattern pattern = Pattern.compile("^Point\\(([-+]?[\\d]{1,2}\\.\\d+\\s*[-+]?[\\d]{1,3}\\.\\d+?)\\)$");
          Matcher matcher = pattern.matcher(string.getValue());
          if (matcher.find()) {
            coordinates.add(matcher.group(1));
          }
          return super.visit(string, data);
        }
      }, null);
    } catch (TokenMgrError | ParseException | VisitorException e) {
      logger.error("Unexpected error while computing the coordinates in " + getQueryString(), e);
    }
  }

}
