package query;

import general.Main;
import openrdffork.StandardizingSPARQLParser;
import openrdffork.TupleExprWrapper;
import org.apache.log4j.Logger;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.ASTVisitorBase;
import org.openrdf.query.parser.sparql.ast.*;
import query.statistics.OpenRDFQuerySizeCalculatorVisitor;
import query.statistics.QueryContainerSparqlStatisticsCollector;
import query.statistics.TupleExprSparqlStatisticsCollector;
import utility.NoURIException;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author jgonsior
 */
public class OpenRDFQueryHandler extends QueryHandler
{
  /**
   * @param validity The validity as determined by the decoding process.
   * @param lineToSet The line this query came from.
   * @param dayToSet The day this query came from.
   * @param queryStringToSet The query as a string.
   */
  public OpenRDFQueryHandler(Validity validity, Long lineToSet, Integer dayToSet, String queryStringToSet)
  {
    super(validity, lineToSet, dayToSet, queryStringToSet);
  }

  /**
   * The base URI to resolve any possible relative URIs against.
   */
  public static final String BASE_URI = "https://query.wikidata.org/bigdata/namespace/wdq/sparql";
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
      this.sparqlStatistics = new HashMap<>();
      return;
    }
    try {
      ASTQueryContainer astQueryContainer = SyntaxTreeBuilder.parseQuery(this.getQueryString());

      QueryContainerSparqlStatisticsCollector queryContainerSparqlStatisticsCollector = new QueryContainerSparqlStatisticsCollector();
      astQueryContainer.jjtAccept(queryContainerSparqlStatisticsCollector, null);

      this.sparqlStatistics = queryContainerSparqlStatisticsCollector.getStatistics();

      TupleExprSparqlStatisticsCollector tupleExprSparqlStatisticsCollector = new TupleExprSparqlStatisticsCollector();
      this.query.getTupleExpr().visitChildren(tupleExprSparqlStatisticsCollector);
      this.query.getTupleExpr().visit(tupleExprSparqlStatisticsCollector);

      this.sparqlStatistics.putAll(tupleExprSparqlStatisticsCollector.getStatistics());

    } catch (TokenMgrError | ParseException e) {
      logger.error("Failed to parse the query although it was found valid - this is a serious bug.", e);
    } catch (VisitorException e) {
      logger.error("Failed to calculate the SPARQL Keyword Statistics. Error occured while visiting the query.", e);
    } catch (Exception e) {
      logger.error("An unknown error occured while computing the sparql statistics: ", e);
    }
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

    final Map<String, Integer> valueConstants = new HashMap<>();

    final Set<String> subjectsAndObjects = new HashSet<String>();
    final Set<String> predicates = new HashSet<String>();

    final Set<String> predicateVariables = new HashSet<String>();

    normalizedQuery.getTupleExpr().visit(new QueryModelVisitorBase<VisitorException>()
    {

      @Override
      public void meet(StatementPattern statementPattern) throws VisitorException
      {
        Var predicate = statementPattern.getPredicateVar();

        if (!predicate.isConstant() && !predicate.isAnonymous()) {
          predicateVariables.add(predicate.getName());
        }

        meetNode(statementPattern);
      }
    }
    );

    normalizedQuery.getTupleExpr().visit(new QueryModelVisitorBase<VisitorException>()
    {

      @Override
      public void meet(ExtensionElem extensionElem) throws VisitorException
      {
        if (!predicateVariables.contains(extensionElem.getName())) {
          extensionElem.setExpr(normalizeValueExprHelper(extensionElem.getExpr(), valueConstants));
        }

        meetNode(extensionElem);
      }
    });

    normalizedQuery.getTupleExpr().visit(new QueryModelVisitorBase<VisitorException>()
    {

      @Override
      public void meet(StatementPattern statementPattern) throws VisitorException
      {
        statementPattern.setSubjectVar(normalizeSubjectsAndObjectsHelper(statementPattern.getSubjectVar(), valueConstants, subjectsAndObjects));
        statementPattern.setObjectVar(normalizeSubjectsAndObjectsHelper(statementPattern.getObjectVar(), valueConstants, subjectsAndObjects));

        try {
          String uri = getURI(statementPattern.getPredicateVar());
          predicates.add(uri);
        } catch (NoURIException e) {
          // NoURIException is used to notify us that there is no URI in this predicate, so we just don't add it.
        }

        checkForVariable(statementPattern.getPredicateVar());
        meetNode(statementPattern);
      }
    });

    normalizedQuery.getTupleExpr().visit(new QueryModelVisitorBase<VisitorException>()
    {

      @Override
      public void meet(ArbitraryLengthPath arbitraryLengthPath) throws VisitorException
      {
        arbitraryLengthPath.setSubjectVar(normalizeSubjectsAndObjectsHelper(arbitraryLengthPath.getSubjectVar(), valueConstants, subjectsAndObjects));
        arbitraryLengthPath.setObjectVar(normalizeSubjectsAndObjectsHelper(arbitraryLengthPath.getObjectVar(), valueConstants, subjectsAndObjects));

        meetNode(arbitraryLengthPath);
      }
    });

    normalizedQuery.getTupleExpr().visit(new QueryModelVisitorBase<VisitorException>()
    {

      @Override
      public void meet(Compare compare) throws VisitorException
      {
        compare.setLeftArg(normalizeValueExprHelper(compare.getLeftArg(), valueConstants));
        compare.setRightArg(normalizeValueExprHelper(compare.getRightArg(), valueConstants));

        meetBinaryValueOperator(compare);
      }
    });

    normalizedQuery.getTupleExpr().visit(new QueryModelVisitorBase<VisitorException>()
    {

      @Override
      public void meet(IsLiteral isLiteral) throws VisitorException
      {
        isLiteral.setArg(normalizeValueExprHelper(isLiteral.getArg(), valueConstants));

        meetUnaryValueOperator(isLiteral);
      }
    });

    this.setqIDs(subjectsAndObjects);
    this.setpIDs(predicates);
    return normalizedQuery;
  }

  /**
   * A helper function to check if a Var is an actual variable.
   *
   * @param var The Var to check.
   */
  private void checkForVariable(Var var)
  {
    if (var != null) {
      if (!var.isConstant()) {
        this.simpleOrComplex = QueryHandler.Complexity.COMPLEX;
      }
    }
  }

  /**
   * A helper function to find the fitting replacement value for wikidata uri normalization.
   *
   * @param valueExpr      The ValueExpr to be normalized.
   * @param valueConstants The list of already found names.
   * @return The normalized name (if applicable)
   */
  private ValueExpr normalizeValueExprHelper(ValueExpr valueExpr, Map<String, Integer> valueConstants)
  {
    String uri;
    try {
      uri = getURI(valueExpr);
    } catch (NoURIException e) {
      return valueExpr;
    }

    if (!valueConstants.containsKey(uri)) {
      valueConstants.put(uri, valueConstants.size());
    }

    try {
      uri = normalizedURI(uri, valueConstants);
    } catch (NoURIException e) {
      return valueExpr;
    }

    ((ValueConstant) valueExpr).setValue(new URIImpl(uri));
    return valueExpr;
  }

  /**
   * A helper function to find the fitting replacement value for wikidata uri normalization.
   *
   * @param var                The variable to be normalized
   * @param foundNames         The list of already found names
   * @param subjectsAndObjects The set to save all found subjects and objects.
   * @return the normalized name (if applicable)
   */
  private Var normalizeSubjectsAndObjectsHelper(Var var, Map<String, Integer> foundNames, Set<String> subjectsAndObjects)
  {
    String uri;
    try {
      uri = getURI(var);
    } catch (NoURIException e) {
      return var;
    }

    if (!foundNames.containsKey(uri)) {
      foundNames.put(uri, foundNames.size() + 1);
      subjectsAndObjects.add(uri);
    }

    try {
      uri = normalizedURI(uri, foundNames);
    } catch (NoURIException e) {
      return var;
    }

    String name = "-const-" + uri + "-uri";
    return new Var(name, new URIImpl(uri));
  }

  /**
   * @param var The variable containing the URI:
   * @return The URI contained in the variable.
   * @throws NoURIException If no URI could be found.
   */
  private String getURI(Var var) throws NoURIException
  {
    if (var == null) {
      throw new NoURIException();
    }
    Value value = var.getValue();
    if (value == null) {
      throw new NoURIException();
    }
    if (!(value instanceof URIImpl)) {
      throw new NoURIException();
    }
    return value.stringValue();
  }

  /**
   * @param valueExpr The value expr containing the URI:
   * @return The URI contained in the variable.
   * @throws NoURIException If no URI could be found.
   */
  private String getURI(ValueExpr valueExpr) throws NoURIException
  {
    if (!(valueExpr instanceof ValueConstant)) {
      throw new NoURIException();
    }
    Value value = ((ValueConstant) valueExpr).getValue();
    if (value == null) {
      throw new NoURIException();
    }
    if (!(value instanceof URIImpl)) {
      throw new NoURIException();
    }
    return value.stringValue();
  }

  /**
   * @param uri        The URI to be normalized
   * @param foundNames The list of already found entities.
   * @return The normalized string based on the already found entities.
   * @throws NoURIException If the supplied string was not a URI.
   */
  private String normalizedURI(String uri, Map<String, Integer> foundNames) throws NoURIException
  {
    String lastIndexOf;
    if (uri.contains("/")) {
      lastIndexOf = "/";
    } else if (uri.contains(":")) {
      lastIndexOf = ":";
    } else {
      logger.error("Variable with uri " + uri + " could not be normalized because the urn formatting is not recognized.\n" +
          "Query was: " + this.getQueryStringWithoutPrefixes());
      throw new NoURIException();
    }
    String normalizedURI = uri.substring(0, uri.lastIndexOf(lastIndexOf)) + lastIndexOf + "QNumber" + foundNames.get(uri);
    return normalizedURI;
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
  public final void computeCoordinates()
  {
    this.coordinates = new HashSet<String>();
    if (getValidityStatus() != QueryHandler.Validity.VALID) {
      this.coordinates.add("NONE");
      return;
    }

    try {
      ASTQueryContainer qc = SyntaxTreeBuilder.parseQuery(getQueryString());
      qc.jjtAccept(new ASTVisitorBase()
      {
        @Override
        public Object visit(ASTString string, Object data) throws VisitorException
        {
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
      this.coordinates.add("ERROR");
    }
  }

  @Override
  public final void computeSimpleOrComplex()
  {
    if (getValidityStatus() != QueryHandler.Validity.VALID) {
      this.simpleOrComplex = QueryHandler.Complexity.NONE;
      return;
    }

    if (this.queryType == null) {
      this.computeQueryType();
    }

    if (this.simpleOrComplex != QueryHandler.Complexity.COMPLEX) {
      for (String predicate : this.getpIDs()) {
        if (checkConstantForWhitelist(predicate) == QueryHandler.Complexity.COMPLEX) {
          this.simpleOrComplex = QueryHandler.Complexity.COMPLEX;
          return;
        }
      }
      this.simpleOrComplex = QueryHandler.Complexity.SIMPLE;
    }
  }

  /**
   * @param constant The constant that should be checked.
   * @return The complexity based on the constant.
   */
  private Complexity checkConstantForWhitelist(String constant)
  {
    for (String whitelistedPredicate : Main.simpleQueryWhitelist) {
      if (constant.matches("^" + whitelistedPredicate + ":.*")) {
        return QueryHandler.Complexity.SIMPLE;
      }
    }
    return QueryHandler.Complexity.COMPLEX;
  }
}
