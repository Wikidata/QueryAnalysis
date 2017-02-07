package query;

import general.Main;
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
import org.openrdf.query.parser.sparql.ast.VisitorException;

import java.util.*;

/**
 * @author jgonsior
 */
public class OpenRDFQueryHandler extends QueryHandler
{
  /**
   * The base URI to resolve any possible relative URIs against.
   */
  public static String BASE_URI = "https://query.wikidata.org/bigdata/namespace/wdq/sparql";
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

  /**
   * {@inheritDoc}
   */
  public final void computeQueryType() throws IllegalStateException
  {
    if (this.getValidityStatus() != 1 || !this.getToolName().equals("0")) {
      throw new IllegalStateException();
    }


    ParsedQuery normalizedQuery;
    try {
      normalizedQuery = normalize(query);
    }
    catch (MalformedQueryException | VisitorException e) {
      logger.error("Unexpected error while normalizing " + getQueryString(), e);
      throw new IllegalStateException();
    }

    synchronized (Main.queryTypes) {
      Iterator<ParsedQuery> iterator = Main.queryTypes.keySet().iterator();
      while (iterator.hasNext()) {
        ParsedQuery next = iterator.next();
        if (next.getTupleExpr().equals(normalizedQuery.getTupleExpr())) {
          this.queryType = Main.queryTypes.get(next);
          return;
        }
      }
    }
    Main.queryTypes.put(normalizedQuery, String.valueOf(Main.queryTypes.size()));
    this.queryType = Main.queryTypes.get(normalizedQuery);
    return;
  }

  /**
   * @return the represented query normalized or null if the represented query was not valid
   */
  public final ParsedQuery getNormalizedQuery()
  {
    try {
      return normalize(this.query);
    }
    catch (MalformedQueryException | VisitorException e) {
      return null;
    }
  }

  /**
   * Normalizes a given query by:
   * - replacing all wikidata uris at subject and object positions with sub1, sub2 ... (obj1, obj2 ...).
   *
   * @param queryToNormalize the query to be normalized
   * @return the normalized query
   * @throws MalformedQueryException If the query was malformed (would be a bug since the input was a parsed query)
   * @throws VisitorException If there is an error during normalization
   */
  private ParsedQuery normalize(ParsedQuery queryToNormalize) throws MalformedQueryException, VisitorException
  {
    ParsedQuery normalizedQuery = new StandardizingSPARQLParser().parseNormalizeQuery(queryToNormalize.getSourceString(), BASE_URI);

    final Map<String, Integer> strings = new HashMap<String, Integer>();

    normalizedQuery.getTupleExpr().visit(new QueryModelVisitorBase<VisitorException>() {

        @Override
        public void meet(StatementPattern statementPattern)
        {
          statementPattern.setSubjectVar(normalizeHelper(statementPattern.getSubjectVar(), strings));
          statementPattern.setObjectVar(normalizeHelper(statementPattern.getObjectVar(), strings));
        }
      });
    normalizedQuery.getTupleExpr().visit(new QueryModelVisitorBase<VisitorException>() {

        @Override
        public void meet(ArbitraryLengthPath arbitraryLengthPath)
        {
          arbitraryLengthPath.setSubjectVar(normalizeHelper(arbitraryLengthPath.getSubjectVar(), strings));
          arbitraryLengthPath.setObjectVar(normalizeHelper(arbitraryLengthPath.getObjectVar(), strings));
        }
      });
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
          if (subjectString.startsWith("http://www.wikidata.org/")) {
            if (!foundNames.containsKey(subjectString)) {
              foundNames.put(subjectString, foundNames.size() + 1);
            }
            String uri = subjectString.substring(0, subjectString.lastIndexOf("/")) + "/QName" + foundNames.get(subjectString);
            String name = "-const-" + uri + "-uri";
            return new Var(name, new URIImpl(uri));
          }
        }
      }
    }
    return var;
  }
}
