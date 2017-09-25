/**
 *
 */
package openrdffork;

import anonymize.Anonymizer;
import org.apache.log4j.Logger;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.Dataset;
import org.openrdf.query.IncompatibleOperationException;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedBooleanQuery;
import org.openrdf.query.parser.ParsedGraphQuery;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.sparql.*;
import org.openrdf.query.parser.sparql.ast.*;
import query.OpenRDFQueryHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * @author adrian
 */
public class StandardizingSPARQLParser extends SPARQLParser
{

  /**
   * Define a static logger variable.
   */
  private static final Logger logger = Logger.getLogger(StandardizingSPARQLParser.class);

  /**
   * Moves BIND()-clauses to the top of the query.
   *
   * @param queryToBeDebugged The query to be debugged
   * @throws MalformedQueryException if the query was malformed
   */
  public static void debug(ASTQueryContainer queryToBeDebugged) throws MalformedQueryException
  {
    try {
      queryToBeDebugged.jjtAccept(new ASTVisitorBase()
      {
        public Object visit(ASTBind node, Object data) throws VisitorException
        {
          Node parent = node.jjtGetParent();
          List<Node> siblings = new ArrayList<>();
          List<Node> binds = new ArrayList<>();
          for (int i = 0; i < parent.jjtGetNumChildren(); i++) {
            Node child = parent.jjtGetChild(i);
            if (child.getClass().equals(ASTBind.class)) {
              binds.add(child);
            } else {
              siblings.add(child);
            }
          }
          int i = 0;
          for (Node nodeToAdd : binds) {
            parent.jjtAddChild(nodeToAdd, i);
            i++;
          }
          for (Node nodeToAdd : siblings) {
            parent.jjtAddChild(nodeToAdd, i);
            i++;
          }
          return super.visit(node, data);
        }
      }, null);
    } catch (VisitorException e) {
      throw new MalformedQueryException();
    }
  }

  /**
   * Normalizes a query by:
   * - replacing all variables with var1, var2 ...
   * - replacing all strings with string1, string2 ...
   * - replacing all rdfLiterals with rdfLiteral1, rdfLiteral2 ...
   *
   * @param queryContainer The query to be anonymized
   * @throws MalformedQueryException if the query was malformed
   */
  public static void anonymize(ASTQueryContainer queryContainer) throws MalformedQueryException
  {
    final Map<String, Integer> variables = new HashMap<>();
    final Map<String, Integer> strings = new HashMap<>();

    try {
      queryContainer.jjtAccept(new ASTVisitorBase()
      {

        public Object visit(ASTVar variable, Object data) throws VisitorException
        {
          if (!variables.containsKey(variable.getName())) {
            variables.put(variable.getName(), variables.keySet().size() + 1);
          }
          variable.setName("var" + variables.get(variable.getName()));
          return super.visit(variable, data);
        }

        @Override
        public Object visit(ASTString string, Object data) throws VisitorException
        {
          // Find the datatype for this.
          Node parent = string.jjtGetParent();
          if (parent instanceof ASTRDFLiteral) {
            if (parent.jjtGetNumChildren() > 1) {
              Node sibling = parent.jjtGetChild(1);
              if (sibling instanceof ASTIRI) {
                if (Anonymizer.whitelistedDatatypes.contains(((ASTIRI) sibling).getValue())) {
                  return super.visit(string, data);
                }
              }
            }
          }

          if (!strings.containsKey(string.getValue())) {
            strings.put(string.getValue(), strings.keySet().size() + 1);
          }
          Matcher matcher = OpenRDFQueryHandler.POINT_REGEX.matcher(string.getValue());
          if (matcher.find()) {
            String number = String.valueOf(strings.get(string.getValue()));
            number = number.substring(0, 1) + "." + number.substring(1);
            string.setValue("POINT(" + number + " " + number + ")");
          } else {
            string.setValue("string" + strings.get(string.getValue()));
          }
          return super.visit(string, data);
        }
      }, null);
    } catch (TokenMgrError | VisitorException e) {
      throw new MalformedQueryException(e);
    }
  }

  /**
   * Normalizes a query by:
   * - replacing all variables with var1, var2 ...
   * - replacing all strings with string1, string2 ...
   * - replacing all limits with 1, 2 ...
   * - replacing all numeric literals with 1, 2 ...
   * - replacing all rdfLiterals with rdfLiteral1, rdfLiteral2 ...
   *
   * @param queryContainer The query to be normalized
   * @throws MalformedQueryException if the query was malformed
   */
  public static void normalize(ASTQueryContainer queryContainer) throws MalformedQueryException
  {
    final Map<String, Integer> variables = new HashMap<>();
    final Map<String, Integer> strings = new HashMap<>();
    final Map<Long, Long> limits = new HashMap<>();
    final Map<Long, Long> offsets = new HashMap<>();
    final Map<String, Integer> numericLiterals = new HashMap<>();
    final Map<String, Integer> rdfLiterals = new HashMap<>();
    try {
      queryContainer.jjtAccept(new ASTVisitorBase()
      {

        public Object visit(ASTVar variable, Object data) throws VisitorException
        {
          if (!variables.containsKey(variable.getName())) {
            variables.put(variable.getName(), variables.keySet().size() + 1);
          }
          variable.setName("var" + variables.get(variable.getName()));
          return super.visit(variable, data);
        }

        @Override
        public Object visit(ASTString string, Object data) throws VisitorException
        {
          if (!strings.containsKey(string.getValue())) {
            strings.put(string.getValue(), strings.keySet().size() + 1);
          }
          string.setValue("string" + strings.get(string.getValue()));
          return super.visit(string, data);
        }

        @Override
        public Object visit(ASTLimit limit, Object data) throws VisitorException
        {
          if (!limits.containsKey(limit.getValue())) {
            limits.put(limit.getValue(), (long) (limits.keySet().size() + 1));
          }
          limit.setValue(limits.get(limit.getValue()));
          return super.visit(limit, data);
        }

        @Override
        public Object visit(ASTOffset offset, Object data) throws VisitorException
        {
          if (!offsets.containsKey(offset.getValue())) {
            offsets.put(offset.getValue(), (long) (offsets.keySet().size() + 1));
          }
          offset.setValue(offset.getValue());
          return super.visit(offset, data);
        }

        @Override
        public Object visit(ASTNumericLiteral numericLiteral, Object data) throws VisitorException
        {
          if (!numericLiterals.containsKey(numericLiteral.getValue())) {
            numericLiterals.put(numericLiteral.getValue(), numericLiterals.keySet().size() + 1);
          }
          numericLiteral.setValue(numericLiterals.get(numericLiteral.getValue()).toString());
          return super.visit(numericLiteral, data);
        }

        @Override
        public Object visit(ASTRDFLiteral rdfLiteral, Object data) throws VisitorException
        {
          if (!rdfLiterals.containsKey(rdfLiteral.getLang())) {
            rdfLiterals.put(rdfLiteral.getLang(), rdfLiterals.keySet().size() + 1);
          }
          rdfLiteral.setLang("language-" + rdfLiterals.get(rdfLiteral.getLang()).toString());
          return super.visit(rdfLiteral, data);
        }
      }, null);
    } catch (TokenMgrError | VisitorException e) {
      throw new MalformedQueryException(e);
    }
  }

  /**
   * @param qc      The query container to be parsed
   * @param baseURI The base URI to resolve any possible relative URIs against
   * @return The parsed query
   * @throws MalformedQueryException If the query was in any way malformed
   */
  @SuppressWarnings("WeakerAccess")
  public final ParsedQuery parseQuery(ASTQueryContainer qc, String baseURI) throws MalformedQueryException
  {
    StringEscapesProcessor.process(qc);
    BaseDeclProcessor.process(qc, baseURI);
    Map<String, String> prefixes = StandardizingPrefixDeclProcessor.process(qc);
    WildcardProjectionProcessor.process(qc);
    BlankNodeVarProcessor.process(qc);

    if (qc.containsQuery()) {

      // handle query operation

      TupleExpr tupleExpr = buildQueryModel(qc);

      ParsedQuery query;

      ASTQuery queryNode = qc.getQuery();
      if (queryNode instanceof ASTSelectQuery) {
        query = new ParsedTupleQuery(qc.getSourceString(), tupleExpr);
      } else if (queryNode instanceof ASTConstructQuery) {
        query = new ParsedGraphQuery(qc.getSourceString(), tupleExpr, prefixes);
      } else if (queryNode instanceof ASTAskQuery) {
        query = new ParsedBooleanQuery(qc.getSourceString(), tupleExpr);
      } else if (queryNode instanceof ASTDescribeQuery) {
        query = new ParsedGraphQuery(qc.getSourceString(), tupleExpr, prefixes);
      } else {
        throw new RuntimeException(
            "Unexpected query type: " + queryNode.getClass());
      }

      // Handle dataset declaration
      Dataset dataset = DatasetDeclProcessor.process(qc);
      if (dataset != null) {
        query.setDataset(dataset);
      }

      return query;
    } else {
      throw new IncompatibleOperationException(
          "supplied string is not a query operation");
    }
  }

  @Override
  public final ParsedQuery parseQuery(String queryString, String baseURI)
      throws MalformedQueryException
  {
    try {
      ASTQueryContainer qc = SyntaxTreeBuilder.parseQuery(queryString);
      debug(qc);
      return parseQuery(qc, baseURI);
    } catch (TokenMgrError | ParseException e) {
      throw new MalformedQueryException(e.getMessage(), e);
    }
  }

  /**
   * @param queryString The query to be parsed normalized
   * @param baseURI     The The base URI to resolve any possible relative URIs against
   * @return The parsed and partially normalized query
   * @throws MalformedQueryException If the query was in any way malformed
   */
  public final ParsedQuery parseNormalizeQuery(String queryString, String baseURI)
      throws MalformedQueryException
  {
    try {
      ASTQueryContainer qc = SyntaxTreeBuilder.parseQuery(queryString);
      debug(qc);
      normalize(qc);
      return parseQuery(qc, baseURI);
    } catch (TokenMgrError | ParseException e) {
      throw new MalformedQueryException(e.getMessage(), e);
    }
  }

  private TupleExpr buildQueryModel(Node qc) throws MalformedQueryException
  {
    TupleExprBuilder tupleExprBuilder = new TupleExprBuilder(
        new ValueFactoryImpl());
    try {
      return (TupleExpr) qc.jjtAccept(tupleExprBuilder, null);
    } catch (VisitorException e) {
      throw new MalformedQueryException(e.getMessage(), e);
    }
  }
}
