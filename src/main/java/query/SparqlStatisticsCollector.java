package query;

import org.openrdf.query.parser.sparql.ASTVisitorBase;
import org.openrdf.query.parser.sparql.ast.*;

import java.util.HashMap;


/**
 * @author Julius Gonsior
 */
public class SparqlStatisticsCollector extends ASTVisitorBase
{
  private HashMap<String, Integer> statistics = new HashMap<>();

  public HashMap<String, Integer> getStatistics()
  {
    return statistics;
  }

  /**
   * Increment the statistic by one per keyword, but only once per Query.
   */
  public void add(String keyword)
  {
    statistics.put(keyword, 1);
  }

  private Object visitNode(SimpleNode node, Object data) throws VisitorException
  {
    String className = node.getClass().getSimpleName().substring(3);
    this.add(className);
    return super.visit(node, data);
  }

  public Object visit(ASTSelectQuery node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTConstructQuery node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTDescribeQuery node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTAskQuery node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTLimit node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTOffset node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTOrderClause node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTOrderCondition node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTAnd node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTUnionGraphPattern node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTOptionalGraphPattern node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTGraphPatternGroup node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTNotExistsFunc node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTMinusGraphPattern node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTExistsFunc node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTCount node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTMax node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTMin node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTAvg node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTSum node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTGroupClause node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTHavingClause node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTServiceGraphPattern node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTSample node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTBind node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTGroupConcat node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTRDFValue node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTBindingValue node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTPathAlternative node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTPathElt node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTPathMod node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTPathOneInPropertySet node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTPathSequence node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

  public Object visit(ASTPathNegatedPropertySet node, Object data) throws VisitorException
  {
    return this.visitNode(node, data);
  }

}