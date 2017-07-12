package query;

import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import java.util.LinkedHashMap;

/**
 * @author Julius Gonsior
 */
public class SparqlStatisticsCollector extends QueryModelVisitorBase
{
  private LinkedHashMap<String, Integer> statistics = getDefaultMap();

  /**
   * Returns a map containing all classes that implement the QueryModelNode
   * Interface and are therfore part of the AST TupleExpr
   */
  public static LinkedHashMap<String, Integer> getDefaultMap()
  {
    LinkedHashMap<String, Integer> defaultMap = new LinkedHashMap<>();
    defaultMap.put("Add", 0);
    defaultMap.put("And", 0);
    defaultMap.put("ArbitraryLengthPath", 0);
    defaultMap.put("Avg", 0);
    defaultMap.put("BindingSetAssignment", 0);
    defaultMap.put("BNodeGenerator", 0);
    defaultMap.put("Bound", 0);
    defaultMap.put("Clear", 0);
    defaultMap.put("Coalesce", 0);
    defaultMap.put("Compare", 0);
    defaultMap.put("CompareAll", 0);
    defaultMap.put("CompareAny", 0);
    defaultMap.put("Copy", 0);
    defaultMap.put("Count", 0);
    defaultMap.put("Create", 0);
    defaultMap.put("Datatype", 0);
    defaultMap.put("DeleteData", 0);
    defaultMap.put("DescripbeOperator", 0);
    defaultMap.put("Difference", 0);
    defaultMap.put("Distinct", 0);
    defaultMap.put("EmptySet", 0);
    defaultMap.put("Exists", 0);
    defaultMap.put("Extension", 0);
    defaultMap.put("ExtensionElem", 0);
    defaultMap.put("Filter", 0);
    defaultMap.put("FunctionCall", 0);
    defaultMap.put("Group", 0);
    defaultMap.put("GroupConcat", 0);
    defaultMap.put("GroupElem", 0);
    defaultMap.put("If", 0);
    defaultMap.put("In", 0);
    defaultMap.put("InsertData", 0);
    defaultMap.put("Intersection", 0);
    defaultMap.put("IRIFunction", 0);
    defaultMap.put("IsBNode", 0);
    defaultMap.put("IsLiteral", 0);
    defaultMap.put("Isnumeric", 0);
    defaultMap.put("IsResource", 0);
    defaultMap.put("IsURI", 0);
    defaultMap.put("Join", 0);
    defaultMap.put("Label", 0);
    defaultMap.put("Lang", 0);
    defaultMap.put("LangMatches", 0);
    defaultMap.put("LeftJoin", 0);
    defaultMap.put("Like", 0);
    defaultMap.put("ListMemberOperator", 0);
    defaultMap.put("Load", 0);
    defaultMap.put("LocalName", 0);
    defaultMap.put("MathExpr", 0);
    defaultMap.put("Max", 0);
    defaultMap.put("Min", 0);
    defaultMap.put("Modify", 0);
    defaultMap.put("Move", 0);
    defaultMap.put("MultiProjection", 0);
    defaultMap.put("Namespace", 0);
    defaultMap.put("Not", 0);
    defaultMap.put("Or", 0);
    defaultMap.put("Order", 0);
    defaultMap.put("OrderElem", 0);
    defaultMap.put("Projection", 0);
    defaultMap.put("ProjectionElem", 0);
    defaultMap.put("ProjectionElemList", 0);
    defaultMap.put("QueryRoot", 0);
    defaultMap.put("Reduced", 0);
    defaultMap.put("Regex", 0);
    defaultMap.put("SameTerm", 0);
    defaultMap.put("Sample", 0);
    defaultMap.put("Service", 0);
    defaultMap.put("SingletonSet", 0);
    defaultMap.put("Slice", 0);
    defaultMap.put("StatementPattern", 0);
    defaultMap.put("Str", 0);
    defaultMap.put("Sum", 0);
    defaultMap.put("Union", 0);
    defaultMap.put("ValueConstant", 0);
    defaultMap.put("Var", 0);
    defaultMap.put("ZeroLengthPath", 0);
    return defaultMap;

  }

  public LinkedHashMap<String, Integer> getStatistics()
  {
    return statistics;
  }

  /**
   * default method which get's called by all meet statements
   *
   * @param node
   * @throws Exception
   */
  @Override
  public void meetNode(QueryModelNode node) throws Exception
  {
    String className = node.getClass().getSimpleName();
    statistics.put(className, statistics.get(className) + 1);
    super.meetNode(node);
  }


}
