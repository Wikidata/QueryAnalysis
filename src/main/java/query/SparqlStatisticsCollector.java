package query;

import java.util.LinkedHashMap;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.Slice;
import org.openrdf.query.algebra.Not;
import org.openrdf.query.algebra.Exists;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.ArbitraryLengthPath;
import org.openrdf.query.algebra.Service;
import java.util.ArrayList;


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
    defaultMap.put("+", 0);
    defaultMap.put("*", 0);
    defaultMap.put("And", 0);
    defaultMap.put("ArbitraryLengthPath", 0);
    defaultMap.put("Ask", 0);
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
    defaultMap.put("Construct", 0);
    defaultMap.put("Create", 0);
    defaultMap.put("Datatype", 0);
    defaultMap.put("DeleteData", 0);
    defaultMap.put("DescribeOperator", 0);
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
    defaultMap.put("Having", 0);
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
    defaultMap.put("LangService", 0);
    defaultMap.put("LeftJoin", 0);
    defaultMap.put("Like", 0);
    defaultMap.put("Limit", 0);
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
    defaultMap.put("Not Exists", 0);
    defaultMap.put("Offset", 0);
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
   * Increment the statistic by one per keyword, but only once per Query.
   */
  public void add(String keyword)
  {
    statistics.put(keyword, 1);
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
    this.add(className);
    super.meetNode(node);
  }

  /**
   * Because of reasons the AST doesn't distinguish between Limit and Offset, so a little manual interference is in need of here.
   */
  @Override
  public void meet(Slice node) throws Exception
  {
    if (node.getOffset() != -1) {
      this.add("Offset");
    }
    if (node.getLimit() != -1) {
      this.add("Limit");
    }

    super.meetNode(node);
  }

  /**
   * To catch a Not Exists you need to check the parent node.
   */
  @Override
  public void meet(Exists node) throws Exception
  {
    if (node.getParentNode() instanceof Not) {
      this.add("Not Exists");
    } else {
      this.add("Exists");
    }
    super.meetNode(node);
  }


  public void meet(StatementPattern node) throws Exception
  {
    if (node.getParentNode() instanceof Service && node.getSubjectVar().getName().equals("-const-http://www.bigdata.com/rdf#serviceParam-uri") && node.getPredicateVar().getName().equals("-const-http://wikiba.se/ontology#language-uri")) {
      this.add("LangService");
      this.add("Service");
    }
    super.meetNode(node);
  }

  public void meet(ProjectionElem node) throws Exception
  {
    if (node.getSourceName().startsWith("-const-")) {
      this.add("Construct");
    }
    this.add("ProjectionElem");
    super.meetNode(node);
  }

  public void meet(ArbitraryLengthPath node) throws Exception
  {
    if(node.getMinLength() == 0) {
      this.add("+");
    } else {
      this.add("*");
    }
    super.meetNode(node);
  }
}
