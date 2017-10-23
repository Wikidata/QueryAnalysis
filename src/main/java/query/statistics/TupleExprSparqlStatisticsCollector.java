package query.statistics;

import org.openrdf.query.algebra.ArbitraryLengthPath;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.Service;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import java.util.HashMap;
import java.util.LinkedList;


/**
 * @author Julius Gonsior
 */
public class TupleExprSparqlStatisticsCollector extends QueryModelVisitorBase
{
  private HashMap<String, Integer> statistics = new HashMap<>();
  private String primaryLanguage = "--DEFAULT--";

  public HashMap<String, Integer> getStatistics()
  {
    return statistics;
  }

  public String getPrimaryLanguage()
  {
    return primaryLanguage;
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
    LinkedList<String> neededNodes = new LinkedList<>();
    //Distinct, Filter, And, , Join, Reduced, Value,
    neededNodes.add("Distinct");
    neededNodes.add("Filter");
    neededNodes.add("Join");
    neededNodes.add("Reduced");
    neededNodes.add("Graph");
    String className = node.getClass().getSimpleName();
    if (neededNodes.contains(className)) {
      this.add(className);
    }
    super.meetNode(node);
  }

  public void meet(StatementPattern node) throws Exception
  {
    if (node.getParentNode() instanceof Service && node.getSubjectVar().getName().equals("-const-http://www.bigdata.com/rdf#serviceParam-uri") && node.getPredicateVar().getName().equals("-const-http://wikiba.se/ontology#language-uri")) {
      this.add("LangService");

      // save first asked language
      String value = node.getObjectVar().getValue().stringValue();
      int index = value.indexOf(",");
      if (index != -1) {
        this.primaryLanguage = value.substring(0, index);
      } else {
        this.primaryLanguage = value;
      }
    }
    super.meetNode(node);
  }

  public void meet(ArbitraryLengthPath node) throws Exception
  {
    if (node.getMinLength() == 0) {
      this.add("*");
    } else {
      this.add("+");
    }
    super.meetNode(node);
  }
}
