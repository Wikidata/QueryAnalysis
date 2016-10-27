package metrics;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;

/**
 * @author adrian
 *
 */
public class CountTriples implements Metric<Integer>
{

  /**
   * Saves the number of triples found by the walker.
   */
  private int triplesCount;

  @Override
  public final Integer analyzeQuery(Query query)
  {
    triplesCount = 0;

    ElementWalker.walk(query.getQueryPattern(), new ElementVisitorBase()
    {
      public void visit(ElementPathBlock el)
      {
        Iterator<TriplePath> triples = el.patternElts();
        while (triples.hasNext()) {
          triplesCount += 1;
          triples.next();
        }
      }
    });
    return triplesCount;
  }

}
