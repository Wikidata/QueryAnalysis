/**
 *
 */
package QueryHandler;

import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Adrian-Bielefeldt
 */
public class QueryHandler
{
  /**
   * Saves the query-string handed to the constructor.
   */
  private String queryString;
  /**
   * Saves if queryString is a valid query.
   */
  private boolean valid = false;
  /**
   * The query object created from query-string.
   */
  private Query query;
  /**
   * Saves the number of triples in the pattern.
   *
   * @todo Should be local variable of getTripleCount() but cannot because of:
   * Local variable triplesCount defined in an enclosing scope must be final or
   * effectively final.
   */
  private int triplesCount;

  /**
   * @param queryToAnalyze String which (if possible) will be parsed
   *                       for further analysis
   */
  public QueryHandler(String queryToAnalyze)
  {
    this.queryString = queryToAnalyze;
    try {
      this.query = QueryFactory.create(queryToAnalyze);
      this.valid = true;
    } catch (QueryException e) {
      this.valid = false;
    }
  }

  /**
   * @return Returns the query-string represented by this handler.
   */
  public final String getQueryString()
  {
    return queryString;
  }

  /**
   * @return Returns true if the query saved in query-string is valid.
   */
  public final boolean isValid()
  {
    return valid;
  }

  /**
   * @return Returns the number of variables in the query pattern.
   */
  public final int getVariableCount() throws InvalidQueryException
  {
    if (!this.valid) {
      throw new InvalidQueryException();
    }
    final Set<Node> variables = new HashSet<>();

    ElementWalker.walk(query.getQueryPattern(), new ElementVisitorBase()
    {
      public void visit(ElementPathBlock el)
      {
        Iterator<TriplePath> triples = el.patternElts();
        while (triples.hasNext()) {
          TriplePath next = triples.next();
          if (next.getSubject().isVariable()) {
            variables.add(next.getSubject());
          }
          if (next.getPredicate() != null) {
            if (next.getPredicate().isVariable()) {
              variables.add(next.getPredicate());
            }
          }
          if (next.getObject().isVariable()) {
            variables.add(next.getObject());
          }
        }
      }
    });
    return variables.size();
  }

  /**
   * @return Returns the number of triples in the query pattern.
   */
  public final int getTripleCount() throws InvalidQueryException
  {
    if (!this.valid) {
      throw new InvalidQueryException();
    }
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
