package query;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;

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
  private boolean valid;
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
   * @return Returns true if the query is valid.
   */
  public final boolean isValid()
  {
    return valid;
  }

  /**
   * @return Returns the length of the query with comments and even if invalid.
   * '/n' counts as one character.
   */
  public final Integer getStringLength()
  {
    return queryString.length();
  }

  /**
   * @return Returns the length of the query without comments (null if invalid).
   */
  public final Integer getStringLengthNoComments()
  {
    if (!valid) {
      return null;
    }
    return query.toString().length();
  }

  /**
   * @return Returns the number of variables in the query pattern.
   */
  public final Integer getVariableCountPattern()
  {
    if (!this.valid) {
      return null;
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
   * @return Returns the number of variables in the query head.
   */
  public final Integer getVariableCountHead()
  {
    if (!this.valid) {
      return null;
    }
    return query.getProjectVars().size();
  }

  /**
   * @return Returns the number of triples in the query pattern
   * (including triples in SERIVCE blocks).
   */
  public final Integer getTripleCountWithService()
  {
    if (!this.valid) {
      return null;
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
