package query;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryException;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;
import com.hp.hpl.jena.sparql.syntax.ElementVisitorBase;
import com.hp.hpl.jena.sparql.syntax.ElementWalker;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Adrian-Bielefeldt
 */
public class JenaQueryHandler extends QueryHandler
{
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
   * {@inheritDoc}
   */
  public final void update()
  {
    try {
      this.query = QueryFactory.create(getQueryString());
      this.setValidityStatus(1);
    } catch (QueryException e) {
      logger.debug("QUE length:" + this.getLengthNoAddedPrefixes());
      logger.debug("Invalid query: \t" + getQueryString() + "\t->\t" + e.getMessage());
      this.setValidityStatus(-1);
    }
  }

  /**
   * The function returns the length of the query as a string
   * without comments and formatting.
   *
   * @return Returns the length of the query without comments (-1 if invalid).
   * @todo Complete removal of formatting
   * and make sure it cannot break the query.
   */
  public final Integer getStringLengthNoComments()
  {
    if (getValidityStatus() != 1) {
      return -1;
    }
    String uncommented = query.toString().trim().replaceAll("[ ]+", " ");
    uncommented = uncommented.replaceAll("\n ", "\n");
    uncommented = uncommented.replaceAll("(\r?\n){2,}", "$1");
    uncommented = uncommented.replaceAll(": ", ":");
    uncommented = uncommented.replaceAll("\n[{]", "{");
    uncommented = uncommented.replaceAll("[{] ", "{");
    uncommented = uncommented.replaceAll(" [}]", "}");
    try {
      QueryFactory.create(uncommented);
    } catch (QueryException e) {
      getLogger().warn("Tried to remove formatting from a valid string" +
          "but broke it while doing so.");
      return -1;
    }
    return uncommented.length();
  }

  /**
   * @return Returns the number of variables in the query head.
   */
  public final Integer getVariableCountHead()
  {
    if (getValidityStatus() != 1) {
      return -1;
    }
    return query.getProjectVars().size();
  }

  /**
   * @return Returns the number of variables in the query pattern.
   */
  public final Integer getVariableCountPattern()
  {
    if (getValidityStatus() != 1) {
      return -1;
    }
    final Set<Node> variables = new HashSet<>();

    try {
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
    } catch (NullPointerException e) {
      getLogger().error("Unexpeted null pointer" +
          "exception while counting variables.", e);
      return -1;
    } catch (Exception e) {
      getLogger().error("Unexpected error while counting variables.", e);
      return -1;
    }
    return variables.size();
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
    triplesCount = 0;

    try {
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
    } catch (NullPointerException e) {
      getLogger().error("Unexcpected null pointer exception " +
          "while counting triples.", e);
      return -1;
    } catch (Exception e) {
      getLogger().error("Unexpected error while counting triples.", e);
      return -1;
    }
    return triplesCount;
  }
}
