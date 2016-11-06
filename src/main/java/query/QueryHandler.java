package query;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryException;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;
import com.hp.hpl.jena.sparql.syntax.ElementVisitorBase;
import com.hp.hpl.jena.sparql.syntax.ElementWalker;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Adrian-Bielefeldt
 */
public class QueryHandler
{
  /**
   * Define a static logger variable.
   */
  private static Logger logger = Logger.getLogger(QueryHandler.class);
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
    } catch (NullPointerException e) {
      this.valid = false;
      this.queryString = "";
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
    if (queryString == null) return -1;
    return queryString.length();
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
    if (!valid) {
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
      logger.warn("Tried to remove formatting from a valid string" +
          "but broke it while doing so.");
      return -1;
    }
    return uncommented.length();
  }

  /**
   * @return Returns the number of variables in the query pattern.
   */
  public final Integer getVariableCountPattern()
  {
    if (!this.valid) {
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
      logger.error("Unexpeted null pointer" +
          "exception while counting variables.", e);
      return -1;
    } catch (Exception e) {
      logger.error("Unexpected error while counting variables.", e);
      return -1;
    }
    return variables.size();
  }

  /**
   * @return Returns the number of variables in the query head.
   */
  public final Integer getVariableCountHead()
  {
    if (!this.valid) {
      return -1;
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
      logger.error("Unexcpected null pointer exception " +
          "while counting triples.", e);
      return -1;
    } catch (Exception e) {
      logger.error("Unexpected error while counting triples.", e);
      return -1;
    }
    return triplesCount;
  }
}
