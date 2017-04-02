package query;

import org.apache.commons.collections.map.LRUMap;
import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.openrdf.query.parser.sparql.ast.TokenMgrError;
import scala.Tuple2;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;


/**
 * @author: Julius Gonsior
 */
public class Cache
{
  /**
   * Define a static logger variable.
   */
  private static final Logger logger = Logger.getLogger(Cache.class);

  static int counter = 0;

  /**
   * Singleton instance
   */
  private static Cache instance;

  private Map<String, ASTQueryContainer> astQueryContainerLRUMap = (Map<String, ASTQueryContainer>) Collections.synchronizedMap(new LRUMap(100000));

  /**
   * the memory cached query handler objects
   */
  private Map<Tuple2<Integer, String>, QueryHandler> queryHandlerLRUMap = (Map<Tuple2<Integer, String>, QueryHandler>) Collections.synchronizedMap(new LRUMap(100000));

  /**
   * exists only to prevent this Class from being instantiated
   */
  protected Cache()
  {
    //nothing to see here
  }

  public static Cache getInstance()
  {
    if (instance == null) {
      instance = new Cache();
    }
    return instance;
  }

  /**
   * @param queryString the queryString which should be converted to an ASTQueryContainer
   * @return ASTQueryContainer object
   */
  public ASTQueryContainer getAstQueryContainerObjectFor(String queryString) throws MalformedQueryException
  {
    //counter++;
    //if(counter%10000 == 0) {
    //  System.out.println("hui");
    //}
    //check if requested object already exists in cache
    if (!astQueryContainerLRUMap.containsKey(queryString)) {
      //if not create a new one
      try {
        astQueryContainerLRUMap.put(queryString, SyntaxTreeBuilder.parseQuery(queryString));
      } catch (TokenMgrError | ParseException e) {
        throw new MalformedQueryException(e.getMessage(), e);
      }
    }

    //and return it
    return astQueryContainerLRUMap.get(queryString);
  }

  /**
   * Returns a QueryHandler object, if it exists in the cache it is being
   * retrieved from there, if not a new one is being created
   * <p>
   * Note that only the validityStatus and queryToAnalyze Setter were called for
   * these QueryHandlers
   *
   * @param validityStatus The validity status which was the result of the decoding process of the URI
   * @param queryToAnalyze The query that should be analyzed and written.
   * @return QueryHandler a QueryHandler object which was created for the same queryString before
   * @todo: cache the result of the getters in QueryHandler internally
   */
  public QueryHandler getQueryHandler(Integer validityStatus, String queryToAnalyze, Class queryHandlerClass)
  {
    Tuple2<Integer, String> tuple = new Tuple2<Integer, String>(validityStatus, queryToAnalyze);
    counter++;
    if (counter % 10000 == 0) {
      System.out.println("hui");
    }
    //check if requested object already exists in cache
    if (!queryHandlerLRUMap.containsKey(tuple)) {
      //if not create a new one
      QueryHandler queryHandler = null;
      try {
        queryHandler = (QueryHandler) queryHandlerClass.getConstructor().newInstance();
      } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
        logger.error("Failed to create query handler object" + e);
      }
      queryHandler.setValidityStatus(validityStatus);
      queryHandler.setQueryString(queryToAnalyze);
      queryHandlerLRUMap.put(tuple, queryHandler);
    }

    //and return it
    return queryHandlerLRUMap.get(tuple);
  }
}