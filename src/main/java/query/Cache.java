package query;

import org.apache.commons.collections.map.LRUMap;
import org.apache.log4j.Logger;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.openrdf.query.parser.sparql.ast.TokenMgrError;
import scala.Tuple2;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Julius Gonsior
 */
public class Cache
{
  /**
   * Define a static logger variable.
   */
  private static final Logger logger = Logger.getLogger(Cache.class);

  /**
   * Singleton instance
   */
  private static Cache instance;

  /**
   * The memory cached ASTQueryContainer objects.
   */
  private Map<String, ASTQueryContainer> astQueryContainerLRUMap = (Map<String, ASTQueryContainer>) Collections.synchronizedMap(new LRUMap(10000));

  /**
   * The memory cached query handler objects.
   */
  private Map<Tuple2<QueryHandler.Validity, String>, QueryHandler> queryHandlerLRUMap = (Map<Tuple2<QueryHandler.Validity, String>, QueryHandler>) Collections.synchronizedMap(new LRUMap(100000));


  private static DB db = DBMaker.memoryDB().make();
  private static ConcurrentMap queryHandlerCache = Cache.db.hashMap("queryHandlerCache").createOrOpen();

  /**
   * exists only to prevent this Class from being instantiated
   */
  protected Cache()
  {
    //nothing to see here
  }

  protected void finalize() throws Throwable
  {
    Cache.db.close();
    super.finalize();
  }

  public static synchronized Cache getInstance()
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
   * Note that only the validityStatus and queryToAnalyze Setters were called for
   * these QueryHandlers
   *
   * @param validityStatus The validity status which was the result of the decoding process of the URI
   * @param queryToAnalyze The query that should be analyzed and written.
   * @return QueryHandler a QueryHandler object which was created for the same queryString before
   */
  public QueryHandler getQueryHandlerFromCache(QueryHandler.Validity validityStatus, String queryToAnalyze, Class queryHandlerClass)
  {
    Tuple2<QueryHandler.Validity, String> tuple = new Tuple2<QueryHandler.Validity, String>(validityStatus, queryToAnalyze);

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

  public QueryHandler getQueryHandler(QueryHandler.Validity validityStatus, String queryToAnalyze, long line, int day, Class queryHandlerClass)
  {
    //check if requested object already exists in cache
    QueryHandler queryHandler = null;
    String id = QueryHandler.generateId(day, line, queryToAnalyze);
    if (!Cache.queryHandlerCache.containsKey(id)) {
      //if not create a new one
        try {
          queryHandler = (QueryHandler) queryHandlerClass.getConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
          logger.error("Failed to create query handler object" + e);
        }
        queryHandler.setValidityStatus(validityStatus);
        queryHandler.setLine(line);
        queryHandler.setDay(day);
        queryHandler.setQueryString(queryToAnalyze);
      Cache.queryHandlerCache.put(queryHandler.getUniqeId(), queryHandler);
      } else {
      queryHandler = (QueryHandler) Cache.queryHandlerCache.get(id);
    }
          return queryHandler;

  }


}
