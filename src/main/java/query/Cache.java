package query;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.index.hash.HashIndex;
import com.googlecode.cqengine.query.Query;
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

import static com.googlecode.cqengine.query.QueryFactory.equal;

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
  private Map<String, ASTQueryContainer> astQueryContainerLRUMap = (Map<String, ASTQueryContainer>) Collections.synchronizedMap(new LRUMap(10));

  /**
   * The memory cached query handler objects.
   */
  private Map<Tuple2<QueryHandler.Validity, String>, QueryHandler> queryHandlerLRUMap = (Map<Tuple2<QueryHandler.Validity, String>, QueryHandler>) Collections.synchronizedMap(new LRUMap(100000));


  private static IndexedCollection<QueryHandlerLite> queryStringToQueryIdDiskMap = new ConcurrentIndexedCollection<>();

  /**
   * exists only to prevent this Class from being instantiated
   */
  protected Cache()
  {
    //nothing to see here
  }

  protected void finalize() throws Throwable
  {
    super.finalize();
  }

  public static synchronized Cache getInstance()
  {
    if (instance == null) {
      instance = new Cache();
      queryStringToQueryIdDiskMap.addIndex(HashIndex.onAttribute(QueryHandlerLite.QUERY_STRING));
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
  public QueryHandler getQueryHandler(QueryHandler.Validity validityStatus, String queryToAnalyze, long line, int day, Class queryHandlerClass)
  {
    Tuple2<QueryHandler.Validity, String> tuple = new Tuple2<QueryHandler.Validity, String>(validityStatus, queryToAnalyze);

    QueryHandler queryHandler = null;

    //check if requested object already exists in cache
    if (!queryHandlerLRUMap.containsKey(tuple)) {
      //if not create a new one
      try {
        queryHandler = (QueryHandler) queryHandlerClass.getConstructor(QueryHandler.Validity.class, Long.class, Integer.class, String.class).newInstance(validityStatus, line, day, queryToAnalyze);
      } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
        logger.error("Failed to create query handler object" + e);
      }


      // check if queryString exists already in queryStringToQueryIdDiskMap and if so exchange originalId
      QueryHandlerLite queryHandlerLite = null;
      Query<QueryHandlerLite> cqEngineQuery = equal(QueryHandlerLite.QUERY_STRING, queryHandler.getQueryStringWithoutPrefixes());
      for (QueryHandlerLite result : queryStringToQueryIdDiskMap.retrieve(cqEngineQuery)) {
        queryHandlerLite = result;
      }

      if (queryHandlerLite != null) {
        queryHandler.setOriginalId(queryHandlerLite.getUniqueId());
      } else {
        queryStringToQueryIdDiskMap.add(new QueryHandlerLite(queryHandler.getUniqeId(), queryHandler.getQueryStringWithoutPrefixes()));
      }

      queryHandlerLRUMap.put(tuple, queryHandler);
    } else {
      queryHandler = queryHandlerLRUMap.get(tuple);

      //if found in cache we need to update the uniqueId to the "real" value!
      queryHandler.setUniqeId(day, line, queryToAnalyze);
    }

    //and return it
    return queryHandler;
  }


}
