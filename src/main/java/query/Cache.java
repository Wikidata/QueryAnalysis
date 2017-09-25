package query;

import general.Main;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.map.LRUMap;
import org.apache.log4j.Logger;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.openrdf.query.parser.sparql.ast.TokenMgrError;
import query.factories.QueryHandlerFactory;
import scala.Tuple2;

import java.util.Collections;
import java.util.Map;

/**
 * Some information to clarify what this class does:
 * first it acts as a memory LRU cache for ASTQueryContainer and QueryHandler objects
 * secondly it is the main place where QueryHandler objects are being created.
 * While creating them, the original and unique Id is being created and stored
 * in a disk based Hash Map to ensure to correctly calculate unique and
 * duplicate queries
 *
 * @author Julius Gonsior
 */
public class Cache
{
  /**
   * Define a static logger variable.
   */
  private static final Logger logger = Logger.getLogger(Cache.class);

  /**
   * The memory cached ASTQueryContainer objects.
   */
  private Map<String, ASTQueryContainer> astQueryContainerLRUMap = (Map<String, ASTQueryContainer>) Collections.synchronizedMap(new LRUMap(100000));

  /**
   * The memory cached query handler objects.
   */
  private Map<Tuple2<QueryHandler.Validity, String>, QueryHandler> queryHandlerLRUMap = (Map<Tuple2<QueryHandler.Validity, String>, QueryHandler>) Collections.synchronizedMap(new LRUMap(100000));

  public static DB mapDb = null;

  private final static int numberOfDiskMaps = 16;

  /**
   * The disk based hash map
   * One diskMap is not enough because MapDB supports only 32bit and is therefore
   * not large enough for our estimated expected monthly amount of queries
   */
  private static HTreeMap<byte[], String>[] onDiskBasedHashMapArray = new HTreeMap[numberOfDiskMaps];

  /**
   * exists only to prevent this Class from being instantiated
   */
  public Cache()
  {

    if (Main.isWithUniqueQueryDetection()) {
      synchronized (this) {
        if (mapDb == null) {
          mapDb = DBMaker.fileDB(Main.getDbLocation()).fileChannelEnable().fileMmapEnable().make();
          for (int i = 0; i < numberOfDiskMaps; i++) {
            onDiskBasedHashMapArray[i] = mapDb.hashMap("map" + i, Serializer.BYTE_ARRAY, Serializer.STRING).createOrOpen();
          }
        }
      }
    }
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
   * @param validityStatus      The validity status which was the result of the decoding process of the URI
   * @param queryToAnalyze      The query that should be analyzed and written.
   * @param line                The line this query came from.
   * @param day                 The day this query came from.
   * @param queryHandlerFactory The query handler factory to supply the query handler.
   * @return QueryHandler a QueryHandler object which was created for the same queryString before
   */
  public QueryHandler getQueryHandler(QueryHandler.Validity validityStatus, String queryToAnalyze, long line, int day, QueryHandlerFactory queryHandlerFactory)
  {
    Tuple2<QueryHandler.Validity, String> tuple = new Tuple2<QueryHandler.Validity, String>(validityStatus, queryToAnalyze);

    QueryHandler queryHandler;


    /**
     * - first check if the new query exists in cache
     * - if query doesn't exists, write with synchronized
     * - if not read like normal
     */


    //check if requested object already exists in cache
    if (!queryHandlerLRUMap.containsKey(tuple)) {
      //if not create a new one
      queryHandler = queryHandlerFactory.getQueryHandler(validityStatus, line, day, queryToAnalyze);

      if (Main.isWithUniqueQueryDetection()) {

        // to prevent null pointer exceptions from empty queries
        String query = "";
        if (queryHandler.getQueryStringWithoutPrefixes() != null) {
          query = queryHandler.getQueryStringWithoutPrefixes();
        }
        byte[] md5 = DigestUtils.md5(query);

        // check if the md5 hash already exists in the onDiskBasedHashMap
        // distribute the queries equally over all hashMaps
        int index = Math.floorMod(query.hashCode(), numberOfDiskMaps);
        if (onDiskBasedHashMapArray[index].containsKey(md5)) {
          queryHandler.setOriginalId(onDiskBasedHashMapArray[index].get(md5));
        } else {
          // we checked unsychronyzed if the query already existed in the code,
          // but if then we write to ensure that we don't add it twice
          synchronized (onDiskBasedHashMapArray[index]) {
            // check again if not another thread was faster (sorry for duplicating the same code from before, but it seemed to me like the easier way to do)
            if (onDiskBasedHashMapArray[index].containsKey(md5)) {
              queryHandler.setOriginalId(onDiskBasedHashMapArray[index].get(md5));
            } else {
              // yay, we're the first and only thread which is allowed to write this queryto the map
              onDiskBasedHashMapArray[index].put(md5, queryHandler.getUniqeId());
              queryHandler.setFirst();
            }
          }
        }
      }

      queryHandlerLRUMap.put(tuple, queryHandler);
    } else {
      queryHandler = queryHandlerLRUMap.get(tuple);

      //if found in cache we need to update the uniqueId to the "real" value!
      queryHandler.setUniqeId(day, line);
    }

    //and return it
    return queryHandler;
  }
}
