package query;

import general.Main;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.map.LRUMap;
import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.openrdf.query.parser.sparql.ast.TokenMgrError;
import query.factories.QueryHandlerFactory;
import scala.Tuple2;

import java.sql.*;
import java.util.Collections;
import java.util.Map;

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
   * The memory cached ASTQueryContainer objects.
   */
  private Map<String, ASTQueryContainer> astQueryContainerLRUMap = (Map<String, ASTQueryContainer>) Collections.synchronizedMap(new LRUMap(100000));

  /**
   * The memory cached query handler objects.
   */
  private Map<Tuple2<QueryHandler.Validity, String>, QueryHandler> queryHandlerLRUMap = (Map<Tuple2<QueryHandler.Validity, String>, QueryHandler>) Collections.synchronizedMap(new LRUMap(100000));


  private static Connection onDiskDatabaseConnection = null;

  /**
   * exists only to prevent this Class from being instantiated
   */
  public Cache()
  {
    synchronized (this) {
      if (onDiskDatabaseConnection == null) {
        if (Main.isWithUniqueQueryDetection()) {
          // first open connection
          try {
            //onDiskDatabaseConnection = DriverManager.getConnection("jdbc:sqlite:" + Main.getWorkingDirectory() + "onDiskDatabase.db");
            onDiskDatabaseConnection = DriverManager.getConnection("jdbc:sqlite:/media/bernd/onDiskDatabase.db");
          } catch (SQLException e) {
            logger.error("Could not open the on disk database " + e);
          }

          // then create the databases

          try {
            Statement statement = onDiskDatabaseConnection.createStatement();
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS uniqueQueryIds (queryString TEXT, uniqueId TEXT NOT NULL);  CREATE INDEX queryStringIndex ON uniqueQueryIds(queryString);");
          } catch (SQLException e) {
            logger.error("Could not create the uniqueQueryIds table in the disk database " + e);
          }
        }
      }
    }
  }

  protected void finalize() throws Throwable
  {
    super.finalize();
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
   * @param line The line this query came from.
   * @param day The day this query came from.
   * @param queryHandlerFactory The query handler factory to supply the query handler.
   * @return QueryHandler a QueryHandler object which was created for the same queryString before
   */
  public QueryHandler getQueryHandler(QueryHandler.Validity validityStatus, String queryToAnalyze, long line, int day, QueryHandlerFactory queryHandlerFactory)
  {
    Tuple2<QueryHandler.Validity, String> tuple = new Tuple2<QueryHandler.Validity, String>(validityStatus, queryToAnalyze);

    QueryHandler queryHandler = null;

    //check if requested object already exists in cache
    if (!queryHandlerLRUMap.containsKey(tuple)) {
      //if not create a new one
      queryHandler = queryHandlerFactory.getQueryHandler(validityStatus, line, day, queryToAnalyze);

      if (Main.isWithUniqueQueryDetection()) {

        // check if queryString exists already in the on disk database and if so exchange originalId
        try {
          PreparedStatement preparedStatement = onDiskDatabaseConnection.prepareStatement("SELECT uniqueId FROM uniqueQueryIds WHERE queryString = ?");
          String query = "";
          if (queryHandler.getQueryStringWithoutPrefixes() != null) {
            query = queryHandler.getQueryStringWithoutPrefixes();
          }
          String md5 = DigestUtils.md5Hex(query);
          preparedStatement.setString(1, md5);
          ResultSet resultSet = preparedStatement.executeQuery();

          String uniqueId = null;

          while (resultSet.next()) {
            uniqueId = resultSet.getString("uniqueId");
          }

          if (uniqueId != null) {
            queryHandler.setOriginalId(uniqueId);
          } else {
            preparedStatement = onDiskDatabaseConnection.prepareStatement("INSERT INTO uniqueQueryIds(queryString, uniqueId) VALUES(?,?) ");
            preparedStatement.setString(1, md5);
            preparedStatement.setString(2, queryHandler.getUniqeId());
            preparedStatement.executeUpdate();
          }
        } catch (SQLException e) {
          e.printStackTrace();
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
