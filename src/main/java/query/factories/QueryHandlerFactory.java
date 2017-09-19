package query.factories;

import query.QueryHandler;
import query.QueryHandler.Validity;

/**
 * @author adrian
 */
public interface QueryHandlerFactory
{
  /**
   * @param validity         The validity as determined by the decoding process.
   * @param lineToSet        The line this query came from.
   * @param dayToSet         The day this query came from.
   * @param queryStringToSet The query as a string.
   * @return A query handler corresponding to this factory based on the parameters.
   */
  QueryHandler getQueryHandler(Validity validity, Long lineToSet, Integer dayToSet, String queryStringToSet);
}
