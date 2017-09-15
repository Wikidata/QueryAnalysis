package query.factories;

import query.OpenRDFQueryHandler;
import query.QueryHandler;
import query.QueryHandler.Validity;

/**
 * @author adrian
 *
 */
public class OpenRDFQueryHandlerFactory implements QueryHandlerFactory
{
  /**
   * @param validity The validity as determined by the decoding process.
   * @param lineToSet The line this query came from.
   * @param dayToSet The day this query came from.
   * @param queryStringToSet The query as a string.
   * @return An OpenRDFQueryHandler based on the parameters.
   */
  @Override
  public QueryHandler getQueryHandler(Validity validity, Long lineToSet, Integer dayToSet, String queryStringToSet)
  {
    return new OpenRDFQueryHandler(validity, lineToSet, dayToSet, queryStringToSet);
  }

}
