package query.factories;

import query.OpenRDFQueryHandler;
import query.QueryHandler;
import query.QueryHandler.Validity;

/**
 * @author adrian
 */
public class OpenRDFQueryHandlerFactory implements QueryHandlerFactory
{
  /**
   * @param validity         The validity as determined by the decoding process.
   * @param lineToSet        The line this query came from.
   * @param dayToSet         The day this query came from.
   * @param queryStringToSet The query as a string.
   * @param userAgentToSet The user agent that send this query.
   * @param currentFileToSet The file this query came from.
   * @param threadNumberToSet The number of the thread (Needs to be unique per thread).
   * @return An OpenRDFQueryHandler based on the parameters.
   */
  @Override
  public QueryHandler getQueryHandler(Validity validity, Long lineToSet, Integer dayToSet, String queryStringToSet, String userAgentToSet, String currentFileToSet, int threadNumberToSet)
  {
    return new OpenRDFQueryHandler(validity, lineToSet, dayToSet, queryStringToSet, userAgentToSet, currentFileToSet, threadNumberToSet);
  }

}
