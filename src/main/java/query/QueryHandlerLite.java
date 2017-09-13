package query;

/**
 * @author: Julius Gonsior
 */
public class QueryHandlerLite
{
  private String uniqueId;
  private String queryStringWithoutPrefixes;

  public String getQueryStringWithoutPrefixes()
  {
    return queryStringWithoutPrefixes;
  }


  public String getUniqueId()
  {
    return uniqueId;
  }

  public QueryHandlerLite(String uniqueId, String queryStringWithoutPrefixes)
  {
    this.uniqueId = uniqueId;
    this.queryStringWithoutPrefixes = queryStringWithoutPrefixes;
  }

}
