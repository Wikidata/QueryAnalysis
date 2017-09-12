package query;

import com.googlecode.cqengine.attribute.Attribute;

import static com.googlecode.cqengine.query.QueryFactory.attribute;

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

  public static final Attribute<QueryHandlerLite, String> QUERY_STRING = attribute("queryString", QueryHandlerLite::getQueryStringWithoutPrefixes);
}
