package query;

import org.apache.commons.collections.map.LRUMap;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;

import java.util.Collections;
import java.util.Map;

import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.Dataset;
import org.openrdf.query.IncompatibleOperationException;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedBooleanQuery;
import org.openrdf.query.parser.ParsedGraphQuery;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.sparql.*;
import org.openrdf.query.parser.sparql.ast.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author: Julius Gonsior
 */
public class Cache
{
  static int counter = 0;
  private static Cache instance;

  private Map<String, ASTQueryContainer> astQueryContainerLRUMap = (Map<String, ASTQueryContainer>) Collections.synchronizedMap(new LRUMap(100000));

  /**
   * exists only to prevent this Class from being instantiated
   */
  protected Cache() {
    //nothing to see here
  }

  public static Cache getInstance() {
    if(instance == null) {
      instance = new Cache();
    }
    return instance;
  }

  /**
   * @param queryString   the queryString which should be converted to an ASTQueryContainer
   * @return ASTQueryContainer object
   */
  public ASTQueryContainer getAstQueryContainerObjectFor(String queryString) throws MalformedQueryException
  {
    counter++;
    if(counter%10000 == 0) {
      System.out.println("hui");
    }
    //check if requested object already exists in cache
    if(!astQueryContainerLRUMap.containsKey(queryString)) {
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
}