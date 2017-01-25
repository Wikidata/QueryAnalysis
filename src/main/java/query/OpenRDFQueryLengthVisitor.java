package query;


import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

/**
 * @author: Julius Gonsior
 */
public class OpenRDFQueryLengthVisitor extends QueryModelVisitorBase
{

  private static Logger logger = Logger.getLogger(OpenRDFQueryLengthVisitor.class);
  private int size = 0;

  public int getSize()
  {
    return size;
  }

  /**
   * Increments counter by one for each node of the AST tree
   *
   * @param node
   */

  @Override
  public void meetNode(QueryModelNode node)throws Exception
  {
    size++;
    logger.info(node);
    //logger.info(node.toString().substring(0, StringUtils.ordinalIndexOf(node.toString(), "\n", 3)));
      node.visitChildren(this);
  }

  @Override
  public void meet(Join node) throws Exception {
    node.visitChildren(this);
  }

  @Override
  public void meet(ProjectionElemList node) throws  Exception {
    //ignore select;
  }

  @Override
  public void meet(ExtensionElem node) throws Exception{
    //ignore
  }

  @Override
  public void meet(OrderElem node) throws Exception{
    //size++;
    //don't increment for children!
    //node.visitChildren(this);
  }

  @Override
  public void meet(Projection node) throws Exception{
    node.visitChildren(this);
  }

  @Override
  public void meet(Extension node) throws Exception{
    node.visitChildren(this);
  }


  @Override
  public void meet(StatementPattern node)
  {
    size++;
    logger.info("visiting statement" + node);
  }
}
