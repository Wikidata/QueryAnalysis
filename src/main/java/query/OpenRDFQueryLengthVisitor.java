package query;


import org.apache.log4j.Logger;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.sparql.ast.ASTPrefixDecl;

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
   * @param node
   */

  @Override
  public void meetNode(QueryModelNode node) {
    size++;
    logger.info(node);
    try {
      node.visitChildren(this);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void meet(StatementPattern node) {
    size++;
    logger.info("visiting statement" + node);
  }
}
