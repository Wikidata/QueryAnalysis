/**
 * 
 */
package openrdffork;

import org.openrdf.query.parser.sparql.ast.ASTIRI;
import org.openrdf.query.parser.sparql.ast.VisitorException;

import query.QueryHandler;
import scala.Tuple2;

/**
 * @author adrian
 *
 */
public class IRIShorteningRenderVisitor extends RenderVisitor
{

  @Override
  public final Object visit(ASTIRI node, Object data) throws VisitorException
  {
    String result = super.visit(node, data).toString();
    Tuple2<Boolean, String> shorteningResult = QueryHandler.replaceExplicitURI(node.getValue());

    if (shorteningResult._1) {
      result = " " + shorteningResult._2 + " ";
      result += super.visitChildren(node, data.toString() + " ");
    }
    return result;
  }
}
