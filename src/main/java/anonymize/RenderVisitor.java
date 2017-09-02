package anonymize;

import org.openrdf.query.parser.sparql.ast.*;

/**
 * @author adrian
 *
 */
public class RenderVisitor implements SyntaxTreeBuilderVisitor
{
  /**
   * @param node
   *          The node to be visited.
   * @param data
   *          The data to be passed along.
   * @return The concatenated results of the children visit methods.
   * @throws VisitorException
   *           If an exception occurs while visiting the children.
   */
  private String visitChildren(Node node, Object data) throws VisitorException
  {
    return visitChildren(node, data, "");
  }

  /**
   * @param node The node to be visited.
   * @param data The data to be passed along.
   * @param childrenLink The String connecting the results of two nodes visited.
   * @return The results of the individual children, concatenated by childrenLink
   * @throws VisitorException If an exception occurs while visiting the children.
   */
  private String visitChildren(Node node, Object data, String childrenLink) throws VisitorException
  {
    String result = "";
    for (int i = 0; i < node.jjtGetNumChildren(); i++) {
      result += node.jjtGetChild(i).jjtAccept(this, data.toString()).toString() + childrenLink;
    }
    //result = result.substring(0, result.length() - childrenLink.length());
    return result;
  }

  @Override
  public final Object visit(SimpleNode node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTUpdateSequence node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTUpdateContainer node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTQueryContainer node, Object data) throws VisitorException
  {
    System.out.println(node.dump(""));
    return visitChildren(node, data.toString());
  }

  @Override
  public final Object visit(ASTBaseDecl node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTPrefixDecl node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTSelectQuery node, Object data) throws VisitorException
  {
    return visitChildren(node, data.toString());
  }

  @Override
  public final Object visit(ASTSelect node, Object data) throws VisitorException
  {
    String result = data.toString() + "SELECT";
    if (node.isDistinct()) {
      result += " DISTINCT";
    }
    if (node.isReduced()) {
      result += " REDUCED";
    }
    if (node.isWildcard()) {
      result += " *";
    }
    result += visitChildren(node, data.toString() + " ");
    result += "\n";
    return result;
  }

  @Override
  public final Object visit(ASTProjectionElem node, Object data) throws VisitorException
  {
    String result;
    if (node.jjtGetNumChildren() == 2) {
      result = node.jjtGetChild(0).jjtAccept(this, data.toString()).toString();
      result += " AS ";
      result += node.jjtGetChild(1).jjtAccept(this, data.toString()).toString();
    } else {
      result = visitChildren(node, data.toString() + " ");
    }
    return result;
  }

  @Override
  public final Object visit(ASTConstructQuery node, Object data) throws VisitorException
  {
    String result = visitChildren(node, data.toString());
    return result;
  }

  @Override
  public final Object visit(ASTConstruct node, Object data) throws VisitorException
  {
    String result = data.toString() + "CONSTRUCT {\n";
    result += visitChildren(node, data.toString() + " ", ".\n");
    result += data.toString() + "}\n";
    return result;
  }

  @Override
  public final Object visit(ASTDescribeQuery node, Object data) throws VisitorException
  {
    String result = visitChildren(node, data.toString());
    return result;
  }

  @Override
  public final Object visit(ASTDescribe node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTAskQuery node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTDatasetClause node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTWhereClause node, Object data) throws VisitorException
  {
    String result = data.toString() + "WHERE";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTBindingsClause node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTInlineData node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTBindingSet node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTBindingValue node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTGroupClause node, Object data) throws VisitorException
  {
    String result = data.toString() + "GROUP BY ";
    result += visitChildren(node, data.toString() + " ") + "\n";
    return result;
  }

  @Override
  public final Object visit(ASTOrderClause node, Object data) throws VisitorException
  {
    String result = data.toString() + "ORDER BY ";
    result += visitChildren(node, data.toString() + " ") + "\n";
    return result;
  }

  @Override
  public final Object visit(ASTGroupCondition node, Object data) throws VisitorException
  {
    // TODO single child that is expression
    String result;
    if (node.jjtGetNumChildren() == 2) {
      result = "( ";
      result += node.jjtGetChild(0).jjtAccept(this, data.toString()).toString();
      result += " AS ";
      result += node.jjtGetChild(1).jjtAccept(this, data.toString()).toString();
      result += " )";
    } else {
      result = visitChildren(node, data.toString());
    }
    return result;
  }

  @Override
  public final Object visit(ASTHavingClause node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTOrderCondition node, Object data) throws VisitorException
  {
    String result;
    if (node.isAscending()) {
      result = " ASC ";
    } else {
      result = " DESC ";
    }
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTLimit node, Object data) throws VisitorException
  {
    String result = data.toString() + "LIMIT " + node.getValue();
    result += visitChildren(node, data.toString() + " ") + "\n";
    return result;
  }

  @Override
  public final Object visit(ASTOffset node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTGraphPatternGroup node, Object data) throws VisitorException
  {
    String result = data.toString() + "{\n";
    result += visitChildren(node, data.toString() + " ");
    result += data.toString() + "}\n";
    return result;
  }

  @Override
  public final Object visit(ASTBasicGraphPattern node, Object data) throws VisitorException
  {
    String result = visitChildren(node, data, ".\n");
    return result;
  }

  @Override
  public final Object visit(ASTOptionalGraphPattern node, Object data) throws VisitorException
  {
    String result = data.toString() + "OPTIONAL {\n";
    result += visitChildren(node, data.toString());
    result += data.toString() + "}\n";
    return result;
  }

  @Override
  public final Object visit(ASTGraphGraphPattern node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTUnionGraphPattern node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTMinusGraphPattern node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTServiceGraphPattern node, Object data) throws VisitorException
  {
    // TODO Multiple Variables that might be relevant
    String result = data.toString() + "SERVICE ";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTConstraint node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTFunctionCall node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTTriplesSameSubject node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTPropertyList node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTObjectList node, Object data) throws VisitorException
  {
    String result = visitChildren(node, data.toString());
    return result;
  }

  @Override
  public final Object visit(ASTTriplesSameSubjectPath node, Object data) throws VisitorException
  {
    String result = data.toString() + visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTPropertyListPath node, Object data) throws VisitorException
  {
    String result;
    if (node.jjtGetNumChildren() == 3) {
      if (node.jjtGetChild(2) instanceof ASTPropertyListPath) {
        result = node.jjtGetChild(0).jjtAccept(this, data.toString()).toString();
        result += node.jjtGetChild(1).jjtAccept(this, data.toString()).toString();
        result += ";\n";
        result += node.jjtGetChild(2).jjtAccept(this, data.toString()).toString();
      } else {
        result = visitChildren(node, data.toString());
      }
    } else {
      result = visitChildren(node, data.toString());
    }
    return result;
  }

  @Override
  public final Object visit(ASTPathAlternative node, Object data) throws VisitorException
  {
    String result = visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTPathSequence node, Object data) throws VisitorException
  {
    String result = visitChildren(node, data.toString() + " ", "/");
    result = result.substring(0, result.length() - 1);
    return result;
  }

  @Override
  public final Object visit(ASTPathElt node, Object data) throws VisitorException
  {
    // TODO Find query using node.inverse
    String result = visitChildren(node, data.toString());
    return result;
  }

  @Override
  public final Object visit(ASTIRI node, Object data) throws VisitorException
  {
    // TODO Find out if this can have a RDFValue
    String result = " <" + node.getValue() + "> ";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTPathOneInPropertySet node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTPathMod node, Object data) throws VisitorException
  {
    String result;
    if (node.getLowerBound() == 1) {
      result = "+";
    } else {
      if (node.getUpperBound() == 1) {
        result = "?";
      } else {
        result = "*";
      }
    }
    result += visitChildren(node, data.toString());
    return result;
  }

  @Override
  public final Object visit(ASTBlankNodePropertyList node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTCollection node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTVar node, Object data) throws VisitorException
  {
    String result = " ?" + node.getName() + " ";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTOr node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTAnd node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTCompare node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTInfix node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTMath node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTNot node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTNumericLiteral node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTCount node, Object data) throws VisitorException
  {
    String result = "COUNT (";
    if (node.isDistinct()) {
      result += " DISTINCT ";
    }
    if (node.isWildcard()) {
      result += "*";
    } else {
      result += visitChildren(node, data.toString());
    }
    result += ")";
    return result;
  }

  @Override
  public final Object visit(ASTSum node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTMin node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTMax node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTAvg node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTSample node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTGroupConcat node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTMD5 node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTSHA1 node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTSHA224 node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTSHA256 node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTSHA384 node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTSHA512 node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTNow node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTYear node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTMonth node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTDay node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTHours node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTMinutes node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTSeconds node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTTimezone node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTTz node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTRand node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTAbs node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTCeil node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTFloor node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTRound node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTSubstr node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTStrLen node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTUpperCase node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTLowerCase node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTStrStarts node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTStrEnds node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTStrBefore node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTStrAfter node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTReplace node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTConcat node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTContains node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTEncodeForURI node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTIf node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTIn node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTNotIn node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTCoalesce node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTStr node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTLang node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTLangMatches node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTDatatype node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTBound node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTSameTerm node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTIsIRI node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTIsBlank node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTIsLiteral node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTIsNumeric node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTBNodeFunc node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTIRIFunc node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTStrDt node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTStrLang node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTUUID node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTSTRUUID node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTBind node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTRegexExpression node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTExistsFunc node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTNotExistsFunc node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTRDFLiteral node, Object data) throws VisitorException
  {
    // TODO Check what happens if the value field is set
    String result = '"' + visitChildren(node, data) + '"';
    result += "@" + node.getLang();
    return result;
  }

  @Override
  public final Object visit(ASTTrue node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTFalse node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTString node, Object data) throws VisitorException
  {
    String result = node.getValue();
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTQName node, Object data) throws VisitorException
  {
    String result = " " + node.getValue() + " ";
    result += visitChildren(node, data.toString());
    return result;
  }

  @Override
  public final Object visit(ASTBlankNode node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTGraphRefAll node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTGraphOrDefault node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTUnparsedQuadDataBlock node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTQuadsNotTriples node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTLoad node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTClear node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTDrop node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTAdd node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTMove node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTCopy node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTCreate node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTInsertData node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTDeleteData node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTDeleteWhere node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTDeleteClause node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTInsertClause node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTModify node, Object data) throws VisitorException
  {
    // TODO Auto-generated method stub
    String result = data.toString() + node.toString() + "\n";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

}
