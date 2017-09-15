package openrdffork;

import org.openrdf.query.parser.sparql.ast.*;

/**
 * @author adrian
 */
public class RenderVisitor implements SyntaxTreeBuilderVisitor
{
  /**
   * The default value to link two visited children.
   */
  private String defaultChildrenLink = "";

  /**
   * The default value to start visiting children.
   */
  private int defaultStartIndex = 0;

  /**
   * @param node The node to be visited.
   * @param data The data to be passed along.
   * @return The concatenated results of the children visit methods.
   * @throws VisitorException If an exception occurs while visiting the children.
   */
  private String visitChildren(Node node, Object data) throws VisitorException
  {
    return visitChildren(node, data, defaultChildrenLink, defaultStartIndex);
  }

  /**
   * @param node         The node to be visited.
   * @param data         The data to be passed along.
   * @param childrenLink The String connecting the results of two nodes visited.
   * @return The results of the individual children, concatenated by childrenLink
   * @throws VisitorException If an exception occurs while visiting the children.
   */
  private String visitChildren(Node node, Object data, String childrenLink) throws VisitorException
  {
    return visitChildren(node, data, childrenLink, defaultStartIndex);
  }

  /**
   * @param node       The node to be visited.
   * @param data       The data to be passed along.
   * @param startIndex The start index for the children to be visited.
   * @return The results of the individual children, concatenated by childrenLink
   * @throws VisitorException If an exception occurs while visiting the children.
   */
  private String visitChildren(Node node, Object data, int startIndex) throws VisitorException
  {
    return visitChildren(node, data, defaultChildrenLink, startIndex);
  }

  /**
   * @param node         The node to be visited.
   * @param data         The data to be passed along.
   * @param childrenLink The String connecting the results of two nodes visited.
   * @param startIndex   The start index for the children to be visited.
   * @return The results of the individual children, concatenated by childrenLink
   * @throws VisitorException If an exception occurs while visiting the children.
   */
  private String visitChildren(Node node, Object data, String childrenLink, int startIndex) throws VisitorException
  {
    String result = "";
    for (int i = startIndex; i < node.jjtGetNumChildren(); i++) {
      result += node.jjtGetChild(i).jjtAccept(this, data.toString()).toString();
      if (i < node.jjtGetNumChildren() - 1) {
        result += childrenLink;
      }
    }
    return result;
  }

  /**
   * @param node The node to be visited.
   * @param data The data to be passed along.
   * @param call The call to be constructed.
   * @return The content of the child nodes wrapped in <call> ( <child nodes> ).
   * @throws VisitorException If an exception occurs while visiting the children.
   */
  private String BuiltInCall(Node node, Object data, String call) throws VisitorException
  {
    return BuiltInCall(node, data, call, "");
  }

  /**
   * @param node         The node to be visited.
   * @param data         The data to be passed along.
   * @param call         The call to be constructed.
   * @param childrenLink The string used to concatenate the child nodes.
   * @return The content of the child nodes wrapped in <call> ( <child nodes> ).
   * @throws VisitorException If an exception occurs while visiting the children.
   */
  private String BuiltInCall(Node node, Object data, String call, String childrenLink) throws VisitorException
  {
    String result = " " + call + " ( ";
    for (int i = 0; i < node.jjtGetNumChildren(); i++) {
      result += node.jjtGetChild(i).jjtAccept(this, data).toString();
      if (i < node.jjtGetNumChildren() - 1) {
        result += childrenLink;
      }
    }
    result += " ) ";
    return result;
  }

  /**
   * @param node The node to be visited.
   * @param data The data to be passed along.
   * @param call The call to be constructed.
   * @return The content of the child nodes wrapped in <call> ( DISTINCT? <child nodes> ).
   * @throws VisitorException If an exception occurs while visiting the children.
   */
  private String Aggregate(ASTAggregate node, Object data, String call) throws VisitorException
  {
    String result = " " + call + " (";
    if (node.isDistinct()) {
      result += " DISTINCT ";
    }
    result += visitChildren(node, data.toString());
    result += " )";
    return result;
  }

  /**
   * @param node The node to be visited. Should be either ASTBindingsClause or ASTInlineData.
   * @param data The data to be passed along.
   * @return The VALUES-Clause constructed from this node. (VALUES (?var1, ?var2) {(<value1>, <value2>)})
   * @throws VisitorException If an exception occurs while visiting the children.
   */
  private String values(Node node, Object data) throws VisitorException
  {
    String result = data.toString() + " VALUES ( ";
    int i = 0;
    while (node.jjtGetChild(i) instanceof ASTVar) {
      result += node.jjtGetChild(i).jjtAccept(this, data).toString();
      i++;
      if (i >= node.jjtGetNumChildren()) {
        break;
      }
    }
    result += " ) {\n";
    result += visitChildren(node, data.toString() + " ", i);
    result += data.toString() + " }\n";
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
    //System.out.println(node.dump(""));
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
    // TODO Could this have a number of children different from 1?
    String result;
    if (node.jjtGetNumChildren() == 1) {
      result = data.toString() + "PREFIX ";
      result += node.getPrefix() + ":";
      result += node.jjtGetChild(0).jjtAccept(this, data);
      result += "\n";
    } else {
      result = visitChildren(node, data.toString() + " ");
    }
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
      result = "( ";
      result += node.jjtGetChild(0).jjtAccept(this, data.toString()).toString();
      result += " AS ";
      result += node.jjtGetChild(1).jjtAccept(this, data.toString()).toString();
      result += " )";
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
    result += "\n";
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
    String result = data.toString() + " ASK\n";
    result += visitChildren(node, data.toString());
    return result;
  }

  @Override
  public final Object visit(ASTDatasetClause node, Object data) throws VisitorException
  {
    String result = data.toString() + "FROM ";
    if (node.isNamed()) {
      result += "NAMED ";
    }
    result += visitChildren(node, data.toString());
    result += "\n";
    return result;
  }

  @Override
  public final Object visit(ASTWhereClause node, Object data) throws VisitorException
  {
    String result = data.toString() + "WHERE ";
    result += visitChildren(node, data.toString());
    return result;
  }

  @Override
  public final Object visit(ASTBindingsClause node, Object data) throws VisitorException
  {
    return values(node, data);
  }

  @Override
  public final Object visit(ASTInlineData node, Object data) throws VisitorException
  {
    return values(node, data);
  }

  @Override
  public final Object visit(ASTBindingSet node, Object data) throws VisitorException
  {
    String result = data.toString() + " ( ";
    result += visitChildren(node, data.toString());
    result += " )\n";
    return result;
  }

  @Override
  public final Object visit(ASTBindingValue node, Object data) throws VisitorException
  {
    String result;
    if (node.jjtGetNumChildren() == 0) {
      result = " UNDEF ";
    } else {
      result = visitChildren(node, data.toString());
    }
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
    String result = " HAVING ";
    result += visitChildren(node, data.toString());
    result += " ";
    return result;
  }

  @Override
  public final Object visit(ASTOrderCondition node, Object data) throws VisitorException
  {
    String result;
    if (node.isAscending()) {
      result = "ASC(";
    } else {
      result = " DESC(";
    }
    result += visitChildren(node, data.toString() + " ");
    result += ")";
    return result;
  }

  @Override
  public final Object visit(ASTLimit node, Object data) throws VisitorException
  {
    String result = data.toString() + "LIMIT " + node.getValue();
    result += visitChildren(node, data.toString());
    result += "\n";
    return result;
  }

  @Override
  public final Object visit(ASTOffset node, Object data) throws VisitorException
  {
    String result = data.toString() + "OFFSET " + node.getValue();
    result += visitChildren(node, data.toString());
    result += "\n";
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
    String result = "";
    for (int i = 0; i < node.jjtGetNumChildren(); i++) {
      // TODO Ugly workaround, but the tree does not differentiate between filter and constraint.
      if (node.jjtGetChild(i) instanceof ASTConstraint) {
        result += " FILTER (";
        result += node.jjtGetChild(i).jjtAccept(this, data.toString()).toString();
        result += ") ";
      } else {
        result += node.jjtGetChild(i).jjtAccept(this, data.toString()).toString();
      }
      result += ".\n";
    }
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
    String result = data.toString() + "GRAPH ";
    result += visitChildren(node, data.toString() + " ");
    return result;
  }

  @Override
  public final Object visit(ASTUnionGraphPattern node, Object data) throws VisitorException
  {
    // TODO Could this have a number of children different from 2?
    String result;
    if (node.jjtGetNumChildren() == 2) {
      result = node.jjtGetChild(0).jjtAccept(this, data).toString();
      result += data.toString() + " UNION\n";
      result += node.jjtGetChild(1).jjtAccept(this, data).toString();
    } else {
      result = visitChildren(node, data.toString());
    }
    return result;
  }

  @Override
  public final Object visit(ASTMinusGraphPattern node, Object data) throws VisitorException
  {
    String result = data.toString() + " MINUS ";
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
    // TODO Could there be cases where this does not represent filter?
    String result = data.toString() + " ( ";
    result += visitChildren(node, data.toString());
    result += " ) \n";
    return result;
  }

  @Override
  public final Object visit(ASTFunctionCall node, Object data) throws VisitorException
  {
    String result = node.jjtGetChild(0).jjtAccept(this, data).toString();
    result += " (";
    result += this.visitChildren(node, data, ", ", 1);
    result += ") ";
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
    String result = visitChildren(node, data.toString(), ", ");
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
    String result = "";
    if (node.jjtGetNumChildren() > 1) {
      result += "(" + visitChildren(node, data.toString() + " ", "|") + ")";
    } else {
      result += visitChildren(node, data.toString() + " ", "|");
    }
    return result;
  }

  @Override
  public final Object visit(ASTPathSequence node, Object data) throws VisitorException
  {
    String result = "";
    if (node.jjtGetNumChildren() > 1) {
      result += "(" + visitChildren(node, data.toString() + " ", "/") + ")";
    } else {
      result += visitChildren(node, data.toString() + " ", "/");
    }
    return result;
  }

  @Override
  public final Object visit(ASTPathElt node, Object data) throws VisitorException
  {
    // TODO Am I interpreting the ASTPathOneInPropertySet correctly?
    String result = "";
    if (node.isInverse()) {
      result += "^";
    }
    boolean allPathOneInPropertySet = true;
    for (int i = 0; i < node.jjtGetNumChildren(); i++) {
      if (!(node.jjtGetChild(i) instanceof ASTPathOneInPropertySet)) {
        allPathOneInPropertySet = false;
        break;
      }
    }
    if (allPathOneInPropertySet) {
      result += "(!(" + visitChildren(node, data.toString(), "|") + "))";
    } else {
      result += visitChildren(node, data.toString());
    }
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
    String result = "";
    if (node.isInverse()) {
      result += "^";
    }
    result += visitChildren(node, data.toString());
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
    String result = " [ ";
    result += visitChildren(node, data.toString());
    result += " ] ";
    return result;
  }

  @Override
  public final Object visit(ASTCollection node, Object data) throws VisitorException
  {
    // TODO What happens if node's varname is set?
    String result;
    if (node.getVarName() == null) {
      result = data.toString() + " (";
      result += visitChildren(node, data.toString());
      result += ")";
    } else {
      result = data.toString() + node.toString() + "\n";
      result += visitChildren(node, data.toString() + " ");
    }
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
    String result = "";
    result += visitChildren(node, data.toString(), " || ");
    return result;
  }

  @Override
  public final Object visit(ASTAnd node, Object data) throws VisitorException
  {
    String result = "";
    result += visitChildren(node, data.toString(), " && ");
    return result;
  }

  @Override
  public final Object visit(ASTCompare node, Object data) throws VisitorException
  {
    // TODO Could this have a number of children different from 2?
    String result;
    if (node.jjtGetNumChildren() == 2) {
      result = "( " + node.jjtGetChild(0).jjtAccept(this, data).toString();
      result += " " + node.getOperator().getSymbol() + " ";
      result += node.jjtGetChild(1).jjtAccept(this, data).toString();
      result += " )";
    } else {
      result = visitChildren(node, data.toString());
    }
    return result;
  }

  @Override
  public final Object visit(ASTInfix node, Object data) throws VisitorException
  {
    // TODO Could there be other infix forms?
    String result = visitChildren(node, data.toString());
    return result;
  }

  @Override
  public final Object visit(ASTMath node, Object data) throws VisitorException
  {
    // TODO Are we setting the brackets correct?
    String result = " ( ";
    result += visitChildren(node, data.toString(), node.getOperator().getSymbol());
    result += " ) ";
    return result;
  }

  @Override
  public final Object visit(ASTNot node, Object data) throws VisitorException
  {
    String result = "!(";
    result += visitChildren(node, data.toString());
    result += ")";
    return result;
  }

  @Override
  public final Object visit(ASTNumericLiteral node, Object data) throws VisitorException
  {
    // TODO Find out if this can have a RDFValue
    String result = '"' + node.getValue() + "\"^^<";
    result += node.getDatatype();
    result += "> ";
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
    result += " )";
    return result;
  }

  @Override
  public final Object visit(ASTSum node, Object data) throws VisitorException
  {
    return Aggregate(node, data, "SUM");
  }

  @Override
  public final Object visit(ASTMin node, Object data) throws VisitorException
  {
    return Aggregate(node, data, "MIN");
  }

  @Override
  public final Object visit(ASTMax node, Object data) throws VisitorException
  {
    return Aggregate(node, data, "MAX");
  }

  @Override
  public final Object visit(ASTAvg node, Object data) throws VisitorException
  {
    return Aggregate(node, data, "AVG");
  }

  @Override
  public final Object visit(ASTSample node, Object data) throws VisitorException
  {
    return Aggregate(node, data, "SAMPLE");
  }

  @Override
  public final Object visit(ASTGroupConcat node, Object data) throws VisitorException
  {
    // TODO Could this have a number of children not in [1, 2]?
    String result;
    if (node.jjtGetNumChildren() == 1 || node.jjtGetNumChildren() == 2) {
      result = " GROUP_CONCAT ( ";
      if (node.isDistinct()) {
        result += " DISTINCT ";
      }
      result += node.jjtGetChild(0).jjtAccept(this, data).toString();
      if (node.jjtGetNumChildren() == 2) {
        result += "; SEPARATOR = " + node.jjtGetChild(1).jjtAccept(this, data).toString();
      }
      result += " ) ";
    } else {
      result = visitChildren(node, data.toString());
    }
    return result;
  }

  @Override
  public final Object visit(ASTMD5 node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "MD5");
  }

  @Override
  public final Object visit(ASTSHA1 node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "SHA1");
  }

  @Override
  public final Object visit(ASTSHA224 node, Object data) throws VisitorException
  {
    // TODO There is no SHA224 in the SPARQL specifications, not sure if I interpret this correctly.
    return BuiltInCall(node, data, "SHA224");
  }

  @Override
  public final Object visit(ASTSHA256 node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "SHA256");
  }

  @Override
  public final Object visit(ASTSHA384 node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "SHA384");
  }

  @Override
  public final Object visit(ASTSHA512 node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "SHA512");
  }

  @Override
  public final Object visit(ASTNow node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "NOW");
  }

  @Override
  public final Object visit(ASTYear node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "YEAR");
  }

  @Override
  public final Object visit(ASTMonth node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "MONTH");
  }

  @Override
  public final Object visit(ASTDay node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "DAY");
  }

  @Override
  public final Object visit(ASTHours node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "HOURS");
  }

  @Override
  public final Object visit(ASTMinutes node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "MINUTES");
  }

  @Override
  public final Object visit(ASTSeconds node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "SECONDS");
  }

  @Override
  public final Object visit(ASTTimezone node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "TIMEZONE");
  }

  @Override
  public final Object visit(ASTTz node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "TZ ");
  }

  @Override
  public final Object visit(ASTRand node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "RAND");
  }

  @Override
  public final Object visit(ASTAbs node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "ABS");
  }

  @Override
  public final Object visit(ASTCeil node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "CEIL");
  }

  @Override
  public final Object visit(ASTFloor node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "FLOOR");
  }

  @Override
  public final Object visit(ASTRound node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "ROUND");
  }

  @Override
  public final Object visit(ASTSubstr node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "SUBSTR", ",");
  }

  @Override
  public final Object visit(ASTStrLen node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "STRLEN");
  }

  @Override
  public final Object visit(ASTUpperCase node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "UCASE");
  }

  @Override
  public final Object visit(ASTLowerCase node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "LCASE");
  }

  @Override
  public final Object visit(ASTStrStarts node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "STRSTARTS", ",");
  }

  @Override
  public final Object visit(ASTStrEnds node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "STRENDS", ",");
  }

  @Override
  public final Object visit(ASTStrBefore node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "STRBEFORE", ",");
  }

  @Override
  public final Object visit(ASTStrAfter node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "STRAFTER", ",");
  }

  @Override
  public final Object visit(ASTReplace node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "REPLACE", ",");
  }

  @Override
  public final Object visit(ASTConcat node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "CONCAT", ",");
  }

  @Override
  public final Object visit(ASTContains node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "CONTAINS", ",");
  }

  @Override
  public final Object visit(ASTEncodeForURI node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "ENCODE_FOR_URI");
  }

  @Override
  public final Object visit(ASTIf node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "IF", ",");
  }

  @Override
  public final Object visit(ASTIn node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "IN", ",");
  }

  @Override
  public final Object visit(ASTNotIn node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "NOT IN", ",");
  }

  @Override
  public final Object visit(ASTCoalesce node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "COALESCE", ",");
  }

  @Override
  public final Object visit(ASTStr node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "STR");
  }

  @Override
  public final Object visit(ASTLang node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "LANG");
  }

  @Override
  public final Object visit(ASTLangMatches node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "LANGMATCHES", ", ");
  }

  @Override
  public final Object visit(ASTDatatype node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "DATATYPE");
  }

  @Override
  public final Object visit(ASTBound node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "BOUND");
  }

  @Override
  public final Object visit(ASTSameTerm node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "sameTerm", ",");
  }

  @Override
  public final Object visit(ASTIsIRI node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "isIRI");
  }

  @Override
  public final Object visit(ASTIsBlank node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "isBLANK");
  }

  @Override
  public final Object visit(ASTIsLiteral node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "isLITERAL");
  }

  @Override
  public final Object visit(ASTIsNumeric node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "isNUMERIC");
  }

  @Override
  public final Object visit(ASTBNodeFunc node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "BNODE");
  }

  @Override
  public final Object visit(ASTIRIFunc node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "IRI");
  }

  @Override
  public final Object visit(ASTStrDt node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "STRDT", ",");
  }

  @Override
  public final Object visit(ASTStrLang node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "STRLANG", ",");
  }

  @Override
  public final Object visit(ASTUUID node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "UUID");
  }

  @Override
  public final Object visit(ASTSTRUUID node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "STRUUID");
  }

  @Override
  public final Object visit(ASTBind node, Object data) throws VisitorException
  {
    // TODO Could this have a number of children different from 2?
    String result;
    if (node.jjtGetNumChildren() == 2) {
      result = data.toString() + " BIND ( ";
      result += node.jjtGetChild(0).jjtAccept(this, data).toString();
      result += " AS ";
      result += node.jjtGetChild(1).jjtAccept(this, data).toString();
      result += ")";
    } else {
      result = visitChildren(node, data.toString());
    }
    return result;
  }

  @Override
  public final Object visit(ASTRegexExpression node, Object data) throws VisitorException
  {
    return BuiltInCall(node, data, "REGEX", ",");
  }

  @Override
  public final Object visit(ASTExistsFunc node, Object data) throws VisitorException
  {
    String result = " EXISTS ";
    result += visitChildren(node, data.toString());
    return result;
  }

  @Override
  public final Object visit(ASTNotExistsFunc node, Object data) throws VisitorException
  {
    String result = " NOT EXISTS ";
    result += visitChildren(node, data.toString());
    return result;
  }

  @Override
  public final Object visit(ASTRDFLiteral node, Object data) throws VisitorException
  {
    // TODO Check what happens if the value field is set
    // TODO Could this have a number of children different from two?
    String result;
    if (node.jjtGetNumChildren() == 1 || node.jjtGetNumChildren() == 2) {
      result = " " + '"' + node.jjtGetChild(0).jjtAccept(this, data).toString() + '"';
      if (node.getLang() != null) {
        result += "@" + node.getLang() + " ";
      } else if (node.jjtGetNumChildren() == 2) {
        result += "^^" + node.jjtGetChild(1).jjtAccept(this, data).toString().trim() + " ";
      }
    } else {
      result = visitChildren(node, data.toString());
    }
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
    String result;
    if (node.getID() != null) {
      result = " _:" + node.getID() + " ";
    } else {
      result = " [] ";
    }
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
