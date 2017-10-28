/**
 * 
 */
package query.statistics;

import java.util.HashSet;
import java.util.Set;

import org.openrdf.query.parser.sparql.ast.*;

import openrdffork.IRIShorteningRenderVisitor;

/**
 * @author adrian
 *
 */
public final class NonSimplePropertyPathVisitor implements SyntaxTreeBuilderVisitor
{

  /**
   * The set containing the non-simple property paths.
   */
  private Set<String> nonSimplePropertyPaths = new HashSet<String>();

  /**
   * @param node The node to find the non-simple property paths in.
   * @return {@link #nonSimplePropertyPaths}
   * @throws VisitorException If there was an error calculating the non-simple property paths.
   */
  public Set<String> getNonSimplePropertyPaths(SimpleNode node) throws VisitorException
  {
    nonSimplePropertyPaths = new HashSet<String>();
    node.childrenAccept(this, null);
    return nonSimplePropertyPaths;
  }

  @Override
  public Object visit(SimpleNode node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTUpdateSequence node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTUpdateContainer node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTQueryContainer node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTBaseDecl node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTPrefixDecl node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTSelectQuery node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTSelect node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTProjectionElem node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTConstructQuery node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTConstruct node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTDescribeQuery node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTDescribe node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTAskQuery node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTDatasetClause node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTWhereClause node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTBindingsClause node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTInlineData node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTBindingSet node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTBindingValue node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTGroupClause node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTOrderClause node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTGroupCondition node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTHavingClause node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTOrderCondition node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTLimit node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTOffset node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTGraphPatternGroup node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTBasicGraphPattern node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTOptionalGraphPattern node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTGraphGraphPattern node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTUnionGraphPattern node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTMinusGraphPattern node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTServiceGraphPattern node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTConstraint node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTFunctionCall node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTTriplesSameSubject node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTPropertyList node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTObjectList node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTTriplesSameSubjectPath node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTPropertyListPath propertyListPath, Object data) throws VisitorException
  {
    return propertyListPath.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTPathAlternative pathAlternative, Object data) throws VisitorException
  {
    Node currentNode = pathAlternative;
    do {
      if (currentNode.jjtGetNumChildren() == 1) {
        Node child2 = currentNode.jjtGetChild(0);

        if (child2 instanceof ASTPathSequence) {
          ASTPathSequence pathSequence = (ASTPathSequence) child2;

          if (pathSequence.jjtGetNumChildren() == 1) {
            Node child3 = pathSequence.jjtGetChild(0);

            if (child3 instanceof ASTPathElt) {
              ASTPathElt pathElt = (ASTPathElt) child3;

              if (pathElt.jjtGetNumChildren() == 1) {
                Node child4 = pathElt.jjtGetChild(0);

                if (child4 instanceof ASTQName || child4 instanceof ASTIRI) {

                  if (child4.jjtGetNumChildren() == 0) {
                    return null;
                  }
                } else {
                  currentNode = child4;
                  continue;
                }
              }
            }
          }
        }
      }
      break;
    }
    while (true);

    nonSimplePropertyPaths.add(new IRIShorteningRenderVisitor().visit(pathAlternative, "").toString());
    return null;
  }

  @Override
  public Object visit(ASTPathSequence node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTPathElt node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTIRI node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTPathOneInPropertySet node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTPathMod node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTBlankNodePropertyList node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTCollection node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTVar node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTOr node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTAnd node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTCompare node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTInfix node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTMath node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTNot node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTNumericLiteral node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTCount node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTSum node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTMin node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTMax node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTAvg node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTSample node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTGroupConcat node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTMD5 node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTSHA1 node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTSHA224 node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTSHA256 node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTSHA384 node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTSHA512 node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTNow node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTYear node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTMonth node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTDay node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTHours node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTMinutes node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTSeconds node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTTimezone node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTTz node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTRand node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTAbs node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTCeil node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTFloor node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTRound node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTSubstr node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTStrLen node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTUpperCase node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTLowerCase node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTStrStarts node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTStrEnds node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTStrBefore node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTStrAfter node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTReplace node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTConcat node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTContains node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTEncodeForURI node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTIf node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTIn node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTNotIn node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTCoalesce node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTStr node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTLang node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTLangMatches node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTDatatype node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTBound node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTSameTerm node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTIsIRI node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTIsBlank node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTIsLiteral node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTIsNumeric node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTBNodeFunc node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTIRIFunc node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTStrDt node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTStrLang node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTUUID node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTSTRUUID node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTBind node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTRegexExpression node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTExistsFunc node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTNotExistsFunc node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTRDFLiteral node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTTrue node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTFalse node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTString node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTQName node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTBlankNode node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTGraphRefAll node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTGraphOrDefault node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTUnparsedQuadDataBlock node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTQuadsNotTriples node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTLoad node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTClear node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTDrop node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTAdd node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTMove node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTCopy node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTCreate node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTInsertData node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTDeleteData node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTDeleteWhere node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTDeleteClause node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTInsertClause node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTModify node, Object data) throws VisitorException
  {
    return node.childrenAccept(this, data);
  }
}
