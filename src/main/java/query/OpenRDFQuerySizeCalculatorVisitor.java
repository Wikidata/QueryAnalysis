package query;


import org.apache.log4j.Logger;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

/**
 * Calculates size metric for a SPARQL query while counting basically
 * nodes of the abstract syntax tree from OpenRDF
 *
 * @author: Julius Gonsior
 */
public class OpenRDFQuerySizeCalculatorVisitor extends QueryModelVisitorBase
{

  private static Logger logger = Logger.getLogger(OpenRDFQuerySizeCalculatorVisitor.class);
  private int size = 0;

  public int getSize()
  {
    return size;
  }

  /**
   * default method which get's called by all meet statements
   * @param node
   * @throws Exception
   */
  @Override
  public void meetNode(QueryModelNode node ) throws Exception{
    size++;
    super.meetNode(node);
  }

  public void meet(Add node) throws Exception
  {
    meetUpdateExpr(node);
  }

  public void meet(And node) throws Exception
  {
    meetBinaryValueOperator(node);
  }

  public void meet(ArbitraryLengthPath node) throws Exception
  {
    meetNode(node);
  }

  public void meet(Avg node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(BindingSetAssignment node) throws Exception
  {
    meetNode(node);
  }

  public void meet(BNodeGenerator node) throws Exception
  {
    meetNode(node);
  }

  public void meet(Bound node) throws Exception
  {
    meetNode(node);
  }

  public void meet(Clear node) throws Exception
  {
    meetUpdateExpr(node);
  }

  public void meet(Coalesce node) throws Exception
  {
    meetNAryValueOperator(node);
  }

  public void meet(Compare node) throws Exception
  {
    meetBinaryValueOperator(node);
  }

  public void meet(CompareAll node) throws Exception
  {
    meetCompareSubQueryValueOperator(node);
  }

  public void meet(CompareAny node) throws Exception
  {
    meetCompareSubQueryValueOperator(node);
  }

  public void meet(DescribeOperator node) throws Exception
  {
    meetUnaryTupleOperator(node);
  }

  public void meet(Copy node) throws Exception
  {
    meetUpdateExpr(node);
  }

  public void meet(Count node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(Create node) throws Exception
  {
    meetUpdateExpr(node);
  }

  public void meet(Datatype node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(DeleteData node) throws Exception
  {
    meetUpdateExpr(node);
  }

  public void meet(Difference node) throws Exception
  {
    meetBinaryTupleOperator(node);
  }

  public void meet(Distinct node) throws Exception
  {
    meetUnaryTupleOperator(node);
  }

  public void meet(EmptySet node) throws Exception
  {
    meetNode(node);
  }

  public void meet(Exists node) throws Exception
  {
    meetSubQueryValueOperator(node);
  }

  public void meet(Extension node) throws Exception
  {
    meetUnaryTupleOperator(node);
  }

  public void meet(ExtensionElem node) throws Exception
  {
    meetNode(node);
  }

  public void meet(Filter node) throws Exception
  {
    meetUnaryTupleOperator(node);
  }

  public void meet(FunctionCall node) throws Exception
  {
    meetNode(node);
  }

  public void meet(Group node) throws Exception
  {
    meetUnaryTupleOperator(node);
  }

  public void meet(GroupConcat node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(GroupElem node) throws Exception
  {
    meetNode(node);
  }

  public void meet(If node) throws Exception
  {
    meetNode(node);
  }

  public void meet(In node) throws Exception
  {
    meetCompareSubQueryValueOperator(node);
  }

  public void meet(InsertData node) throws Exception
  {
    meetUpdateExpr(node);
  }

  public void meet(Intersection node) throws Exception
  {
    meetBinaryTupleOperator(node);
  }

  public void meet(IRIFunction node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(IsBNode node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(IsLiteral node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(IsNumeric node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(IsResource node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(IsURI node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(Join node) throws Exception
  {
    meetBinaryTupleOperator(node);
  }

  public void meet(Label node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(Lang node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(LangMatches node) throws Exception
  {
    meetBinaryValueOperator(node);
  }

  public void meet(LeftJoin node) throws Exception
  {
    meetBinaryTupleOperator(node);
  }

  public void meet(Like node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(Load node) throws Exception
  {
    meetUpdateExpr(node);
  }

  public void meet(LocalName node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(MathExpr node) throws Exception
  {
    meetBinaryValueOperator(node);
  }

  public void meet(Max node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(Min node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(Modify node) throws Exception
  {
    meetUpdateExpr(node);
  }

  public void meet(Move node) throws Exception
  {
    meetUpdateExpr(node);
  }

  public void meet(MultiProjection node) throws Exception
  {
    meetUnaryTupleOperator(node);
  }

  public void meet(Namespace node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(Not node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(Or node) throws Exception
  {
    meetBinaryValueOperator(node);
  }

  public void meet(Order node) throws Exception
  {
    meetUnaryTupleOperator(node);
  }

  public void meet(OrderElem node) throws Exception
  {
    meetNode(node);
  }

  public void meet(Projection node) throws Exception
  {
    meetUnaryTupleOperator(node);
  }

  public void meet(ProjectionElem node) throws Exception
  {
    meetNode(node);
  }

  public void meet(ProjectionElemList node) throws Exception
  {
    meetNode(node);
  }

  public void meet(QueryRoot node) throws Exception
  {
    meetNode(node);
  }

  public void meet(Reduced node) throws Exception
  {
    meetUnaryTupleOperator(node);
  }

  public void meet(Regex node) throws Exception
  {
    meetBinaryValueOperator(node);
  }

  public void meet(SameTerm node) throws Exception
  {
    meetBinaryValueOperator(node);
  }

  public void meet(Sample node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(Service node) throws Exception
  {
    meetNode(node);
  }

  public void meet(SingletonSet node) throws Exception
  {
    meetNode(node);
  }

  public void meet(Slice node) throws Exception
  {
    meetUnaryTupleOperator(node);
  }

  public void meet(StatementPattern node) throws Exception
  {
    size++;
    //with commenting this line out we don't meet/visit the children of a statement pattern
    //meetNode(node);
  }

  public void meet(Str node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(Sum node) throws Exception
  {
    meetUnaryValueOperator(node);
  }

  public void meet(Union node) throws Exception
  {
    meetBinaryTupleOperator(node);
  }

  public void meet(ValueConstant node) throws Exception
  {
    meetNode(node);
  }

  public void meet(ListMemberOperator node) throws Exception
  {
    meetNAryValueOperator(node);
  }

  public void meet(Var node) throws Exception
  {
    meetNode(node);
  }

  public void meet(ZeroLengthPath node) throws Exception
  {
    meetNode(node);
  }

}
