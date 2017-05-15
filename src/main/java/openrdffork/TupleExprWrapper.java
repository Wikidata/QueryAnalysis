package openrdffork;

import org.openrdf.query.algebra.TupleExpr;

/**
 * @author adrian
 * A wrapper class as a workaround for the broken .hashCode()-function of the TupleExpr-class
 */
public class TupleExprWrapper
{
  /**
   * The TupleExpr represented by this wrapper.
   */
  private TupleExpr tupleExpr;

  /**
   * @param tupleExprToSet {@link #tupleExpr}
   */
  public TupleExprWrapper(TupleExpr tupleExprToSet)
  {
    this.tupleExpr = tupleExprToSet;
  }

  /**
   * @return {@link #tupleExpr}
   */
  public TupleExpr getTupleExpr()
  {
    return this.tupleExpr;
  }

  /**
   * @param tupleExprToSet {@link #tupleExpr}
   */
  public void setTupleExpr(TupleExpr tupleExprToSet)
  {
    this.tupleExpr = tupleExprToSet;
  }

  @Override
  public final String toString()
  {
    return this.tupleExpr.toString();
  }

  @Override
  public final int hashCode()
  {
    return this.tupleExpr.toString().hashCode();
  }

  @Override
  public final boolean equals(Object o)
  {
    if (this == o) return true;

    if (null == o) return false;

    if (getClass() != o.getClass()) return false;

    TupleExprWrapper tupleExprWrapper = (TupleExprWrapper) o;

    return this.getTupleExpr().equals(tupleExprWrapper.getTupleExpr());
  }
}
