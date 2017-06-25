package openrdffork;

import general.Main;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.sparql.ASTVisitorBase;
import org.openrdf.query.parser.sparql.PrefixDeclProcessor;
import org.openrdf.query.parser.sparql.ast.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author adrian
 */
public class StandardizingPrefixDeclProcessor extends PrefixDeclProcessor
{

  /**
   * Processes prefix declarations in queries. This method collects all
   * prefixes that are declared in the supplied query, verifies that prefixes
   * are not redefined and replaces any {@link ASTQName} nodes in the query
   * with equivalent {@link ASTIRI} nodes.
   *
   * @param qc The query that needs to be processed.
   * @return A map containing the prefixes that are declared in the query (key)
   * and the namespace they map to (value).
   * @throws MalformedQueryException If the query contains redefined prefixes or qnames that use
   *                                 undefined prefixes.
   */
  public static Map<String, String> process(ASTOperationContainer qc) throws MalformedQueryException
  {
    List<ASTPrefixDecl> prefixDeclList = qc.getPrefixDeclList();

    //Build a prefix --> IRI map
    Map<String, String> prefixMap = new LinkedHashMap<String, String>();

    for (ASTPrefixDecl prefixDecl : prefixDeclList) {
      String prefix = prefixDecl.getPrefix();
      String iri = prefixDecl.getIRI().getValue();

      if (prefixMap.containsKey(prefix)) {
        throw new MalformedQueryException("Multiple prefix declarations for prefix '" + prefix + "'");
      }

      prefixMap.put(prefix, iri);
    }

    // insert some default prefixes (if not explicitly defined in the query)
    for (Map.Entry<String, String> entry : Main.prefixes.entrySet()) {
      insertDefaultPrefix(prefixMap, entry.getKey(), entry.getValue());
    }

    ASTUnparsedQuadDataBlock dataBlock = null;
    if (qc.getOperation() instanceof ASTInsertData) {
      ASTInsertData insertData = (ASTInsertData) qc.getOperation();
      dataBlock = insertData.jjtGetChild(ASTUnparsedQuadDataBlock.class);

    } else if (qc.getOperation() instanceof ASTDeleteData) {
      ASTDeleteData deleteData = (ASTDeleteData) qc.getOperation();
      dataBlock = deleteData.jjtGetChild(ASTUnparsedQuadDataBlock.class);
    }

    if (dataBlock != null) {
      String prefixes = createPrefixesInSPARQLFormat(prefixMap);
      dataBlock.setDataBlock(prefixes + dataBlock.getDataBlock());
    } else {
      QNameProcessor visitor = new QNameProcessor(prefixMap);
      try {
        qc.jjtAccept(visitor, null);
      } catch (VisitorException e) {
        throw new MalformedQueryException(e);
      }
    }

    return prefixMap;
  }

  /**
   * Taken from org.openrdf.query.parser.sparql.PrefixDeclProcessor.
   * Adds a default prefix to the query.
   *
   * @param prefixMap The prefix map to insert into
   * @param prefix    The prefix to insert
   * @param namespace The namespace for this prefix
   */
  private static void insertDefaultPrefix(Map<String, String> prefixMap, String prefix, String namespace)
  {
    if (!prefixMap.containsKey(prefix) && !prefixMap.containsValue(namespace)) {
      prefixMap.put(prefix, namespace);
    }
  }

  /**
   * Taken from org.openrdf.query.parser.sparql.PrefixDeclProcessor.
   * Turns the prefix-to-namespace map into SPARQL-Code.
   *
   * @param prefixMap The prefix map to be converted
   * @return The prefixes in valid SPARQL
   */
  private static String createPrefixesInSPARQLFormat(Map<String, String> prefixMap)
  {
    StringBuilder sb = new StringBuilder();
    for (Entry<String, String> entry : prefixMap.entrySet()) {
      sb.append("PREFIX");
      final String prefix = entry.getKey();
      if (prefix != null) {
        sb.append(" " + prefix);
      }
      sb.append(":");
      sb.append(" <" + entry.getValue() + "> \n");
    }
    return sb.toString();
  }

  /**
   * Taken from org.openrdf.query.parser.sparql.PrefixDeclProcessor.
   */
  private static class QNameProcessor extends ASTVisitorBase
  {

    /**
     * A map containing the prefixes and namespaces.
     */
    private Map<String, String> prefixMap;

    /**
     * Creates a QNameProcessor for this prefixMap.
     *
     * @param prefixMapToUse The prefix map to process the QNames with.
     */
    QNameProcessor(Map<String, String> prefixMapToUse)
    {
      this.prefixMap = prefixMapToUse;
    }

    @Override
    public Object visit(ASTQName qnameNode, Object data) throws VisitorException
    {
      String qname = qnameNode.getValue();

      int colonIdx = qname.indexOf(':');
      assert colonIdx >= 0 : "colonIdx should be >= 0: " + colonIdx;

      String prefix = qname.substring(0, colonIdx);
      String localName = qname.substring(colonIdx + 1);

      String namespace = prefixMap.get(prefix);
      if (namespace == null) {
        throw new VisitorException("QName '" + qname + "' uses an undefined prefix");
      }

      localName = processEscapesAndHex(localName);

      // Replace the qname node with a new IRI node in the parent node
      ASTIRI iriNode = new ASTIRI(SyntaxTreeBuilderTreeConstants.JJTIRI);
      iriNode.setValue(namespace + localName);
      qnameNode.jjtReplaceWith(iriNode);

      return null;
    }

    /**
     * Taken from org.openrdf.query.parser.sparql.PrefixDeclProcessor.
     *
     * @param localName
     * @return
     */
    private String processEscapesAndHex(String localName)
    {

      // first process hex-encoded chars.
      StringBuffer unencoded = new StringBuffer();
      Pattern hexPattern = Pattern.compile("([^\\\\]|^)(%[A-F\\d][A-F\\d])", Pattern.CASE_INSENSITIVE);
      Matcher m = hexPattern.matcher(localName);
      boolean result = m.find();
      while (result) {
        // we match the previous char because we need to be sure we are not
        // processing an escaped % char rather than
        // an actual hex encoding, for example: 'foo\%bar'.
        String previousChar = m.group(1);
        String encoded = m.group(2);

        int codePoint = Integer.parseInt(encoded.substring(1), 16);
        String decoded = String.valueOf(Character.toChars(codePoint));

        m.appendReplacement(unencoded, previousChar + decoded);
        result = m.find();
      }
      m.appendTail(unencoded);

      // then process escaped special chars.
      StringBuffer unescaped = new StringBuffer();
      Pattern escapedCharPattern = Pattern.compile("\\\\[_~\\.\\-!\\$\\&\\'\\(\\)\\*\\+\\,\\;\\=\\:\\/\\?#\\@\\%]");
      m = escapedCharPattern.matcher(unencoded.toString());
      result = m.find();
      while (result) {
        String escaped = m.group();
        m.appendReplacement(unescaped, escaped.substring(1));
        result = m.find();
      }
      m.appendTail(unescaped);

      return unescaped.toString();
    }

    @Override
    public Object visit(ASTServiceGraphPattern node, Object data) throws VisitorException
    {
      node.setPrefixDeclarations(prefixMap);
      return super.visit(node, data);
    }

  }
}
