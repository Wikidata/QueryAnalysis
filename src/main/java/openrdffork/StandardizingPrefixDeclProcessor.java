package openrdffork;

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
    insertDefaultPrefix(prefixMap, "hint", "http://www.bigdata.com/queryHints#");
    insertDefaultPrefix(prefixMap, "hint", "http://www.bigdata.com/queryHints#");
    insertDefaultPrefix(prefixMap, "gas", "http://www.bigdata.com/rdf/gas#");
    insertDefaultPrefix(prefixMap, "bds", "http://www.bigdata.com/rdf/search#");
    insertDefaultPrefix(prefixMap, "bd", "http://www.bigdata.com/rdf#");
    insertDefaultPrefix(prefixMap, "schema", "http://schema.org/");
    insertDefaultPrefix(prefixMap, "cc", "http://creativecommons.org/ns#");
    insertDefaultPrefix(prefixMap, "geo", "http://www.opengis.net/ont/geosparql#");
    insertDefaultPrefix(prefixMap, "prov", "http://www.w3.org/ns/prov#");
    insertDefaultPrefix(prefixMap, "xsd", "http://www.w3.org/2001/XMLSchema#");
    insertDefaultPrefix(prefixMap, "skos", "http://www.w3.org/2004/02/skos/core#");
    insertDefaultPrefix(prefixMap, "owl", "http://www.w3.org/2002/07/owl#");
    insertDefaultPrefix(prefixMap, "rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
    insertDefaultPrefix(prefixMap, "rdfs", "http://www.w3.org/2000/01/rdf-schema#");
    insertDefaultPrefix(prefixMap, "wdata", "http://www.wikidata.org/wiki/Special:EntityData/");
    insertDefaultPrefix(prefixMap, "wdno", "http://www.wikidata.org/prop/novalue/");
    insertDefaultPrefix(prefixMap, "prn", "http://www.wikidata.org/prop/reference/value-normalized/");
    insertDefaultPrefix(prefixMap, "prv", "http://www.wikidata.org/prop/reference/value/");
    insertDefaultPrefix(prefixMap, "pr", "http://www.wikidata.org/prop/reference/");
    insertDefaultPrefix(prefixMap, "pqn", "http://www.wikidata.org/prop/qualifier/value-normalized/");
    insertDefaultPrefix(prefixMap, "pqv", "http://www.wikidata.org/prop/qualifier/value/");
    insertDefaultPrefix(prefixMap, "pq", "http://www.wikidata.org/prop/qualifier/");
    insertDefaultPrefix(prefixMap, "psn", "http://www.wikidata.org/prop/statement/value-normalized/");
    insertDefaultPrefix(prefixMap, "psv", "http://www.wikidata.org/prop/statement/value/");
    insertDefaultPrefix(prefixMap, "ps", "http://www.wikidata.org/prop/statement/");
    insertDefaultPrefix(prefixMap, "wdv", "http://www.wikidata.org/value/");
    insertDefaultPrefix(prefixMap, "wdref", "http://www.wikidata.org/reference/");
    insertDefaultPrefix(prefixMap, "p", "http://www.wikidata.org/prop/");
    insertDefaultPrefix(prefixMap, "wds", "http://www.wikidata.org/entity/statement/");
    insertDefaultPrefix(prefixMap, "wdt", "http://www.wikidata.org/prop/direct/");
    insertDefaultPrefix(prefixMap, "wd", "http://www.wikidata.org/entity/");
    insertDefaultPrefix(prefixMap, "wikibase", "http://wikiba.se/ontology#");

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
