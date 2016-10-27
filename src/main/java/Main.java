/*-
 * #%L
 * sparqlQueryTester
 * %%
 * Copyright (C) 2016 QueryAnalysis
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import analyzer.QueryHandler;
import analyzer.SparqlQueryAnalyzer;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;


/**
 * @author jgonsior
 */
public class Main
{

  public static void main(String[] args) throws UnsupportedEncodingException, NoSuchAlgorithmException
  {
    String query = "PREFIX wd: <http://www.wikidata.org/entity/>\n" +
        "PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n" +
        "PREFIX wikibase: <http://wikiba.se/ontology#>\n" +
        "PREFIX p: <http://www.wikidata.org/prop/>\n" +
        "PREFIX ps: <http://www.wikidata.org/prop/statement/>\n" +
        "PREFIX pq: <http://www.wikidata.org/prop/qualifier/>\n" +
        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
        "PREFIX bd: <http://www.bigdata.com/rdf#>\n" +
        "\n" +
        "#Children of Genghis Khan\n" +
        "#added before 2016-10\n" +
        " #defaultView:Graph\n" +
        "PREFIX gas: <http://www.bigdata.com/rdf/gas#>\n" +
        "\n" +
        "SELECT ?item ?itemLabel ?pic ?linkTo\n" +
        "WHERE\n" +
        "{\n" +
        "  SERVICE gas:service {\n" +
        "    gas:program gas:gasClass \"com.bigdata.rdf.graph.analytics.SSSP\" ;\n" +
        "                gas:in wd:Q720 ;\n" +
        "                gas:traversalDirection \"Forward\" ;\n" +
        "                gas:out ?item ;\n" +
        "                gas:out1 ?depth ;\n" +
        "                gas:maxIterations 4 ;\n" +
        "                gas:linkType wdt:P40 .\n" +
        "  }\n" +
        "  OPTIONAL { ?item wdt:P40 ?linkTo }\n" +
        "  OPTIONAL { ?item wdt:P18 ?pic }\n" +
        "  SERVICE wikibase:label {bd:serviceParam wikibase:language \"en\" }\n" +
        "}";
    QueryHandler queryHandler = new QueryHandler(query);
    System.out.println("Number of Variables: " +
        queryHandler.getVariableCount());
    System.out.println("Number of Triples: " + queryHandler.getTripleCount());
  }
}