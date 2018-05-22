import edu.stanford.nlp.coref.data.CorefChain;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.ie.util.*;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.semgraph.*;
import edu.stanford.nlp.trees.*;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;

import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;


import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class NERTweets {
    public static void main(String args []) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String OUTPUT_FILE = params.get("output");
        String NAMESPACE = params.get("namespace");
        String FROM = params.get("from");
        String TO = params.get("to");

        Configuration cfg = new HBaseGraphConfiguration()
                .setInstanceType(HBaseGraphConfiguration.InstanceType.DISTRIBUTED)
                .setGraphNamespace(NAMESPACE)
                .setCreateTables(true)
                .set("hbase.zookeeper.quorum", "127.0.0.1")
                .set("zookeeper.znode.parent", "/hbase")
                .setUseSchema(true);

        HBaseGraph tg = HBaseGraph.open(cfg);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

        // We only get the original tweets to link the retweeted status with the corresponding entities
        GraphTraversal<Vertex , Vertex> g = tg.traversal().V().has("tweet", "created",
                P.inside(LocalDateTime.parse(FROM, formatter), LocalDateTime.parse(TO, formatter))).
                where(__.outE("RETWEETED_STATUS").count().is(0));

        // The tagger
        NameThatEntity ner_tagger = new NameThatEntity();
        ner_tagger.initPipeline();

        while (g.hasNext()) {
            System.out.println("\nProcessing...");
            Vertex tweet = g.next();
            String text = tweet.values("text").next().toString();
            Long id = (Long) tweet.values("id").next();

            System.out.println(id + ": " + text);

            // The original tweet and all the retweets in a list
            ArrayList<Vertex> listOfTweets = new ArrayList<>();
            listOfTweets.add(tweet);
            listOfTweets.addAll(tg.traversal().V(id).outE("RETWEETED_STATUS").inV().toList());

            for (NamedEntity entity : ner_tagger.tagIt(text)) {
                System.out.println(entity.getToken() + ":" + entity.getEntity());

                String token = entity.getToken().toLowerCase();
                String namedEntity = entity.getEntity().toLowerCase();

                GraphTraversal<Vertex, Vertex> t_entity = tg.traversal().V().has(namedEntity, "name", token);
                // Create the vertex if it does not exist
                Vertex v_entity;
                if (!t_entity.hasNext()) {
                    v_entity = tg.addVertex(T.label, namedEntity, "name", token);
                } else {
                    v_entity = t_entity.next();
                }

                for (Vertex tw : listOfTweets) {
                    tw.addEdge("MENTIONS", v_entity);
                }
            }

        }

        tg.close();
    }
}
