import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.ArrayList;
import java.util.Map;

import java.io.IOException;

public class HashtagsNetwork {
    public static void main(String args[]) {
        TinkerGraph tg = TinkerGraph.open() ;
        final ParameterTool params = ParameterTool.fromArgs(args);
        String INPUT_FILE = params.get("input");
        String OUTPUT_FILE = params.get("output");

        try {
            tg.io(IoCore.graphml()).readGraph(INPUT_FILE);
        } catch( IOException e ) {
            System.out.println("File not found");
            System.exit(1);
        }

        GraphTraversalSource g = tg.traversal();
        // Map of co-occuring hashtags
        Map<?, Long> co_hashtags = g.V().hasLabel("hashtag").as("h1").
                in("HAS_TAG").out("HAS_TAG").where(P.neq("h1")).as("h2").select("h1", "h2").by("tag").
                groupCount().by(Column.values).next();

        TinkerGraph hash_graph = TinkerGraph.open();
        GraphTraversalSource hash_g = hash_graph.traversal();

        for (Map.Entry<?, Long> entry : co_hashtags.entrySet()) {
            ArrayList hashtags = (ArrayList) entry.getKey();
            Long weight = entry.getValue();

            // Corresponding users
            GraphTraversal<Vertex, Vertex> h1 = hash_g.V().has("hashtag", "tag", hashtags.get(0));
            GraphTraversal<Vertex, Vertex> h2 = hash_g.V().has("hashtag", "tag", hashtags.get(1));

            //If first user does not exist yet
            if (!h1.hasNext()) {
                //System.out.println("hashtag " + hashtags.get(0) + " created");
                //hash_g.addV("hashtag").
//                        property("tag", hashtags.get(0),
//                        "n", g.V().has("hashtag", "tag", hashtags.get(0)).
//                                        in("HAS_TAG").count().next()).
//                        iterate();
                hash_graph.addVertex(T.id, hashtags.get(0), T.label, "hashtag",
                        "tag", hashtags.get(0));
            }

            if (!h2.hasNext()) {
                //System.out.println("hashtag " + hashtags.get(1) + " created");
//                hash_g.addV("hashtag").property("tag", hashtags.get(1),
//                        "n", g.V().has("hashtag", "tag", hashtags.get(1)).
//                                in("HAS_TAG").count().next()).
//                        iterate();
                hash_graph.addVertex(T.id, hashtags.get(1), T.label, "hashtag",
                        "tag", hashtags.get(1));
            }
            //System.out.println(hashtags.get(0) + ":" + hashtags.get(1));
            System.out.println(hash_g.E().groupCount().by(T.label).toList());

            hash_g.V().has("hashtag", "tag", hashtags.get(0)).as("h1").
                    V().has("hashtag", "tag", hashtags.get(1)).as("h2").
                    addE("CO_HASHTAG").property("weight", weight).
                    from("h1").to("h2").
                    iterate();
        }

        try {
            hash_graph.io(IoCore.graphml()).writeGraph(OUTPUT_FILE);
        } catch (IOException e) {
            System.out.println("File not found");
            System.exit(1);
        }
    }
}
