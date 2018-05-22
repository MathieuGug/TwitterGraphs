import org.apache.commons.configuration.Configuration;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Graph;

import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphConfiguration;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Map;
import java.io.IOException;

public class HashtagsNetwork {
    public static void main(String args[]) {
        // Usage : --output <output_file> --namespace <dB_name> --from <yyyy-MM-dd HH:mm> --to <yyyy-MM-dd HH:mm>

        final ParameterTool params = ParameterTool.fromArgs(args);
        String OUTPUT_FILE = params.get("output");
        String NAMESPACE = params.get("namespace");
        String from = params.get("from");
        String to = params.get("to");

        Configuration cfg = new HBaseGraphConfiguration()
                .setInstanceType(HBaseGraphConfiguration.InstanceType.DISTRIBUTED)
                .setGraphNamespace(NAMESPACE)
                .setCreateTables(true)
                .set("hbase.zookeeper.quorum", "127.0.0.1")
                .set("zookeeper.znode.parent", "/hbase")
                .setUseSchema(true);

        HBaseGraph tg = HBaseGraph.open(cfg);
        GraphTraversalSource g = tg.traversal();


        // On dates
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
        Graph subg = (Graph) g.V().has("tweet", "created",
                P.inside(LocalDateTime.parse(from, formatter), LocalDateTime.parse(to, formatter))).
                //P.lt(LocalDateTime.parse(to, formatter))).
                bothE().subgraph("subGraph").cap("subGraph").next();

        // Map of co-occuring hashtags
        Map<?, Long> co_hashtags = subg.traversal().V().hasLabel("hashtag").as("h1").
                in("HAS_TAG").out("HAS_TAG").where(P.neq("h1")).as("h2").select("h1", "h2").by("tag").
                groupCount().by(Column.values).next();
        tg.close();

        // The graph of the hashtags
        TinkerGraph hash_graph = TinkerGraph.open();
        GraphTraversalSource hash_g = hash_graph.traversal();

        for (Map.Entry<?, Long> entry : co_hashtags.entrySet()) {
            ArrayList hashtags = (ArrayList) entry.getKey();
            Long weight = entry.getValue();

            // Corresponding users
            GraphTraversal<Vertex, Vertex> h1 = hash_g.V(hashtags.get(0));
            GraphTraversal<Vertex, Vertex> h2 = hash_g.V(hashtags.get(1));

            //If first user does not exist yet
            if (!h1.hasNext()) {
                hash_graph.addVertex(T.id, hashtags.get(0), T.label, "hashtag",
                        "tag", hashtags.get(0),
                        "n", subg.traversal().V().has("hashtag", "tag", hashtags.get(0)).in("HAS_TAG").count().next());
            }

            if (!h2.hasNext()) {
                hash_graph.addVertex(T.id, hashtags.get(1), T.label, "hashtag",
                        "tag", hashtags.get(1),
                        "n", subg.traversal().V().has("hashtag", "tag", hashtags.get(1)).in("HAS_TAG").count().next());
            }

            //Check if there is already an edge between the two hashtags
            GraphTraversal<Vertex, Vertex> h12 = hash_g.V(hashtags.get(0)).out().has(T.id, hashtags.get(1));
            GraphTraversal<Vertex, Vertex> h21 = hash_g.V(hashtags.get(1)).out().has(T.id, hashtags.get(0));
            if (!h12.hasNext() && !h21.hasNext()) {
                hash_g.V(hashtags.get(0)).as("h1").
                        V(hashtags.get(1)).as("h2").
                        addE("CO_HASHTAG").property("weight", weight).
                        from("h1").to("h2").
                        iterate();
            }

        }
        try {
            hash_graph.io(IoCore.graphml()).writeGraph(OUTPUT_FILE);
            System.out.println("Hashtags network saved in " + OUTPUT_FILE);
        } catch (IOException e) {
            System.out.println("File not found");
            System.exit(1);
        }
    }
}