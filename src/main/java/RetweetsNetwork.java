import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;

import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RetweetsNetwork {
    public static void main(String args []) {
        final ParameterTool params = ParameterTool.fromArgs(args);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

        String OUTPUT_FILE = params.get("output");
        String NAMESPACE = params.get("namespace");
        LocalDateTime FROM = LocalDateTime.parse(params.get("from"), formatter);
        LocalDateTime TO = LocalDateTime.parse(params.get("to"), formatter);

        Configuration cfg = new HBaseGraphConfiguration()
                .setInstanceType(HBaseGraphConfiguration.InstanceType.DISTRIBUTED)
                .setGraphNamespace(NAMESPACE)
                .setCreateTables(true)
                .set("hbase.zookeeper.quorum", "127.0.0.1")
                .set("zookeeper.znode.parent", "/hbase")
                .setUseSchema(true);

        HBaseGraph tg = HBaseGraph.open(cfg);
        GraphTraversalSource g = tg.traversal();

        // Create a subgraph with a step of 2 from tweets to get the retweeted users as well

        //Graph subg = (Graph) g.V().has("tweet", "created",
        //        P.inside(FROM, TO)).
        //        repeat(__.bothE().subgraph("subGraph").outV()).times(2).cap("subGraph").next();

        // A map [v1, v2] : nb_retweets
        Map<?, Long> nb_retweets = g.V().hasLabel("user").as("u1").
                out().out("RETWEETED_STATUS").
                where(__.values("created").is(P.inside(FROM, TO))).
                in("POSTED").as("u2").
                select("u1", "u2").
                groupCount().by(Column.values).next();

        System.out.println("Retweets calculated");

        // The graph of retweets
        //TinkerGraph rt = TinkerGraph.open();
        //GraphTraversalSource rt_g = rt.traversal();

        File output_vertices = new File(OUTPUT_FILE);
        File output_edges = new File(OUTPUT_FILE);

        try {
            FileWriter f0 = new FileWriter(output_vertices);
            FileWriter f1 = new FileWriter(output_edges);

            f0.write("id\n");
            f1.write("srcId;dstId;count\n");

            System.out.println("Starting encoding users");
            Map<String, Object> usernames = g.V().valueMap("screen_name").next();
            System.out.println("Done.");

            // Write the users
            for (Map.Entry<String, Object> u : usernames.entrySet()) {
                f0.write(u.getValue() + "\n");
            }

            System.out.println("Starting encoding edges");
            // The edges
            for (Map.Entry<?, Long> entry : nb_retweets.entrySet()) {
                ArrayList users = (ArrayList) entry.getKey();
                Long weight = entry.getValue();

                Vertex user1 = (Vertex) users.get(0);
                Vertex user2 = (Vertex) users.get(1);

                f1.write(user1.values("screen_name").next() + ";" +
                        user2.values("screen_name").next() + ";" + weight + "\n"
                );
                // Corresponding users
//                GraphTraversal<Vertex, Vertex> u1 = rt_g.V(user1.values("screen_name").next());
//                GraphTraversal<Vertex, Vertex> u2 = rt_g.V(user2.values("screen_name").next());
//
//
//                //If first user does not exist yet
//                if (!u1.hasNext()) {
//                    rt.addVertex(T.label, "user", T.id, user1.values("screen_name").next(),
//                            "followers_count", user1.values("followers_count").next(),
//                            "statuses_count", user1.values("statuses_count").next());
//                }
//
//                if (!u2.hasNext()) {
//                    rt.addVertex(T.label, "user", T.id, user2.values("screen_name").next(),
//                            "followers_count", user2.values("followers_count").next(),
//                            "statuses_count", user2.values("statuses_count").next());
//                }
//
//                rt_g.V(user1.values("screen_name").next()).as("u1").
//                        V(user2.values("screen_name").next()).as("u2").
//                        addE("RETWEETED_USER").property("weight", weight).
//                        from("u1").to("u2").
//                        iterate();
            }
            f0.flush();
            f1.flush();
            f0.close();
            f1.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
