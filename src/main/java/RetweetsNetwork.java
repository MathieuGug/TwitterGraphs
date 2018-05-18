import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.GremlinDslProcessor;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;

import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.*;
//import org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

public class RetweetsNetwork {
    public static void main(String args []) {
        // Usage : --input <input_file> --output <output_file>
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

        // A map [v1, v2] : nb_retweets
        Map<?, Long> nb_retweets = g.V().hasLabel("user").as("u1").
                out().out("RETWEETED_STATUS").
                in("POSTED").as("u2").
                select("u1", "u2").//by("screen_name").
                groupCount().by(Column.values).next();

        TinkerGraph rt = TinkerGraph.open();
        GraphTraversalSource rt_g = rt.traversal();

        for (Map.Entry<?, Long> entry : nb_retweets.entrySet()) {
            ArrayList users = (ArrayList) entry.getKey();
            Long weight = entry.getValue();

            Vertex user1 = (Vertex) users.get(0);
            Vertex user2 = (Vertex) users.get(1);

            // Corresponding users
            GraphTraversal<Vertex, Vertex> u1 = rt_g.V(user1.value("screen_name"));
            GraphTraversal<Vertex, Vertex> u2 = rt_g.V(user2.value("screen_name"));

            //If first user does not exist yet
            if (!u1.hasNext()) {
                //System.out.println("user " + users.get(0) + " created");
                rt.addVertex(T.label, "user", T.id, user1.values("screen_name").next(),
                        "followers_count", user1.values("followers_count").next(),
                        "statuses_count", user1.values("statuses_count").next());
            }

            if (!u2.hasNext()) {
                //System.out.println("user " + users.get(1) + " created");
                //System.out.println(user2.value("followers_count"));
                rt.addVertex(T.label, "user", T.id, user2.values("screen_name").next(),
                        "followers_count", user2.values("followers_count").next(),
                        "statuses_count", user2.values("statuses_count").next());
            }

            rt_g.V(user1.value("screen_name")).as("u1").
                    V(user2.value("screen_name")).as("u2").
                    addE("RETWEETED_USER").property("weight", weight).
                    from("u1").to("u2").
                    iterate();
        }

        try {
            rt.io(IoCore.graphml()).writeGraph(OUTPUT_FILE);
        } catch (IOException e) {
            System.out.println("File not found");
            System.exit(1);
        }
    }

}
