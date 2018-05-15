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

        Map<?, Long> nb_retweets = g.V().hasLabel("user").as("u1").
                out().out("RETWEETED_STATUS").
                in("POSTED").as("u2").
                select("u1", "u2").by("screen_name").
                groupCount().by(Column.values).next();

        TinkerGraph rt = TinkerGraph.open();
        GraphTraversalSource rt_g = rt.traversal();

        for (Map.Entry<?, Long> entry : nb_retweets.entrySet()) {
            ArrayList users = (ArrayList) entry.getKey();
            Long weight = entry.getValue();

            // Corresponding users
            GraphTraversal<Vertex, Vertex> u1 = rt_g.V().has("user", "screen_name", users.get(0));
            GraphTraversal<Vertex, Vertex> u2 = rt_g.V().has("user", "screen_name", users.get(1));

            //If first user does not exist yet
            if (!u1.hasNext()) {
                System.out.println("user " + users.get(0) + " created");
                rt_g.addV("user").property("screen_name", users.get(0)).iterate();
            }

            if (!u2.hasNext()) {
                System.out.println("user " + users.get(1) + " created");
                rt_g.addV("user").property("screen_name", users.get(1)).iterate();
            }

            System.out.println(rt_g.V().groupCount().by(T.label).toList());

            rt_g.V().has("user", "screen_name", users.get(0)).as("u1").
                    V().has("user", "screen_name", users.get(1)).as("u2").
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
