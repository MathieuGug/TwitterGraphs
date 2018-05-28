import org.apache.commons.configuration.Configuration;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
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

import java.io.File;
import java.io.FileWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.io.IOException;

public class HashtagsFrequencies {
    public static void main(String args[]) throws Exception {
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
        LocalDateTime FROM = LocalDateTime.parse(from, formatter);
        LocalDateTime TO = LocalDateTime.parse(to, formatter);
        Graph subg = (Graph) g.V().has("tweet", "created",
                P.inside(FROM, TO)).
                //P.lt(LocalDateTime.parse(to, formatter))).
                        bothE().subgraph("subGraph").cap("subGraph").next();

        List<Map<String, Object>> hashtags = g.V().has("tweet", "created",
                P.inside(FROM, TO)).
                as("date").
                out("HAS_TAG").as("tag").
                select("date", "tag").
                by("created").by("tag").toList();

        File output = new File(OUTPUT_FILE);
        FileWriter f0 = new FileWriter(output);
        f0.write("hashtag;date\n");

        for (Map<String, Object> hashtag : hashtags) {
            String tag = hashtag.get("tag").toString();
            String date = hashtag.get("date").toString();
            f0.write(tag + ";" + date + "\n");
        }

        f0.flush();
        f0.close();
        tg.close();
    }
}
