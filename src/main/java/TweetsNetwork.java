import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.io.FileWriter;
import java.io.File;

public class TweetsNetwork {
    public static void main(String args []) throws Exception, IOException {
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

        // List of map with id, text and retweeted_status
        List<Map<String, Object>> tweets = g.V().has("tweet", "created",
                P.inside(LocalDateTime.parse(from, formatter), LocalDateTime.parse(to, formatter))).
                project("id", "tweet", "retweeted_status", "hashtags").
                by(T.id).
                by("text").
                by(__.coalesce(__.out("RETWEETED_STATUS").values("text"), __.constant("Not a retweet"))).
                toList();

        File output = new File(OUTPUT_FILE);
        FileWriter f0 = new FileWriter(output);
        f0.write("id;tweet;retweeted_status;hashtags\n");

        for (Map<String, Object> tweet : tweets) {
            String id = tweet.get("id").toString();
            String tw = tweet.get("tweet").toString().
                    replaceAll("\n", "").replaceAll(";", "");
            String rt = tweet.get("retweeted_status").toString().
                    replaceAll("\n", "").replaceAll(";", "");
            List<Object> hashtags = g.V(Long.parseLong(id)).out("HAS_TAG").values("tag").fold().next();

            f0.write(id + ";" + tw + ";" + rt + ";" + hashtags.toString() + "\n");
        }

        f0.flush();
        f0.close();

        tg.close();
    }
}
