import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static org.janusgraph.diskstorage.es.ElasticSearchIndex.*;

import java.util.Date;
import java.io.IOException;

public class GraphOfTwitterFactory {
    private static final String INDEX_NAME = "search";
    private static final String ERR_NO_INDEXING_BACKEND =
            "The indexing backend with name \"%s\" is not defined. Specify an existing indexing backend or " +
                    "use GraphOfTheGodsFactory.loadWithoutMixedIndex(graph,true) to load without the use of an " +
                    "indexing backend.";

    public static void loadWithoutMixedIndex(final JanusGraph graph, boolean uniqueNameCompositeIndex) {
        load(graph, null, uniqueNameCompositeIndex);
    }

    private static void load(final JanusGraph graph) {
        load(graph, INDEX_NAME, true);
    }

    private static boolean mixedIndexNullOrExists(StandardJanusGraph graph, String indexName) {
        return indexName == null || graph.getIndexSerializer().containsIndex(indexName);
    }

    private static void load(final JanusGraph graph, String mixedIndexName, boolean uniqueNameCompositeIndex) {
        JanusGraphManagement mgmt = graph.openManagement();

        /* USER */
        addVertexLabel(mgmt, "user");

        PropertyKey id = addPropertyKey(mgmt, "id", Long.class, Cardinality.SINGLE);
        PropertyKey user_key = addPropertyKey(mgmt, "user_key", String.class, Cardinality.SINGLE);
        PropertyKey screen_name = addPropertyKey(mgmt, "screen_name", String.class, Cardinality.SINGLE);
        PropertyKey created = addPropertyKey(mgmt,"created", Date.class, Cardinality.SINGLE);
        PropertyKey favourites_count = addPropertyKey(mgmt,"favourites_count", Integer.class, Cardinality.SINGLE);
        PropertyKey followers_count = addPropertyKey(mgmt,"followers_count", Integer.class, Cardinality.SINGLE);
        PropertyKey listed_count = addPropertyKey(mgmt,"listed_count", Integer.class, Cardinality.SINGLE);
        PropertyKey statuses_count = addPropertyKey(mgmt,"statuses_count", Integer.class, Cardinality.SINGLE);
        PropertyKey lang = addPropertyKey(mgmt,"lang", String.class, Cardinality.SINGLE);
        PropertyKey time_zone = addPropertyKey(mgmt,"time_zone", String.class, Cardinality.SINGLE);
        PropertyKey verified = addPropertyKey(mgmt,"verified", Boolean.class, Cardinality.SINGLE);
        PropertyKey location = addPropertyKey(mgmt,"location", String.class, Cardinality.SINGLE);

        mgmt.buildIndex("byIdUnique", Vertex.class).addKey(id).unique().buildCompositeIndex();
        mgmt.buildIndex("byScreenNameUnique", Vertex.class).addKey(screen_name).unique().buildCompositeIndex();
        mgmt.buildIndex("byLocation", Vertex.class).addKey(location).buildCompositeIndex();

        mgmt.buildIndex("byDate", Vertex.class).addKey(created).buildMixedIndex("search");
        mgmt.buildIndex("byFollowersCount", Vertex.class).addKey(followers_count).buildMixedIndex("search");
        mgmt.buildIndex("byFavouritesCount", Vertex.class).addKey(favourites_count).buildMixedIndex("search");
        mgmt.buildIndex("byStatusesCount", Vertex.class).addKey(statuses_count).buildMixedIndex("search");

        /* TWEET */
        addVertexLabel(mgmt, "tweet");
        PropertyKey text = addPropertyKey(mgmt,"text", String.class, Cardinality.SINGLE);
        PropertyKey retweets_count = addPropertyKey(mgmt,"retweets_count", Integer.class, Cardinality.SINGLE);
        PropertyKey followers_at_time = addPropertyKey(mgmt,"followers_at_time", Integer.class, Cardinality.SINGLE);
        PropertyKey friends_at_time = addPropertyKey(mgmt,"friends_at_time", Integer.class, Cardinality.SINGLE);
        PropertyKey statuses_at_time = addPropertyKey(mgmt,"statuses_at_time", Integer.class, Cardinality.SINGLE);
        PropertyKey listed_at_time = addPropertyKey(mgmt,"listed_at_time", Integer.class, Cardinality.SINGLE);

        mgmt.buildIndex("byRetweetsCount", Vertex.class).addKey(retweets_count).buildMixedIndex("search");
        mgmt.buildIndex("byFollowersAtTime", Vertex.class).addKey(followers_at_time).buildMixedIndex("search");
        mgmt.buildIndex("byFriendsAtTime", Vertex.class).addKey(friends_at_time).buildMixedIndex("search");
        mgmt.buildIndex("byStatusesAtTime", Vertex.class).addKey(statuses_at_time).buildMixedIndex("search");
        mgmt.buildIndex("byListedAtTime", Vertex.class).addKey(listed_at_time).buildMixedIndex("search");

        /* PERSON, ORGANIZATION, LOCATION */
        addVertexLabel(mgmt, "person");
        addVertexLabel(mgmt, "organization");
        addVertexLabel(mgmt, "location");

        /* HASHTAG */
        addVertexLabel(mgmt, "hashtag");
        PropertyKey tag = addPropertyKey(mgmt,"tag", String.class, Cardinality.SINGLE);
        mgmt.buildIndex("byHashtag", Vertex.class).addKey(tag).buildMixedIndex("search");

        /* URL */
        addVertexLabel(mgmt, "url");
        PropertyKey expanded_url = addPropertyKey(mgmt,"expanded_url", String.class, Cardinality.SINGLE);
        PropertyKey domain = addPropertyKey(mgmt,"domain", String.class, Cardinality.SINGLE);

        mgmt.buildIndex("byExpandedUrl", Vertex.class).addKey(expanded_url).buildMixedIndex("search");
        mgmt.buildIndex("byDomain", Vertex.class).addKey(domain).buildCompositeIndex();

        /* SOURCE */
        addVertexLabel(mgmt, "source");
        addPropertyKey(mgmt,"name", String.class, Cardinality.SINGLE);

        /* EDGES */
        addEdgeLabel(mgmt, "RETWEETED_STATUS");
        addEdgeLabel(mgmt, "QUOTED_STATUS");
        addEdgeLabel(mgmt, "IN_REPLY_TO");
        addEdgeLabel(mgmt, "HAS_LINK");
        addEdgeLabel(mgmt, "MENTIONS");
        addEdgeLabel(mgmt, "HAS_TAG");
        addEdgeLabel(mgmt, "POSTED");
        addEdgeLabel(mgmt, "POSTED_VIA");

        mgmt.commit();
    }

    private static void addVertexLabel(JanusGraphManagement mgmt, String label) {
        if (mgmt.getVertexLabel(label) != null)
            return;

        mgmt.makeVertexLabel(label).make();
    }

    private static void addEdgeLabel(JanusGraphManagement mgmt, String label) {
        if (mgmt.getEdgeLabel(label) != null)
            return;

        mgmt.makeEdgeLabel(label).make();
    }

    private static PropertyKey addPropertyKey(JanusGraphManagement mgmt, String label, Class dataType, Cardinality cardinality) {
        if (mgmt.getPropertyKey(label) != null)
            return null;

        return mgmt.makePropertyKey(label).dataType(dataType).cardinality(cardinality).make();
    }
    public static JanusGraph create(String CONFIG_PATH) {
        //final JanusGraphFactory.Builder config = JanusGraphFactory.open(CONFIG_PATH);
//                build();
//        config.set("storage.backend", "hbase");
//        config.set("storage.hbase.table", "janusgraph_tweets2");
//        config.set("index." + INDEX_NAME + ".backend", "elasticsearch");

        //JanusGraph graph = config.open();
        JanusGraph graph = JanusGraphFactory.open(CONFIG_PATH);
        GraphOfTwitterFactory.load(graph);
        return graph;
    }

    public static void main(String[] args) throws IOException {
        if (null == args || 1 != args.length) {
            System.out.println(args[0]);
            System.err.println("Usage: GraphOfTwitterFactory <janusgraph-config-file>");
            System.exit(1);
        }

        JanusGraph graph = create(args[0]);
        graph.close();
    }
}
