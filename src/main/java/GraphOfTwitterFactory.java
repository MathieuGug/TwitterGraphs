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

import java.util.Date;
import java.io.IOException;

public class GraphOfTwitterFactory {
    public static void main(String[] args) throws IOException {
        String path = args[0];

        final JanusGraph graph = JanusGraphFactory.build()
                .set("storage.backend", "hbase")
                .set("storage.hostname", "10.16.6.21,10.16.6.22,10.16.6.23,10.16.6.24")
                .set("schema.default", "none")
                .set("storage.username", "tcolloca")
                .set("storage.password", "tcolloca")
                .set("index.search.backend", "elasticsearch")
                .set("index.search.hostname", "elasticsearch")
                .open();

        buildSchema(graph);
    }

    private static void buildSchema(final JanusGraph graph) {
        JanusGraphManagement mgmt = graph.openManagement();

        /* USER */
        addVertexLabel(mgmt, "user");

        PropertyKey id = addPropertyKey(mgmt, "id", Long.class, Cardinality.SINGLE);
        PropertyKey user_key = addPropertyKey(mgmt, "user_key", String.class, Cardinality.SINGLE);
        PropertyKey screen_name = addPropertyKey(mgmt, "screen_name", String.class, Cardinality.SINGLE);
        PropertyKey created_at = addPropertyKey(mgmt,"created_at", Date.class, Cardinality.SINGLE);
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

        mgmt.buildIndex("byDate", Vertex.class).addKey(created_at).buildMixedIndex("search");
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

        addVertexLabel(mgmt, "person");
        addVertexLabel(mgmt, "organization");
        addVertexLabel(mgmt, "location");

        addVertexLabel(mgmt, "hashtag");
        PropertyKey tag = addPropertyKey(mgmt,"tag", String.class, Cardinality.SINGLE);
        mgmt.buildIndex("byHashtag", Vertex.class).addKey(tag).buildMixedIndex("search");

        addVertexLabel(mgmt, "url");
        PropertyKey expanded_url = addPropertyKey(mgmt,"expanded_url", String.class, Cardinality.SINGLE);
        PropertyKey domain = addPropertyKey(mgmt,"domain", String.class, Cardinality.SINGLE);

        mgmt.buildIndex("byExpandedUrl", Vertex.class).addKey(expanded_url).buildMixedIndex("search");
        mgmt.buildIndex("byDomain", Vertex.class).addKey(domain).buildCompositeIndex();

        addVertexLabel(mgmt, "source");
        addPropertyKey(mgmt,"name", String.class, Cardinality.SINGLE);

        /* Edges */
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
}
