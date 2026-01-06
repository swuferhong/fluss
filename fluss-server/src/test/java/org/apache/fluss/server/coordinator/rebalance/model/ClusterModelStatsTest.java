package org.apache.fluss.server.coordinator.rebalance.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.SortedSet;
import java.util.TreeSet;

/** Tests for the {@link ClusterModelStats}. */
public class ClusterModelStatsTest {
    private SortedSet<ServerModel> servers;

    @BeforeEach
    public void setup() {
        servers = new TreeSet<>();
        ServerModel server0 = new ServerModel(0, "rack0", true);
        ServerModel server1 = new ServerModel(1, "rack1", true);
        servers.add(server0);
        servers.add(server1);
    }

    @Test
    void testPopulate() throws Exception {
        // TODO add test for this method, trace by https://github.com/apache/fluss/issues/2315
    }
}
