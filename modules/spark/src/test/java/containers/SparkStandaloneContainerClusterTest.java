package containers;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.SparkStandaloneContainerCluster;

public class SparkStandaloneContainerClusterTest {

    @Test
    public void testSparkContainerCluster() throws Exception {
        try (SparkStandaloneContainerCluster cluster = new SparkStandaloneContainerCluster(2)) {
            cluster.start();

            Assertions.assertTrue(cluster.getMaster().isRunning());
            Assertions.assertTrue(cluster.getWorkers().size() == 2);
            cluster
                .getWorkers()
                .stream()
                .forEach(worker -> {
                    int workerPort = worker.getMappedPort(cluster.SPARK_WORKER_PORT);
                    Assertions.assertTrue(workerPort > 0);
                });
        }
    }
    // try to commit a wordcount app to the cluster
}
