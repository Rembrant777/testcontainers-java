package containers;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.SparkStandaloneContainerCluster;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Slf4j
public class SparkStandaloneContainerClusterTest {

    private static SparkStandaloneContainerCluster cluster;

    @BeforeAll
    public static void setUp() {
        try {
            log.info("#setUp start the standalone cluster ... ");
            cluster = new SparkStandaloneContainerCluster(2);
            cluster.start();
        } catch (Exception e) {
            log.error("#setUp failed!", e);
        }
    }

    @AfterAll
    public static void shutDown() {
        log.info("#shutDown stop the standalone cluster ...");
        cluster.stop();
        Assertions.assertFalse(cluster.getMaster().isRunning());
    }

    @Test
    @Order(1)
    public void checkClusterAvailable() throws Exception {
        Assertions.assertTrue(cluster.getMaster().isRunning());
        Assertions.assertTrue(cluster.getWorkers().size() == 2);

        log.info(
            "#testSparkContainerCluster master works on port {}",
            cluster.getMaster().getMappedPort(cluster.SPARK_MASTER_WEBUI_PORT)
        );
        cluster
            .getWorkers()
            .stream()
            .forEach(worker -> {
                int workerPort = worker.getMappedPort(cluster.SPARK_WORKER_PORT);
                Assertions.assertTrue(workerPort > 0);
            });
        log.info("#testSparkContainerCluster master running status {}", cluster.getMaster().isRunning());
    }

    @Test
    @Order(2)
    public void verifySparkMasterAddress() {
        Assertions.assertNotNull(cluster.getMaster());
        List<String> networkAliasList = cluster.getMaster().getNetworkAliases();
        Assertions.assertNotNull(Objects.nonNull(networkAliasList) && networkAliasList.size() > 0);
        String sparkMasterStr = cluster.getMasterAddr();
        String sparkMasterWebAddress = cluster.getMasterWebAddr();
        Assertions.assertTrue(Objects.nonNull(sparkMasterStr) && sparkMasterStr.length() > 0);
        Assertions.assertTrue(sparkMasterStr.contains("spark://"));
    }

    @Test
    @Order(3)
    public void testSparkWordCountApplication() throws InterruptedException {
        SparkConf sparkConf = new SparkConf()
            .setMaster(cluster.getMasterAddr())
            .setAppName("WordCountApp")
            .set("spark.serializer", KryoSerializer.class.getName());

        SparkSession sc = SparkSession.builder().config(sparkConf).getOrCreate();

        log.info(
            "#testSparkWordCountApplication app will be submitted to local spark standalone cluster: {}",
            cluster.getMasterWebAddr()
        );

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc.sparkContext());

        JavaRDD<String> lines = jsc.textFile("src/test/resources/input.txt");

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        List<Tuple2<String, Integer>> wcResultList = words
            .filter(word -> Objects.nonNull(word) && !word.isEmpty())
            .map(item -> item.toLowerCase())
            .mapToPair(item -> new Tuple2<>(item, 1))
            .reduceByKey(Integer::sum)
            .collect();

        Assertions.assertTrue(Objects.nonNull(wcResultList) && wcResultList.size() > 0);
        jsc.stop();
    }
}
