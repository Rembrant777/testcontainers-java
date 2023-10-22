package org.testcontainers.containers;

import lombok.extern.slf4j.Slf4j;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class SparkStandaloneContainerCluster implements Startable {

    private final int workersNum;

    private final Network network;

    private final String SPARK_MASTER_IMAGE = "bde2020/spark-master:3.3.0-hadoop3.3";

    private final String SPARK_WORKER_IMAGE = "bde2020/spark-worker:3.3.0-hadoop3.3";

    private final Collection<GenericContainer> workers;

    private final GenericContainer<?> master;

    public final int SPARK_WORKER_PORT = 8091;

    public final int SPARK_MASTER_PORT = 7070;

    public final int SPARK_MASTER_WEBUI_PORT = 8080;

    public SparkStandaloneContainerCluster(int workersNum) {
        log.info(
            "#SPARK init container cluster with spark master image {}, spark worker image {}," + " workerNum {}",
            this.SPARK_MASTER_IMAGE,
            this.SPARK_WORKER_IMAGE,
            workersNum
        );

        if (workersNum < 0) {
            throw new IllegalArgumentException("workersNum ' " + workersNum + "' must be greater than 0");
        }

        this.workersNum = workersNum;
        this.network = Network.newNetwork();

        this.master =
            new GenericContainer<>(DockerImageName.parse(SPARK_MASTER_IMAGE))
                .withNetwork(this.network)
                .withNetworkAliases("spark-master")
                .withEnv("SPARK_MASTER_PORT", String.valueOf(SPARK_MASTER_PORT))
                .withEnv("SPARK_MASTER_WEBUI_PORT", String.valueOf(SPARK_MASTER_WEBUI_PORT))
                .withExposedPorts(SPARK_MASTER_PORT, SPARK_MASTER_WEBUI_PORT);

        this.workers =
            IntStream
                .range(0, this.workersNum)
                .mapToObj(workerId -> {
                    return new GenericContainer<>(DockerImageName.parse(SPARK_WORKER_IMAGE))
                        .withNetwork(this.network)
                        .withNetworkAliases("spark-worker-" + workerId)
                        .withExposedPorts(SPARK_WORKER_PORT)
                        .dependsOn(this.master)
                        .withEnv("SPARK_WORKER_PORT", String.valueOf(SPARK_WORKER_PORT))
                        .withStartupTimeout(Duration.ofMinutes(1));
                })
                .collect(Collectors.toSet());
    }

    public Collection<GenericContainer> getWorkers() {
        return this.workers;
    }

    public GenericContainer<?> getMaster() {
        return this.master;
    }

    @Override
    public void start() {
        // sequential setup to avoid resource allocation error caused cluster setup fail
        this.master.start();
        this.workers.stream().forEach(GenericContainer::start);

        Unreliables.retryUntilTrue(
            30,
            TimeUnit.SECONDS,
            () -> {
                return this.master.isRunning();
            }
        );
    }

    @Override
    public void stop() {
        this.workers.stream().forEach(GenericContainer::stop);
        this.master.stop();
    }

    @Override
    public void close() {
        Startable.super.close();
    }
}
