package org.testcontainers.containers;

import org.testcontainers.utility.DockerImageName;

/**
 * Spark Worker Container
 */
public class SparkWorkerContainer extends GenericContainer<SparkWorkerContainer> {
    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("bde2020/spark-master:3.3.0-hadoop3.3");

    private static final String DEFAULT_TAG = "";

    public static final int SPARK_WORKER_PORT = 8081;


    public SparkWorkerContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

        // set context env variables here

        // expose ports in spark standalone mode

        withExposedPorts(SPARK_WORKER_PORT);

        withCreateContainerCmdModifier(cmd -> {
            cmd.withEntrypoint("sh");
        });

        // here implement spark setup shell commands and dump commands to the scripts with already declared script name
        // withCommand();
    }
}
