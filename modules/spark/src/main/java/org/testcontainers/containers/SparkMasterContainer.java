package org.testcontainers.containers;

import org.testcontainers.utility.DockerImageName;

/**
 * Spark Master Container
 */
public class SparkMasterContainer extends GenericContainer<SparkMasterContainer> {
    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("");

    private static final String DEFAULT_TAG = "3.3.0-hadoop3.3";

    public static final int SPARK_WEB_PORT = 8080;

    public static final int SPARK_MASTER_PORT = 7077;



    public SparkMasterContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

        // set context env variables here

        // expose ports in spark standalone mode

        withExposedPorts(SPARK_WEB_PORT, SPARK_MASTER_PORT);

        withCreateContainerCmdModifier(cmd -> {
            cmd.withEntrypoint("sh");
        });

        // here implement spark setup shell commands and dump commands to the scripts with already declared script name
        // withCommand();
    }
}
