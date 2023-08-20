package com.rajitha.kafka.service;

import com.rajitha.kafka.config.kafkaConstants;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ClusterService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterService.class);

    public Properties getKafkaAdminConfig(String clusterName,String clusterEnv) {
        Properties config = new Properties();

        // Add cluster-specific configuration based on clusterName and environment
        if (clusterName.equals(kafkaConstants.CLUSTER_1) && clusterEnv.equals(kafkaConstants.DEV)) {
            config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "server1:9092");
            // Add other cluster-specific properties if needed
        } else if (clusterName.equals(kafkaConstants.CLUSTER_2) && clusterEnv.equals(kafkaConstants.DEV)) {
            config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "server2:9092");
            // Add other cluster-specific properties if needed
        } else {
            throw new IllegalArgumentException("Invalid clusterName or environment");
        }


        // Add common configuration properties
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, "admin-client");
        // Add other common properties if needed

        return config;
    }
}
