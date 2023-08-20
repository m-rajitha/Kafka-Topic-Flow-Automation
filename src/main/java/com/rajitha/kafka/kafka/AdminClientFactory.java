package com.rajitha.kafka.kafka;

import com.rajitha.kafka.service.ClusterService;
import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AdminClientFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdminClientFactory.class);
    private ClusterService ClusterService = new ClusterService();
    public AdminClient getAdminClient(String clusterName,String clusterEnv){
        Properties config = ClusterService.getKafkaAdminConfig(clusterName,clusterEnv);
        AdminClient adminClient = AdminClient.create(config);
        LOGGER.info("Successfully created admin client ");
        return adminClient;
    }
}
