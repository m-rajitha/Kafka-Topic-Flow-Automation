package com.rajitha.kafka.service;

import com.rajitha.kafka.kafka.AdminClientFactory;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.apache.commons.lang3.StringUtils.repeat;


public class TopicService {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicService.class);
    private final AdminClientFactory adminClientFactory;
    private final String asterisks = repeat("*", 42);

    public TopicService() {
        this.adminClientFactory = new AdminClientFactory();
    }

    public void createOrUpdateTopic(Map<String, Object> topicConfig) throws ExecutionException, InterruptedException {
        // Getting topic and cluster configuration from input map topicConfig
        String topicName = (String) topicConfig.get("topicName");
        Map<String, Object> spec = (Map<String, Object>) topicConfig.get("spec");
        Map<String, Object> targetCluster = (Map<String, Object>) spec.get("target-cluster");
        String clusterName = (String) targetCluster.get("name");
        String clusterEnv = (String) targetCluster.get("env");

        LOGGER.info("Topic Name: {}", topicName);
        LOGGER.info("Cluster Name: {}", clusterName);
        LOGGER.info("Cluster Environment: {}", clusterEnv);

        AdminClient adminClient = adminClientFactory.getAdminClient(clusterName, clusterEnv);

        boolean isTopicPresent = isTopicPresent(adminClient, topicName);

        if (isTopicPresent) {
            LOGGER.info("Topic '{}' is available.", topicName);
            LOGGER.info("Lets check for any config update for the topic");
            LOGGER.info(asterisks);
            updateTopic(adminClient, topicConfig);
        } else {
            LOGGER.info(asterisks);
            LOGGER.info("Topic '{}' is not available.", topicName);
            LOGGER.info("Hence creating the topic with provided configuration");
            LOGGER.info(asterisks);
            String createTopic_result=createTopic(adminClient, topicConfig);
            LOGGER.info(createTopic_result);
        }
        closeAdminClient(adminClient);
    }

    public String createTopic(AdminClient adminClient, Map<String, Object> topicConfig) {
        // Getting topic and cluster configuration from input map topicConfig
        String topicName = (String) topicConfig.get("topicName");
        Map<String, Object> spec = (Map<String, Object>) topicConfig.get("spec");
        Map<String, Object> config = (Map<String, Object>) spec.get("config");
        int partitions = (int) spec.get("partitions");
        int replicationFactor = (int) spec.get("replication-factor");

        // Create a new HashMap to store the config section
        Map<String, String> configSection = new HashMap<>();

        // Convert the values to String and store in the config section HashMap
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            configSection.put(entry.getKey(), entry.getValue().toString());
        }

        // Print the topic configuration from the repository
        LOGGER.info("Printing the topic configuration from the repository :");
        LOGGER.info("topic name : {}",topicName);
        LOGGER.info("partitions : {}",partitions);
        LOGGER.info("replication factor : {}",replicationFactor);
        LOGGER.info("Printing HashMap using a loop:");
        for (Map.Entry<String, String> entry : configSection.entrySet()) {
            LOGGER.info("{} -> {}", entry.getKey(), entry.getValue());
        }

        try {
            NewTopic newTopic = new NewTopic(topicName, partitions, (short) replicationFactor);
            newTopic.configs(configSection);
            CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
            createTopicsResult.all().get(); // Wait for topic creation to complete
            LOGGER.info("Topic '{}' created successfully.", topicName);
            return "Topic created successfully: " + topicName;
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Error occurred while creating the topic '{}'.", topicName, e);
            return "Failed to create topic: " + topicName;
        }
    }

    public void updateTopic(AdminClient adminClient, Map<String, Object> topicConfig) {

        // Getting topic and cluster configuration from input map topicConfig
        String topicName = (String) topicConfig.get("topicName");
        Map<String, Object> fileSpec = (Map<String, Object>) topicConfig.get("spec");
        Map<String, Object> fileTopicConfig = (Map<String, Object>) fileSpec.get("config");
        int filePartitions = (int) fileSpec.get("partitions");
        int fileReplicationFactor = (int) fileSpec.get("replication-factor");

        // Create a new HashMap to store the config section
        Map<String, String> fileConfigConverted = new HashMap<>();

        // Convert the values to String and store in the config section HashMap
        for (Map.Entry<String, Object> entry : fileTopicConfig.entrySet()) {
            fileConfigConverted.put(entry.getKey(), entry.getValue().toString());
        }

        //Print the configuration from repository
        LOGGER.info("Printing the topic configuration from the repository :");
        LOGGER.info(asterisks);
        LOGGER.info("topic name : {}",topicName);
        LOGGER.info("partitions : {}",filePartitions);
        LOGGER.info("replication factor : {}",fileReplicationFactor);
        LOGGER.info("Printing HashMap using a loop:");
        for (Map.Entry<String, String> entry : fileConfigConverted.entrySet()) {
            LOGGER.info("{} -> {}", entry.getKey(), entry.getValue());
        }
        LOGGER.info(asterisks);

        // Call the getTopicDetails method
        Map<String, Object> topicDetails = getTopicDetails(adminClient, topicName);
        if (topicDetails != null) {
            // Retrieve the partition count, replication factor, and configuration
            int clusterPartitions = (int) topicDetails.get("partitions");
            int clusterReplicationFactor = (int) topicDetails.get("replicationFactor");
            Map<String, String> clusterTopicConfig = (Map<String, String>) topicDetails.get("config");

            // Print the cluster topic configuration values
            LOGGER.info("Configurations currently in cluster");
            LOGGER.info(asterisks);
            LOGGER.info("Topic: " + topicName);
            LOGGER.info("Partitions: " + clusterPartitions);
            LOGGER.info("Replication Factor: " + clusterReplicationFactor);
            LOGGER.info("Configuration:");
            for (Map.Entry<String, String> entry : clusterTopicConfig.entrySet()) {
                LOGGER.info(entry.getKey() + " -> " + entry.getValue());
            }
            LOGGER.info(asterisks);
            // Call the compareConfigurations method
            Map<String, String> differentConfigurations = compareConfigurations(fileConfigConverted, clusterTopicConfig);
            // Print the different configurations
            if (!differentConfigurations.isEmpty()){
                LOGGER.info("Configuration different from cluster and local are : ");
                for (Map.Entry<String, String> entry : differentConfigurations.entrySet()) {
                    LOGGER.info(entry.getKey() + " -> " + entry.getValue());
                }
            } else {
                LOGGER.info("No change found in the topic config");
            }

            // Check for any partition increase and update the topic partitions
            if (filePartitions != clusterPartitions) {
                if (filePartitions > clusterPartitions) {
                    // Perform the partition increase
                    increasePartitions(adminClient, topicName, filePartitions);
                    LOGGER.info("Partition count is increased in {} topic from {} to {}",topicName,clusterPartitions,filePartitions);
                } else {
                    LOGGER.error("Partition count cannot be decreased in existing topic");
                }
            } else {
                LOGGER.info("Partition count is already matching. No partition increase needed.");
            }

            if (!differentConfigurations.isEmpty()) {
                // Perform the topic configuration update
                updateTopicConfiguration(adminClient, topicName, differentConfigurations);
                LOGGER.info("Update topic configuration is successfully done");
            } else {
                LOGGER.info("Other configurations are in sync between file and cluster.");
            }

        } else {
            LOGGER.error("Failed to retrieve topic details.update topic is not done , check the errors");
        }
    }

    public boolean isTopicPresent(AdminClient adminClient, String topicName) {
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singleton(topicName));
        try {
            KafkaFuture<Map<String, TopicDescription>> futureTopicDescription = describeTopicsResult.all();
            Map<String, TopicDescription> topicDescriptionMap = futureTopicDescription.get();
            TopicDescription topicDescription = topicDescriptionMap.get(topicName);
            return topicDescription != null;
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Error occurred while checking topic presence.", e);
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                // Handle the UnknownTopicOrPartitionException here
                LOGGER.info("Handling : UnknownTopicOrPartitionException since topic is not present.");
            } else {
                // Handle other exceptions
                LOGGER.error("Error occurred while checking topic presence: " + e.getMessage());
            }
            return false;
        }
    }

    public Map<String, Object> getTopicDetails(AdminClient adminClient, String topicName) {
        Map<String, Object> topicDetails = new HashMap<>();

        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singleton(topicName));
        try {
            KafkaFuture<Map<String, TopicDescription>> futureTopicDescription = describeTopicsResult.all();
            Map<String, TopicDescription> topicDescriptionMap = futureTopicDescription.get();
            TopicDescription topicDescription = topicDescriptionMap.get(topicName);

            // Retrieve partition count and replication factor
            int partitions = topicDescription.partitions().size();
            int replicationFactor = topicDescription.partitions().get(0).replicas().size();

            // Retrieve topic configuration
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singleton(topicResource));
            Map<ConfigResource, Config> configMap = describeConfigsResult.all().get();
            Config topicConfig = configMap.get(topicResource);
            Map<String, String> config = new HashMap<>();
            for (ConfigEntry entry : topicConfig.entries()) {
                config.put(entry.name(), entry.value());
            }

            // Populate the topic details map
            topicDetails.put("partitions", partitions);
            topicDetails.put("replicationFactor", replicationFactor);
            topicDetails.put("config", config);

            return topicDetails;
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Error occurred while retrieving topic details.", e);
            return null;
        }
    }

    public Map<String, String> compareConfigurations(Map<String, String> fileTopicConfig, Map<String, String> clusterTopicConfig) {
        Map<String, String> differentConfigurations = new HashMap<>();

        for (Map.Entry<String, String> entry : fileTopicConfig.entrySet()) {
            String configKey = entry.getKey();
            String configFileValue = entry.getValue();
            String clusterConfigValue = clusterTopicConfig.get(configKey);

            if (clusterConfigValue == null || !clusterConfigValue.equals(configFileValue)) {
                differentConfigurations.put(configKey, configFileValue);
            }
        }

        return differentConfigurations;
    }
    public void increasePartitions(AdminClient adminClient, String topicName, int newPartitionCount) {
        NewPartitions newPartitions = NewPartitions.increaseTo(newPartitionCount);
        Map<String, NewPartitions> topicPartitions = Collections.singletonMap(topicName, newPartitions);

        try {
            CreatePartitionsResult result = adminClient.createPartitions(topicPartitions);
            result.values().get(topicName).get();
            LOGGER.info("Partition count increased to " + newPartitionCount + " successfully.");
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Failed to increase partition count: " + e.getMessage());
        }
    }

    public void updateTopicConfiguration(AdminClient adminClient, String topicName, Map<String, String> configToUpdate) {
        ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        ConfigEntry newConfigEntry;
        List<AlterConfigOp> configOps = new ArrayList<>();

        for (Map.Entry<String, String> entry : configToUpdate.entrySet()) {
            String configKey = entry.getKey();
            String configValue = entry.getValue();
            newConfigEntry = new ConfigEntry(configKey, configValue);
            configOps.add(new AlterConfigOp(newConfigEntry, AlterConfigOp.OpType.SET));
        }

        AlterConfigsResult alterConfigsResult = adminClient.incrementalAlterConfigs(Collections.singletonMap(topicResource, configOps));
        try {
            alterConfigsResult.all().get();
            LOGGER.info("Topic configuration updated successfully.");
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Failed to update topic configuration: " + e.getMessage());
        }
    }

    public void closeAdminClient(AdminClient adminClient) {
        adminClient.close();
        LOGGER.info("AdminClient closed successfully.");
    }

}
