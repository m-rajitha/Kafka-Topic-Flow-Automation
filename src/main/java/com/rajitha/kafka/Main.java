package com.rajitha.kafka;

import com.rajitha.kafka.service.TopicService;
import com.rajitha.kafka.utils.BitbucketRepositoryCloner;
import com.rajitha.kafka.config.kafkaConstants;
import com.rajitha.kafka.utils.PullRequestChangesAnalyzer;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import static org.apache.commons.lang3.StringUtils.repeat;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private final TopicService topicService;
    private final String asterisks = repeat("*", 42);
    private final String equalseparater = repeat("=", 42);

    public Main(TopicService topicService) {
        this.topicService = topicService;
    }

    public void run(String[] args) {
        if (args.length < 4) {
            System.err.println("Insufficient number of arguments provided.");
            System.err.println("Usage: java -jar your-jar-file.jar <config-path> <username> <password> <topic-file-names> <topic-env>");
            System.exit(1);
        }

        LOGGER.info("Application started successfully");

        // Reading the args
        String configFile = args[0];
        String username = args[1];
        String password = args[2];
        String commitId = args[3];

        LOGGER.info("Configuration path given in application is: {}", configFile);

        // Creating variables and assigning values
        String baseUrl = kafkaConstants.BITBUCKET_BASE_URL;
        String projectKey = kafkaConstants.PROJECT_KEY;
        String repositorySlug = kafkaConstants.REPOSITORY_SLUG;
        String[] topicFileList = null; // Initialize the variable with null

        PullRequestChangesAnalyzer analyzer = new PullRequestChangesAnalyzer(baseUrl, username, password, projectKey, repositorySlug);

        // Get the Pull Request ID for the specified commit
        Integer pullRequestId = analyzer.getPullRequestId(commitId);
        if (pullRequestId != null) {
            LOGGER.info("Pull Request ID: {} for the Commit ID {}", pullRequestId, commitId);
            // Get the changes in the Pull Request
            Map<String, String> fileChanges = analyzer.getPullRequestChanges(pullRequestId);
            if (fileChanges != null) {
                // Taking the fileNamePath alone in Array of String
                topicFileList = fileChanges.keySet().toArray(new String[0]);
                // Print the files Changed on the pull request
                LOGGER.info(asterisks);
                LOGGER.info("Files created / modified on pull request {} .",pullRequestId);
                LOGGER.info(equalseparater);
                for (Map.Entry<String, String> entry : fileChanges.entrySet()) {
                    LOGGER.info("Topic File : " + entry.getKey() + ", File change type: " + entry.getValue());
                }
                LOGGER.info(asterisks);
            } else {
                LOGGER.error("No file changes found for the specified Pull Request ID.");
            }
        } else {
            LOGGER.error("Pull Request ID not found for the specified commit.");
        }

        // Check if topicFileList is null or empty
        if (topicFileList == null || topicFileList.length == 0) {
            LOGGER.error("No topic files found for processing.");
            System.exit(1);
        }

        // Cloning the repository
        try {
            BitbucketRepositoryCloner cloner = new BitbucketRepositoryCloner(configFile, username, password);
            cloner.cloneOrSyncRepository();
        } catch (IOException | GitAPIException e) {
            LOGGER.error("Error cloning or syncing the repository", e);
            System.exit(1); // Terminate the program with a non-zero exit code to indicate failure
        }

        for (String topicFileName : topicFileList) {
            LOGGER.info(asterisks+asterisks);
            LOGGER.info("Given topic file for creation or update is: {} ", topicFileName);
            // Calling the topic service with respected topic yaml
            Properties config = loadConfigProperties(configFile);
            String targetDir = config.getProperty("targetDir");
            String filePath = generateFilePath(targetDir, topicFileName);

            try (InputStream inputStream = new FileInputStream(filePath)) {
                // Load the YAML file
                Yaml yaml = new Yaml();
                Map<String, Object> data = yaml.load(inputStream);
                topicService.createOrUpdateTopic(data);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static Properties loadConfigProperties(String fileName) {
        Properties properties = new Properties();
        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(fileName);
            properties.load(inputStream);
        } catch (FileNotFoundException e) {
            throw new RuntimeException();
        } catch (IOException e) {
            throw new RuntimeException();
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return properties;
    }

    private static String generateFilePath(String targetDirectory, String topicFileName) {
        File file = new File(targetDirectory, topicFileName);
        return file.getAbsolutePath();
    }

    public static void main(String[] args) {
        Main main = new Main(new TopicService());
        main.run(args);
        LOGGER.info("Application finished successfully.");
    }
}
