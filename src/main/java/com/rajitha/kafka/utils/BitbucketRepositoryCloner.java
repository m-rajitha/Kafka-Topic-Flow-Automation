package com.rajitha.kafka.utils;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.PullCommand;
import org.eclipse.jgit.api.ResetCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class BitbucketRepositoryCloner {
    private static final Logger LOGGER = LoggerFactory.getLogger(BitbucketRepositoryCloner.class);
    private final String repoPath;
    private final String targetDir;
    private final String branch;
    private final CredentialsProvider credentialsProvider;

    public BitbucketRepositoryCloner(String configFile, String username, String password) throws IOException {
        Properties properties = new Properties();
        FileInputStream inputStream = new FileInputStream(configFile);
        properties.load(inputStream);

        repoPath = properties.getProperty("repoPath");
        targetDir = properties.getProperty("targetDir");
        branch = properties.getProperty("branch");

        credentialsProvider = new UsernamePasswordCredentialsProvider(username, password);
    }

    public void cloneOrSyncRepository() throws GitAPIException {
        LOGGER.info("Starting the Repository cloning process");
        Git git;

        try {
            git = Git.open(new File(targetDir));
        } catch (IOException e) {
            // Clone the repository if it doesn't exist locally
            git = Git.cloneRepository()
                    .setURI(repoPath)
                    .setDirectory(new File(targetDir))
                    .setCredentialsProvider(credentialsProvider)
                    .setBranch(branch)
                    .call();
        }

        // Reset local changes and discard them
        ResetCommand resetCommand = git.reset();
        resetCommand.setMode(ResetCommand.ResetType.HARD);
        resetCommand.call();

        // Pull changes from the remote repository
        PullCommand pullCommand = git.pull();
        pullCommand.setCredentialsProvider(credentialsProvider);
        pullCommand.call();

        LOGGER.info("Repository successfully cloned or synced.");
    }
}
