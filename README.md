# Kafka-Topic-Flow-Automation
KafkaTopicAutoManager is a Java-based automation tool designed to streamline and simplify the management of Kafka topics. This project aims to empower developers, operations teams, and Kafka administrators by automating the otherwise manual processes of creating, updating, and synchronizing Kafka topics.

```markdown

Automate the creation and updating of Kafka topics based on changes in a Bitbucket repository's pull request.

## Description

This Java project aims to automate the management of Kafka topics using the Kafka Admin Client API. It monitors changes in a Bitbucket repository's pull request and creates or updates Kafka topics accordingly. The automation includes cloning/synchronizing the repository, analyzing pull request changes, and applying appropriate actions to the Kafka topics.

## Features

- Cloning and synchronization of a Git repository from a remote URL.
- Analysis of pull request changes to identify modified or added files.
- Creation or updating of Kafka topics based on configuration changes.
- Comparison of configuration settings and application of necessary updates.
- Handling partition increase in Kafka topics.
- Interaction with the Bitbucket API to fetch pull request details.

## Prerequisites

- Java Development Kit (JDK)
- Apache Maven
- Git

## Getting Started

1. Clone the repository to your local machine:

   ```bash
   git clone https://github.com/your-username/your-repo.git
   ```

2. Navigate to the cloned repository directory:

   ```bash
   cd your-repo
   ```

3. Edit the `config.properties` file with your Bitbucket and Kafka configuration details.

4. Build the project using Maven:

   ```bash
   mvn clean install
   ```

5. Run the main Java application:

   ```bash
   java -jar target/your-jar-file.jar config.properties username password commit-id
   ```

## Configuration

The `config.properties` file contains configuration details required for the project:

- `repoPath`: Remote URL of the Git repository to clone/synchronize.
- `targetDir`: Local directory to clone the repository.
- `branch`: Repository branch to work with.

## Usage

Run the Java application with the necessary arguments:

- `configPath`: Path to the configuration properties file.
- `username`: Bitbucket username for authentication.
- `password`: Bitbucket password for authentication.
- `commitId`: Commit ID associated with the pull request.

Example:

```bash
java -jar target/your-jar-file.jar config.properties your-username your-password your-commit-id
```

## Contributing

Contributions are welcome! Feel free to open issues and pull requests.
