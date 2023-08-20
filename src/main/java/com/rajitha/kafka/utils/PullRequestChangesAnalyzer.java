package com.rajitha.kafka.utils;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class PullRequestChangesAnalyzer {
    private final String baseUrl;
    private final String username;
    private final String password;
    private final String projectKey;
    private final String repositorySlug;
    private static final Logger LOGGER = LoggerFactory.getLogger(PullRequestChangesAnalyzer.class);

    public PullRequestChangesAnalyzer(String baseUrl,  String username, String password, String projectKey, String repositorySlug) {
        this.baseUrl = baseUrl;
        this.username = username;
        this.password = password;
        this.projectKey = projectKey;
        this.repositorySlug = repositorySlug;
    }

    public Integer getPullRequestId(String commitId) {
        try {
            String url = String.format("%s/rest/api/1.0/projects/%s/repos/%s/commits/%s/pull-requests", baseUrl, projectKey, repositorySlug, commitId);
            HttpURLConnection conn = createConnection(url);

            if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
                reader.close();

                JSONObject jsonResponse = new JSONObject(response.toString());
                JSONArray values = jsonResponse.getJSONArray("values");
                if (values.length() > 0) {
                    JSONObject pullRequest = values.getJSONObject(0);
                    return pullRequest.getInt("id");
                }
            } else {
                LOGGER.error("Failed to get Pull Request ID. HTTP response code: " + conn.getResponseCode());
            }

            conn.disconnect();
        } catch (IOException e) {
            LOGGER.error("Error while fetching Pull Request ID: " + e.getMessage());
        }
        return null;
    }

    public Map<String, String> getPullRequestChanges(Integer pullRequestId) {
        Map<String, String> filePathAndChangeTypeMap = new HashMap<>();
        String apiUrl = baseUrl + "/rest/api/1.0/projects/" + projectKey + "/repos/" + repositorySlug
                + "/pull-requests/" + pullRequestId + "/changes";

        try {
            HttpURLConnection conn = createConnection(apiUrl);
            if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
                reader.close();

                JSONObject jsonObject = new JSONObject(response.toString());
                JSONArray values = jsonObject.getJSONArray("values");
                for (int i = 0; i < values.length(); i++) {
                    JSONObject changeObject = values.getJSONObject(i);
                    String gitChangeType = changeObject.getJSONObject("properties").getString("gitChangeType");

                    // Check for "MODIFY" or "ADD" gitChangeType
                    if ("MODIFY".equals(gitChangeType) || "ADD".equals(gitChangeType)) {
                        JSONObject pathObject = changeObject.getJSONObject("path");
                        String filePath = pathObject.getString("toString");
                        filePathAndChangeTypeMap.put(filePath, gitChangeType);
                    }
                }
            } else {
                LOGGER.error("Failed to get Pull Request Changes. HTTP response code: " + conn.getResponseCode());
            }

            conn.disconnect();
        } catch (IOException e) {
            LOGGER.error("Error while fetching Pull Request Changes: " + e.getMessage());
        }

        return filePathAndChangeTypeMap;
    }

    private HttpURLConnection createConnection(String url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Authorization", "Basic " + getEncodedCredentials());
        return conn;
    }

    private String getEncodedCredentials() {
        String userCredentials = username + ":" + password;
        return java.util.Base64.getEncoder().encodeToString(userCredentials.getBytes(StandardCharsets.UTF_8));
    }
}
