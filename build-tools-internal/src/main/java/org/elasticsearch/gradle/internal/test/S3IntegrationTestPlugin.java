/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.PropertyNormalization;
import org.elasticsearch.gradle.internal.info.BuildParams;
import org.elasticsearch.gradle.internal.testfixtures.TestFixtureExtension;
import org.elasticsearch.gradle.internal.testfixtures.TestFixturesPlugin;
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.tasks.testing.Test;

import java.util.Locale;

public class S3IntegrationTestPlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        boolean useFixture = false;
        String accessKey = System.getenv("amazon_s3_access_key");
        String secretKey = System.getenv("amazon_s3_secret_key");
        String bucket = System.getenv("amazon_s3_bucket");
        String basePath = System.getenv("amazon_s3_base_path");

        if (accessKey == null && secretKey == null && bucket == null && basePath == null) {
            accessKey = "s3_test_access_key";
            secretKey = "s3_test_secret_key";
            bucket = "bucket";
            basePath = null;
            useFixture = true;
        } else if (accessKey == null || secretKey == null || bucket == null || basePath == null) {
            throw new IllegalArgumentException("not all options specified to run against external S3 service are present");
        }

        if (useFixture) {
            project.getPluginManager().apply(TestFixturesPlugin.class);
        }

        project.getExtensions().create("s3Fixture", S3Extension.class, project, useFixture, accessKey, secretKey, bucket, basePath);
    }

    public static class S3Extension {
        private final Project project;
        private final boolean useFixture;
        private final String accessKey;
        private final String secretKey;
        private final String bucket;
        private final String basePath;

        public S3Extension(Project project,
                           boolean useFixture,
                           String accessKey,
                           String secretKey,
                           String bucket,
                           String basePath) {
            this.project = project;
            this.useFixture = useFixture;
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.bucket = bucket;
            this.basePath = basePath;
        }

        public void configure(String fixtureName, String s3ClientName, String testClusterType) {
            Project fixture = project.project(":test:fixtures:s3-fixture");
            Project repositoryPlugin = project.project(":plugins:repository-s3");

            System.out.println("Use fixture " + useFixture);
            System.out.println("Access key " + accessKey);
            System.out.println("Secret key " + secretKey);
            System.out.println("Bucket " + bucket);
            System.out.println("base path " + basePath);

            if (useFixture) {
                TestFixtureExtension testFixtures = (TestFixtureExtension) project.getExtensions().getByName("testFixtures");
                testFixtures.useFixture(fixture.getPath(), fixtureName);
            }

            final String basePath =
                this.basePath == null ? "base_path" : this.basePath + "_" + s3ClientName + "_tests" + BuildParams.getTestSeed();
            project.getTasks().withType(Test.class).configureEach(test -> {
                test.systemProperty("test.s3.bucket", bucket);
                test.systemProperty("test.s3.base_path", basePath);
            });

            testClusters(project, "testClusters")
                .matching(s -> s.getName().equals(testClusterType))
                .configureEach(cluster -> {
                    cluster.plugin(repositoryPlugin.getPath());

                    cluster.keystore(format("s3.client.%s.access_key", s3ClientName), accessKey);
                    cluster.keystore(format("s3.client.%s.secret_key", s3ClientName), secretKey);

                    if (useFixture) {
                        cluster.setting(format("s3.client.%s.protocol", s3ClientName), "http");
                        cluster.setting(format("s3.client.%s.endpoint", s3ClientName),
                            () -> {
                                Integer ephemeralPort = (Integer) fixture.getTasks().findByName("postProcessFixture")
                                    .getExtensions()
                                    .getByType(ExtraPropertiesExtension.class)
                                    .get(format("test.fixtures.%s.tcp.80", fixtureName));

                                assert ephemeralPort != null && ephemeralPort > 0;
                                return "127.0.0.1:" + ephemeralPort;
                            },
                            PropertyNormalization.IGNORE_VALUE);
                    } else {
                        System.out.println("Using an external service to test " + project.getName());
                    }
                });
        }

        @SuppressWarnings("unchecked")
        private static NamedDomainObjectContainer<ElasticsearchCluster> testClusters(Project project, String extensionName) {
            return (NamedDomainObjectContainer<ElasticsearchCluster>) project.getExtensions().getByName(extensionName);
        }

        private static String format(String format, String arg) {
            return String.format(Locale.ROOT, format, arg);
        }
    }
}
