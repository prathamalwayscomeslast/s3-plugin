/*
 * The MIT License
 *
 * Copyright (c) 2021, CloudBees Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package hudson.plugins.s3;

import hudson.ExtensionList;
import hudson.FilePath;
import hudson.Launcher;
import hudson.model.AbstractBuild;
import hudson.model.BuildListener;
import hudson.model.FreeStyleProject;
import hudson.model.Label;
import hudson.plugins.copyartifact.LastCompletedBuildSelector;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.jvnet.hudson.test.CreateFileBuilder;
import org.jvnet.hudson.test.JenkinsRule;
import org.jvnet.hudson.test.RealJenkinsRule;
import org.jvnet.hudson.test.TestBuilder;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class MinIOTest {

    private static final String ACCESS_KEY = "supersecure";
    private static final String SECRET_KEY = "donttell";
    private static final String CONTAINER_NAME = "jenkins";
    private static final String CONTAINER_PREFIX = "ci/";
    private static final String REGION = "local";
    public static final String FILE_CONTENT = "Hello World";

    private static GenericContainer minioServer;
    private static String minioServiceEndpoint;

    @Rule
    public RealJenkinsRule rr = new RealJenkinsRule().javaOptions("-Xmx256m",
            "-Dhudson.plugins.s3.ENDPOINT=http://" + minioServiceEndpoint);

    @BeforeClass
    public static void setUpClass() throws Exception {
        try {
            DockerClientFactory.instance().client();
        } catch (Exception x) {
            Assume.assumeNoException("does not look like Docker is available", x);
        }
        int port = 9000;
        minioServer = new GenericContainer("minio/minio")
                .withEnv("MINIO_ACCESS_KEY", ACCESS_KEY)
                .withEnv("MINIO_SECRET_KEY", SECRET_KEY)
                .withCommand("server /data")
                .withExposedPorts(port)
                .waitingFor(new HttpWaitStrategy()
                        .forPath("/minio/health/ready")
                        .forPort(port)
                        .withStartupTimeout(Duration.ofSeconds(10)));
        minioServer.start();

        Integer mappedPort = minioServer.getFirstMappedPort();
        Testcontainers.exposeHostPorts(mappedPort);
        minioServiceEndpoint = String.format("%s:%s", minioServer.getContainerIpAddress(), mappedPort);
        S3ClientBuilder builder = S3Client.builder().credentialsProvider(() -> AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY));
        builder.region(Region.US_EAST_1).endpointOverride(URI.create("http://" + minioServiceEndpoint))
                .forcePathStyle(true);

        try (S3Client client = builder.build()) {
            if (!doesBucketExistV2(client, "test")) {
                client.createBucket(CreateBucketRequest.builder().bucket("test").build());
                assertTrue(doesBucketExistV2(client, "test"));
            }
        }
    }

    static boolean doesBucketExistV2(S3Client client, String bucketName) {
        try {
            HeadBucketResponse test = client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        }
    }

    @AfterClass
    public static void shutDownClass() {
        if (minioServer != null && minioServer.isRunning()) {
            minioServer.stop();
        }
    }

    @Test
    public void testS3BucketPublisher() throws Throwable {
        final String endpoint = minioServiceEndpoint;
        rr.then(r -> {
            /*r.jenkins.setLabelString("work"); //Able to debug when running on the controller but not an agent
            r.jenkins.setNumExecutors(1);*/
            r.createOnlineSlave(Label.get("work"));
            createProfile();
            createAndRunPublisherWithCustomTimeout(r);
        });
    }

    private static @NotNull S3BucketPublisher getS3BucketPublisher(String sourceFileName) {
        return new S3BucketPublisher(
                "Local",
                Collections.singletonList(new Entry("test",
                        sourceFileName,
                        "",
                        null,
                        "",
                        false,
                        true,
                        true,
                        false,
                        false,
                        false,
                        true,
                        false,
                        Collections.emptyList())),
                Collections.emptyList(),
                true,
                "FINE",
                null,
                false);
    }

    private static void createAndRunPublisherWithCustomTimeout(final JenkinsRule r) throws Exception {
        final FreeStyleProject job = r.createFreeStyleProject("publisherJob");
        job.setAssignedLabel(Label.get("work"));
        final String fileName = "test.txt";
        job.getBuildersList().add(new CreateFileBuilder(fileName, FILE_CONTENT));
        S3BucketPublisher publisher = getS3BucketPublisher(fileName);
        publisher.setUploadTimeout(10);
        job.getPublishersList().add(publisher);
        r.buildAndAssertSuccess(job);
    }

    private static void createProfile() {
        final S3BucketPublisher.DescriptorImpl descriptor = ExtensionList.lookup(S3BucketPublisher.DescriptorImpl.class).get(0);
        S3Profile profile = new S3Profile(
                "Local",
                ACCESS_KEY, SECRET_KEY,
                false,
                10000,
                "", "", "", "", true);
        profile.setUsePathStyle(true);
        descriptor.replaceProfiles(Collections.singletonList(profile));
    }

    @Test
    public void testS3CopyArtifact() throws Throwable {
        final String endpoint = minioServiceEndpoint;
        rr.then(r -> {
            r.createOnlineSlave(Label.get("work"));
            r.createOnlineSlave(Label.get("copy"));

            createProfile();
            createAndRunPublisherWithCustomTimeout(r);

            FreeStyleProject job = r.createFreeStyleProject("copierJob");
            job.setAssignedLabel(Label.get("copy"));
            job.getBuildersList().add(new S3CopyArtifact(
                    "publisherJob",
                    new LastCompletedBuildSelector(),
                    "*.txt",
                    "",
                    "",
                    false,
                    false
            ));
            job.getBuildersList().add(new VerifyFileBuilder());
            r.buildAndAssertSuccess(job);
        });
    }

    // TODO: Use ParameterizedTest after migrating to junit 5
    @Test
    public void testChecksumAlgorithms() throws Throwable {
        List<String> failures = new ArrayList<>();
        List<ChecksumAlgorithm> algos = new ArrayList<>(ChecksumAlgorithm.knownValues());
        // Apparently CRC64NVME is incompatible with MinIO, leading to unstable builds and failure in file uploads
        // This line should be removed when it is eventually supported
        algos.remove(ChecksumAlgorithm.CRC64_NVME);
        algos.add(null); // For checking if default algorithm is CRC32

        rr.then( r -> {
            r.createOnlineSlave(Label.get("work"));
            createProfile();
            for (ChecksumAlgorithm algo : algos) {
                String suffix = (algo == null) ? "default" : algo.toString();
                String fileName = "test-" + suffix + ".txt";
                FreeStyleProject job = r.createFreeStyleProject("checksum-" + suffix);
                job.setAssignedLabel(Label.get("work"));
                job.getBuildersList().add(new CreateFileBuilder(fileName, FILE_CONTENT));
                S3BucketPublisher publisher = getS3BucketPublisher(fileName);
                if (algo != null)
                    publisher.setChecksumAlgorithm(algo);

                job.getPublishersList().add(publisher);
                try {
                    r.buildAndAssertSuccess(job);
                } catch (Exception e) {
                    throw new AssertionError("Build failed for algorithm: " + suffix, e);
                }
            }
        });

        try (S3Client client = S3Client.builder()
                .credentialsProvider(() -> AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY))
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create("http://" + minioServiceEndpoint))
                .forcePathStyle(true)
                .build()) {

            for (ChecksumAlgorithm algo : algos) {
                String suffix = (algo == null) ? "default" : algo.toString();
                String fileName = "test-" + suffix + ".txt";
                System.out.println(fileName);
                HeadObjectResponse head = client.headObject(
                    HeadObjectRequest.builder()
                            .bucket("test")
                            .key(fileName)
                            .build()
                );
                String actual = head.checksumTypeAsString();
                String expected = (algo == null) ? "CRC32" : algo.toString();
                if (!expected.equals(actual)) {
                    failures.add(
                            "Checksum validation failed for algorithm: " + suffix +
                            "\nExpected: " + expected +
                            "\nActual: " + actual
                    );
                }
            }
        }
        if (!failures.isEmpty()) {
            fail(String.join("\n", failures));
        }
    }

    public static class VerifyFileBuilder extends TestBuilder {
        @Override
        public boolean perform(final AbstractBuild<?, ?> build, final Launcher launcher, final BuildListener listener) throws InterruptedException, IOException {
            final FilePath child = build.getWorkspace().child("test.txt");
            assertTrue("No test.txt in workspace!", child.exists());

            final String s = child.readToString();
            assertEquals(FILE_CONTENT, s);

            return true;
        }
    }
}
