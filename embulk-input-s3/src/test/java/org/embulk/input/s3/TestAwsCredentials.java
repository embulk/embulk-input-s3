package org.embulk.input.s3;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetFederationTokenRequest;
import com.amazonaws.services.securitytoken.model.GetFederationTokenResult;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.input.s3.TestS3FileInputPlugin.Control;
import org.embulk.spi.FileInputRunner;
import org.embulk.spi.TestPageBuilderReader;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static org.embulk.input.s3.TestS3FileInputPlugin.assertRecords;
import static org.embulk.input.s3.TestS3FileInputPlugin.parserConfig;
import static org.embulk.input.s3.TestS3FileInputPlugin.schemaConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

public class TestAwsCredentials
{
    private static String EMBULK_S3_TEST_BUCKET;
    private static String EMBULK_S3_TEST_ACCESS_KEY_ID;
    private static String EMBULK_S3_TEST_SECRET_ACCESS_KEY;
    private static final String EMBULK_S3_TEST_PATH_PREFIX = "embulk_input_s3_test";

    /*
     * This test case requires environment variables:
     *   EMBULK_S3_TEST_BUCKET
     *   EMBULK_S3_TEST_ACCESS_KEY_ID
     *   EMBULK_S3_TEST_SECRET_ACCESS_KEY
     * If the variables not set, the test case is skipped.
     */
    @BeforeClass
    public static void initializeConstantVariables()
    {
        EMBULK_S3_TEST_BUCKET = System.getenv("EMBULK_S3_TEST_BUCKET");
        EMBULK_S3_TEST_ACCESS_KEY_ID = System.getenv("EMBULK_S3_TEST_ACCESS_KEY_ID");
        EMBULK_S3_TEST_SECRET_ACCESS_KEY = System.getenv("EMBULK_S3_TEST_SECRET_ACCESS_KEY");
        assumeNotNull(EMBULK_S3_TEST_BUCKET, EMBULK_S3_TEST_ACCESS_KEY_ID, EMBULK_S3_TEST_SECRET_ACCESS_KEY);
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ConfigSource config;
    private FileInputRunner runner;
    private TestPageBuilderReader.MockPageOutput output;

    @Before
    public void createResources()
    {
        config = runtime.getExec().newConfigSource()
                .set("type", "s3")
                .set("bucket", EMBULK_S3_TEST_BUCKET)
                .set("path_prefix", EMBULK_S3_TEST_PATH_PREFIX)
                .set("parser", parserConfig(schemaConfig()));
        runner = new FileInputRunner(runtime.getInstance(S3FileInputPlugin.class));
        output = new TestPageBuilderReader.MockPageOutput();
    }

    private void doTest(ConfigSource config)
    {
        ConfigDiff configDiff = runner.transaction(config, new Control(runner, output));

        assertEquals(EMBULK_S3_TEST_PATH_PREFIX + "/sample_01.csv", configDiff.get(String.class, "last_path"));
        assertRecords(config, output);
    }

    @Test
    public void useBasic()
    {
        ConfigSource config = this.config.deepCopy()
                .set("auth_method", "basic")
                .set("access_key_id", EMBULK_S3_TEST_ACCESS_KEY_ID)
                .set("secret_access_key", EMBULK_S3_TEST_SECRET_ACCESS_KEY);
        doTest(config);
    }

    @Test
    public void useEnv()
    {
        // TODO
    }

    @Test
    public void useInstance()
    {
        // TODO
    }

    @Test
    public void useProfile()
    {
        // TODO
    }

    @Test
    public void useProperties()
    {
        String origAccessKeyId = System.getProperty("aws.accessKeyId");
        String origSecretKey = System.getProperty("aws.secretKey");
        try {

            ConfigSource config = this.config.deepCopy().set("auth_method", "properties");
            System.setProperty("aws.accessKeyId", EMBULK_S3_TEST_ACCESS_KEY_ID);
            System.setProperty("aws.secretKey", EMBULK_S3_TEST_SECRET_ACCESS_KEY);
            doTest(config);
        }
        finally {
            if (origAccessKeyId != null) {
                System.setProperty("aws.accessKeyId", origAccessKeyId);
            }
            if (origSecretKey != null) {
                System.setProperty("aws.secretKey", origAccessKeyId);
            }
        }
    }

    @Test
    public void useAnonymous()
    {
        // TODO
    }

    @Test
    public void useSession()
    {
        BasicSessionCredentials sessionCredentials = getSessionCredentials();
        ConfigSource config = this.config.deepCopy()
                .set("auth_method", "session")
                .set("access_key_id", sessionCredentials.getAWSAccessKeyId())
                .set("secret_access_key", sessionCredentials.getAWSSecretKey())
                .set("session_token", sessionCredentials.getSessionToken());
        doTest(config);
    }

    private static BasicSessionCredentials getSessionCredentials()
    {
        AWSSecurityTokenServiceClient stsClient = new AWSSecurityTokenServiceClient(
                new StaticCredentialsProvider(new BasicAWSCredentials(EMBULK_S3_TEST_ACCESS_KEY_ID, EMBULK_S3_TEST_SECRET_ACCESS_KEY)));

        GetFederationTokenRequest getFederationTokenRequest = new GetFederationTokenRequest();
        getFederationTokenRequest.setDurationSeconds(7200);
        getFederationTokenRequest.setName("dummy");

        Policy policy = new Policy().withStatements(new Statement(Statement.Effect.Allow)
                .withActions(S3Actions.ListObjects, S3Actions.GetObject)
                .withResources(
                        new Resource("arn:aws:s3:::" + EMBULK_S3_TEST_BUCKET + "/" + EMBULK_S3_TEST_PATH_PREFIX + "/*"),
                        new Resource("arn:aws:s3:::" + EMBULK_S3_TEST_BUCKET)));
        getFederationTokenRequest.setPolicy(policy.toJson());

        GetFederationTokenResult federationTokenResult = stsClient.getFederationToken(getFederationTokenRequest);
        Credentials sessionCredentials = federationTokenResult.getCredentials();

        return new BasicSessionCredentials(
                sessionCredentials.getAccessKeyId(),
                sessionCredentials.getSecretAccessKey(),
                sessionCredentials.getSessionToken());
    }
}
