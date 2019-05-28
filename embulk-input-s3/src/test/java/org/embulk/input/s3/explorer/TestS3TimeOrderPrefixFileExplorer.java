package org.embulk.input.s3.explorer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.embulk.EmbulkTestRuntime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.FieldSetter;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestS3TimeOrderPrefixFileExplorer
{
    private static final String BUCKET_NAME = "bucket_name";
    private static final String PATH_PREFIX = "path_prefix";

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Mock
    private AmazonS3 s3Client;

    private S3TimeOrderPrefixFileExplorer s3TimeOrderPrefixFileExplorer;

    @Before
    public void setUp()
    {
        final Calendar cal = Calendar.getInstance();
        cal.set(2019, Calendar.MAY, 25, 10, 0);
        s3TimeOrderPrefixFileExplorer = new S3TimeOrderPrefixFileExplorer(BUCKET_NAME, s3Client, null, PATH_PREFIX,
                false, Optional.empty(), cal.getTime());
    }

    @Test
    public void fetch_should_return_list_filtered_objects_by_time()
    {
        final S3ObjectSummary s3ObjectBefore = mock(S3ObjectSummary.class);
        final Calendar cal = Calendar.getInstance();
        cal.set(2019, Calendar.MAY, 24, 10, 0);
        when(s3ObjectBefore.getLastModified()).thenReturn(cal.getTime());

        final S3ObjectSummary s3ObjectAfter = mock(S3ObjectSummary.class);
        cal.set(2019, Calendar.MAY, 26, 10, 0);
        when(s3ObjectAfter.getLastModified()).thenReturn(cal.getTime());

        final ObjectListing ol = mock(ObjectListing.class);
        when(s3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(ol);
        when(ol.getObjectSummaries()).thenReturn(Arrays.asList(s3ObjectBefore, s3ObjectAfter));

        final List<S3ObjectSummary> result = s3TimeOrderPrefixFileExplorer.fetch();
        assertEquals(1, result.size());
        assertEquals(s3ObjectBefore, result.get(0));
    }

    @Test
    public void hasNext_should_return_false_if_no_lastpath() throws NoSuchFieldException
    {
        new FieldSetter(s3TimeOrderPrefixFileExplorer, s3TimeOrderPrefixFileExplorer.getClass().getDeclaredField("lastPath")).set(null);
        assertFalse(s3TimeOrderPrefixFileExplorer.hasNext());
    }
}
