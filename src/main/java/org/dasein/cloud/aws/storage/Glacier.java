/**
 * Copyright (C) 2009-2013 Dell, Inc.
 * See annotations for authorship information
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.cloud.aws.storage;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.NameRules;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.storage.Blob;
import org.dasein.cloud.storage.BlobStoreSupport;
import org.dasein.cloud.storage.FileTransfer;
import org.dasein.cloud.storage.OfflineStoreRequest;
import org.dasein.cloud.storage.OfflineStoreRequestAction;
import org.dasein.cloud.storage.OfflineStoreRequestStatus;
import org.dasein.cloud.storage.OfflineStoreSupport;
import org.dasein.cloud.util.APITrace;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorPopulator;
import org.dasein.util.PopulatorThread;
import org.dasein.util.uom.storage.Byte;
import org.dasein.util.uom.storage.Megabyte;
import org.dasein.util.uom.storage.Storage;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SimpleTimeZone;

/**
 * Implements support for Amazon Glacier using the Dasein Cloud blob storage interface. Dasein Cloud buckets are
 * Glacier vaults and Dasein Cloud objects are Glacier archives. All multi-part transformations are handled within the
 * Dasein Cloud implementation.
 * @author George Reese
 * @version 2013.07 initial implementation (issue #45)
 * @since 2013.07
 */
public class Glacier implements OfflineStoreSupport {
    static private final Logger logger = AWSCloud.getLogger(Glacier.class);

    static public final int                                       MAX_VAULTS       = 1000;
    static public final int                                       MAX_ARCHIVES     = -1;
    static public final Storage<Megabyte>                         MAX_OBJECT_SIZE  = new Storage<Megabyte>(100L, Storage.MEGABYTE);
    public static final String ACTION_ARCHIVE_RETRIEVAL = "ArchiveRetrieval";
    public static final String ACTION_INVENTORY_RETRIEVAL = "InventoryRetrieval";
    public static final String HEADER_JOB_ID = "x-amz-job-id";
    public static final String MARKER = "Marker";

    private AWSCloud provider = null;

    public Glacier(AWSCloud provider) {
        this.provider = provider;
    }

    @Override
    public boolean allowsNestedBuckets() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean allowsRootObjects() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean allowsPublicSharing() throws CloudException, InternalException {
        return true;
    }

    @Override
    public void clearBucket(@Nonnull String bucket) throws CloudException, InternalException {
        throw new OperationNotSupportedException("bucket clearing is not supported by Glacier");
    }

    @Override
    public @Nonnull Blob createBucket(@Nonnull String bucketName, boolean findFreeName) throws InternalException, CloudException {
        APITrace.begin(provider, "Blob.createBucket");
        try {
            if( bucketName.contains("/") ) {
                throw new OperationNotSupportedException("Nested buckets are not supported");
            }
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new InternalException("No context was set for this request");
            }
            String regionId = ctx.getRegionId();

            if( regionId == null ) {
                throw new InternalException("No region ID was specified for this request");
            }

            GlacierMethod method = GlacierMethod.build(
                    provider, GlacierAction.CREATE_VAULT).vaultId(bucketName).toMethod();
            method.invoke();
            String url = method.getUrl();
            return Blob.getInstance(regionId, url, bucketName, System.currentTimeMillis());
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public FileTransfer download(@Nullable String bucket, @Nonnull String objectName, @Nonnull File toFile) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Glacier downloads must be performed using createDownloadRequest()");
    }

    @Override
    public boolean exists(@Nonnull String bucketName) throws InternalException, CloudException {
        APITrace.begin(provider, "Blob.exists");
        try {
            return getBucket(bucketName) != null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Blob getBucket(@Nonnull String bucketName) throws InternalException, CloudException {
        APITrace.begin(provider, "Blob.getBucket");
        try {
            if( bucketName.contains("/") ) {
                return null;
            }
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            String regionId = ctx.getRegionId();

            if( regionId == null ) {
                throw new CloudException("No region was set for this request");
            }

            try {
                GlacierMethod method = GlacierMethod.build(provider, GlacierAction.DESCRIBE_VAULT)
                        .vaultId(bucketName).toMethod();
                JSONObject jsonObject = method.invokeJson();
                if (jsonObject == null) {
                    return null;
                }
                return loadVaultJson(jsonObject, regionId, method.getUrl());

            } catch (GlacierException e) {
                if (e.getHttpCode() == 404) {
                    return null;
                }
                throw e;
            } catch (JSONException e) {
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Blob getObject(@Nullable String bucketName, @Nonnull String objectName) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Glacier objects cannot be accessed synchronously");
    }

    @Override
    public @Nullable Storage<org.dasein.util.uom.storage.Byte> getObjectSize(@Nullable String bucket, @Nullable String object) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Glacier objects cannot be accessed synchronously");
    }

    @Override
    public int getMaxBuckets() throws CloudException, InternalException {
        return MAX_VAULTS;
    }

    @Override
    public Storage<org.dasein.util.uom.storage.Byte> getMaxObjectSize() {
        return (Storage<org.dasein.util.uom.storage.Byte>)MAX_OBJECT_SIZE.convertTo(Storage.BYTE);
    }

    @Override
    public int getMaxObjectsPerBucket() throws CloudException, InternalException {
        return MAX_ARCHIVES;
    }

    @Override
    public @Nonnull String getProviderTermForBucket(@Nonnull Locale locale) {
        return "vault";
    }

    @Override
    public @Nonnull String getProviderTermForObject(@Nonnull Locale locale) {
        return "archive";
    }

    @Override
    public boolean isPublic(@Nullable String bucket, @Nullable String object) throws CloudException, InternalException {
        return false;
    }
    
    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(provider, "Blob.isSubscribed");
        try {
            final ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was specified for this request");
            }
            final String regionId = ctx.getRegionId();

            if( regionId == null ) {
                throw new CloudException("No region ID was specified");
            }
            try {
                GlacierMethod method = GlacierMethod.build(provider, GlacierAction.LIST_VAULTS).toMethod();
                method.invoke();
                return true;
            } catch (GlacierException e) {
                return false;
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Collection<Blob> list(final @Nullable String bucket) throws CloudException, InternalException {
        final ProviderContext ctx = provider.getContext();
        PopulatorThread <Blob> populator;

        if( ctx == null ) {
            throw new CloudException("No context was specified for this request");
        }
        final String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region ID was specified");
        }
    	provider.hold();
    	populator = new PopulatorThread<Blob>(new JiteratorPopulator<Blob>() {
    		public void populate(@Nonnull Jiterator<Blob> iterator) throws CloudException, InternalException {
                try {
                    list(regionId, bucket, iterator);
                }
                finally {
                    provider.release();
                }
    		}
    	});
    	populator.populate();
    	return populator.getResult();
    }

    private void list(@Nonnull String regionId, @Nullable String bucket, @Nonnull Jiterator<Blob> iterator) throws CloudException, InternalException {
        APITrace.begin(provider, "Blob.list");
        try {
            if( bucket == null ) {
                loadVaults(regionId, iterator);
            }
            else {
                throw new OperationNotSupportedException("Glacier vault contents cannot be listed synchronously");
            }
        }
        finally {
            APITrace.end();
        }
    }

    private void loadVaults(@Nonnull String regionId, @Nonnull Jiterator<Blob> iterator) throws CloudException, InternalException {

        boolean needQuery = true;
        String marker = null;
        Map<String, String> queryParameters = new HashMap<String, String>(1);

        // glacier can paginate results. it returns a "marker" string in the JSON response
        // which indicates you should make another query. The marker string should be passed
        // as a query parameter in the next query.

        while (needQuery) {
            if (marker != null) {
                queryParameters.put("marker", marker);
            }

            GlacierMethod method = GlacierMethod.build(provider, GlacierAction.LIST_VAULTS)
                    .queryParameters(queryParameters).toMethod();
            String baseUrl = method.getUrl();
            JSONObject jsonObject = method.invokeJson();

            try {
                JSONArray vaultList = jsonObject.getJSONArray("VaultList");

                for (int i=0; i<vaultList.length(); i++) {
                    iterator.push(loadVaultJson(vaultList.getJSONObject(i), regionId, baseUrl));
                }

                marker = getPaginationMarker(jsonObject);
                if (marker == null) {
                    needQuery = false;
                }
            } catch (JSONException e) {
                throw new CloudException(e);
            }
        }
    }

    private String getPaginationMarker(JSONObject jsonObject) throws JSONException {
        if (jsonObject.has(MARKER) && !jsonObject.isNull(MARKER)) {
            final String marker = jsonObject.getString(MARKER);
            if (marker.length() > 0) {
                return marker;
            }
        }
        return null;
    }

    private Blob loadVaultJson(JSONObject jsonObject, String regionId, String baseUrl) throws JSONException {
        String vaultName = jsonObject.getString("VaultName");
        String url;
        if (baseUrl.endsWith(vaultName)) {
            url = baseUrl;
        } else {
            url = baseUrl + "/" + vaultName;
        }

        String creationDate = jsonObject.getString("CreationDate");

        long creationTs = parseTimestamp(creationDate);

        if (jsonObject.has("SizeInBytes") && !jsonObject.isNull("SizeInBytes")) {
            Storage<Byte> storage = new Storage<Byte>(jsonObject.getLong("SizeInBytes"), Storage.BYTE);

            // somewhat dangerous: we want to specify a size for the vault, but currently
            // dasein-core only allows this specified on the Blob.getInstance() intended for
            // objects. It has a @Nonnull decorator for objectName, but we pass a null value
            // here. Currently in the implementation it does not matter.

            //noinspection ConstantConditions
            return Blob.getInstance(regionId, url, vaultName, null, creationTs, storage);
        }
        return Blob.getInstance(regionId, url, vaultName, creationTs);
    }

    private static long parseTimestamp(String timestamp) {
        if (timestamp == null) {
            return -1;
        }
        long creationTs;
        SimpleDateFormat fmt;

        // some response dates have MS component, some do not.
        if (timestamp.contains(".")) {
            fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        }
        else {
            fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        }
        Calendar cal = Calendar.getInstance(new SimpleTimeZone(0, "GMT"));
        fmt.setCalendar(cal);
        try {
            creationTs = fmt.parse(timestamp).getTime();
        } catch (ParseException e) {
            creationTs = System.currentTimeMillis();
        }
        return creationTs;
    }

    @Override
    public void makePublic(@Nonnull String bucket) throws InternalException, CloudException {
    	makePublic(bucket, null);
    }

    @Override
    public void makePublic(@Nullable String bucket, @Nullable String object) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Cannot make vaults public");
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        if( action.equals(BlobStoreSupport.ANY) ) {
            return new String[] { GlacierMethod.GLACIER_PREFIX + "*" };
        }
        else if( action.equals(BlobStoreSupport.CREATE_BUCKET) ) {
            return new String[] { GlacierMethod.GLACIER_PREFIX + "CreateVault" };
        }
        else if( action.equals(BlobStoreSupport.GET_BUCKET) ) {
            return new String[] { GlacierMethod.GLACIER_PREFIX + "DescribeVault" };
        }
        else if( action.equals(BlobStoreSupport.LIST_BUCKET) ) {
            return new String[] { GlacierMethod.GLACIER_PREFIX + "ListVaults" };
        }
        else if ( action.equals(BlobStoreSupport.REMOVE_BUCKET) ) {
            return new String[] { GlacierMethod.GLACIER_PREFIX + "DeleteVault"};
        }
        else if( action.equals(BlobStoreSupport.UPLOAD) ) {
            return new String[] { GlacierMethod.GLACIER_PREFIX + "UploadArchive" };
        }
        else if ( action.equals(OfflineStoreSupport.CREATE_REQUEST) ) {
            return new String[] { GlacierMethod.GLACIER_PREFIX + "InitiateJob" };
        }
        else if ( action.equals(OfflineStoreSupport.GET_REQUEST) ) {
            return new String[] { GlacierMethod.GLACIER_PREFIX + "DescribeJob" };
        }
        else if ( action.equals(OfflineStoreSupport.LIST_REQUEST) ) {
            return new String[] { GlacierMethod.GLACIER_PREFIX + "ListJobs" };
        }
        else if ( action.equals(OfflineStoreSupport.GET_REQUEST_RESULT) ) {
            return new String[] { GlacierMethod.GLACIER_PREFIX + "GetJobOutput" };
        }
        return new String[0];
    }

    @Override
    public void move(@Nullable String sourceBucket, @Nullable String object, @Nullable String targetBucket) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Cannot move vaults Glacier archives");
    }

    protected void put(@Nullable String bucket, @Nonnull String object, @Nonnull File file) throws CloudException, InternalException {
        APITrace.begin(provider, "Blob.putFile");
        try {
            // TODO: upload
        }
        finally {
            APITrace.end();
        }
    }

    protected void put(@Nullable String bucket, @Nonnull String object, @Nonnull String content) throws CloudException, InternalException {
        APITrace.begin(provider, "Blob.putString");
        try {
            // TODO: upload
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeBucket(@Nonnull String bucket) throws CloudException, InternalException {
        APITrace.begin(provider, "Blob.removeBucket");
        try {

            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            String regionId = ctx.getRegionId();

            if( regionId == null ) {
                throw new CloudException("No region was set for this request");
            }

            GlacierMethod method = GlacierMethod.build(provider, GlacierAction.DELETE_VAULT)
                    .vaultId(bucket).toMethod();
            method.invoke();
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeObject(@Nullable String bucket, @Nonnull String name) throws CloudException, InternalException {
        APITrace.begin(provider, "Blob.removeObject");
        try {
            if( bucket == null ) {
                throw new CloudException("No bucket was specified for this request");
            }
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            String regionId = ctx.getRegionId();

            if( regionId == null ) {
                throw new CloudException("No region was set for this request");
            }

            GlacierMethod method = GlacierMethod.build(provider, GlacierAction.DELETE_ARCHIVE)
                    .vaultId(bucket).archiveId(name).toMethod();
            method.invoke();
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String renameBucket(@Nonnull String oldName, @Nonnull String newName, boolean findFreeName) throws CloudException, InternalException {
        throw new OperationNotSupportedException();
    }

    @Override
    public void renameObject(@Nullable String bucket, @Nonnull String object, @Nonnull String newName) throws CloudException, InternalException {
        throw new OperationNotSupportedException();
    }

    @Override
    public @Nonnull Blob upload(@Nonnull File source, @Nullable String bucket, @Nonnull String fileName) throws CloudException, InternalException {
        APITrace.begin(provider, "Blob.upload");
        try {
            if( bucket == null ) {
                throw new OperationNotSupportedException("Root objects are not supported");
            }
            if( !exists(bucket) ) {
                createBucket(bucket, false);
            }
            put(bucket, fileName, source);
            return getObject(bucket, fileName);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull NameRules getBucketNameRules() throws CloudException, InternalException {
        return NameRules.getInstance(1, 255, false, true, true, new char[] { '-', '.' });
    }

    @Override
    public @Nonnull NameRules getObjectNameRules() throws CloudException, InternalException {
        return NameRules.getInstance(1, 255, false, true, true, new char[] { '-', '.', ',', '#', '+' });
    }

    @Nonnull
    @Override
    public Iterable<OfflineStoreRequest> listRequests(@Nonnull final String bucket) throws CloudException, InternalException {
        final ProviderContext ctx = provider.getContext();
        PopulatorThread <OfflineStoreRequest> populator;

        if( ctx == null ) {
            throw new CloudException("No context was specified for this request");
        }
        final String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region ID was specified");
        }
        provider.hold();
        populator = new PopulatorThread<OfflineStoreRequest>(new JiteratorPopulator<OfflineStoreRequest>() {
            public void populate(@Nonnull Jiterator<OfflineStoreRequest> iterator) throws CloudException, InternalException {
                try {
                    listRequests(bucket, iterator);
                }
                finally {
                    provider.release();
                }
            }
        });
        populator.populate();
        return populator.getResult();
    }
    private void listRequests(@Nonnull String bucket, @Nonnull Jiterator<OfflineStoreRequest> iterator) throws CloudException, InternalException {
        APITrace.begin(provider, "Blob.listRequests");
        try {

            boolean needQuery = true;
            String marker = null;
            Map<String, String> queryParameters = new HashMap<String, String>(1);

            // glacier can paginate results. it returns a "marker" string in the JSON response
            // which indicates you should make another query. The marker string should be passed
            // as a query parameter in the next query.

            while (needQuery) {
                if (marker != null) {
                    queryParameters.put("marker", marker);
                }

                GlacierMethod method = GlacierMethod.build(provider, GlacierAction.LIST_JOBS)
                        .vaultId(bucket).queryParameters(queryParameters).toMethod();
                final JSONObject jsonObject = method.invokeJson();

                try {
                    JSONArray vaultList = jsonObject.getJSONArray("JobList");

                    for (int i=0; i<vaultList.length(); i++) {
                        iterator.push(loadRequestJson(vaultList.getJSONObject(i), bucket));
                    }

                    marker = getPaginationMarker(jsonObject);
                    if (marker == null) {
                        needQuery = false;
                    }
                } catch (JSONException e) {
                    throw new CloudException(e);
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    private OfflineStoreRequest loadRequestJson(JSONObject jsonObject, String bucket)
            throws CloudException, JSONException {
        String jobId = jsonObject.getString("JobId");
        String actionDescription = jsonObject.getString("Action");


        String archiveId = null;
        if (jsonObject.has("ArchiveId") && !jsonObject.isNull("ArchiveId")) {
            archiveId = jsonObject.getString("ArchiveId");
        }

        OfflineStoreRequestAction action = OfflineStoreRequestAction.UNKNOWN;
        String sizeKey = null;
        if (actionDescription == null) {
            throw new CloudException("invalid glacier job action");
        }
        else if (actionDescription.equalsIgnoreCase(ACTION_ARCHIVE_RETRIEVAL)) {
            action = OfflineStoreRequestAction.DOWNLOAD;
            sizeKey = "ArchiveSizeInBytes";
        } else if (actionDescription.equalsIgnoreCase(ACTION_INVENTORY_RETRIEVAL)) {
            action = OfflineStoreRequestAction.LIST;
            sizeKey = "InventorySizeInBytes";
        }
        long bytes = -1;
        if (jsonObject.has(sizeKey) && !jsonObject.isNull(sizeKey)) {
            bytes = jsonObject.getLong(sizeKey);
        }

        String jobDescription = jsonObject.getString("JobDescription");
        if (jobDescription == null) {
            jobDescription = "";
        }

        String statusCode = jsonObject.getString("StatusCode");
        String statusDescription = jsonObject.getString("StatusMessage");
        if (statusDescription == null) {
            statusDescription = "";
        }

        long creationTs = parseTimestamp(jsonObject.getString("CreationDate"));
        long completionTs = jsonObject.isNull("CompletionDate") ? -1L : parseTimestamp(jsonObject.getString("CompletionDate"));

        Storage<Byte> storage = bytes != -1 ? new Storage<Byte>(bytes, Storage.BYTE) : null;
        OfflineStoreRequestStatus requestStatus = parseRequestStatus(statusCode);

        return new OfflineStoreRequest(jobId, bucket, archiveId, action, actionDescription,
                storage, jobDescription, requestStatus, statusDescription, creationTs, completionTs);
    }

    private static OfflineStoreRequestStatus parseRequestStatus(String statusCode) throws CloudException {
        OfflineStoreRequestStatus requestStatus;
        if (statusCode == null) {
            throw new CloudException("invalid glacier job status");
        }
        else if (statusCode.equalsIgnoreCase("Succeeded")) {
            requestStatus = OfflineStoreRequestStatus.SUCCEEDED;
        }
        else if (statusCode.equalsIgnoreCase("InProgress")) {
            requestStatus = OfflineStoreRequestStatus.IN_PROGRESS;
        }
        else if (statusCode.equalsIgnoreCase("Failed")) {
            requestStatus = OfflineStoreRequestStatus.FAILED;
        }
        else {
            throw new CloudException("invalid glacier job status: " + statusCode);
        }
        return requestStatus;
    }

    @Nullable
    @Override
    public OfflineStoreRequest getRequest(@Nonnull String bucket, @Nonnull String requestId) throws CloudException, InternalException {
        APITrace.begin(provider, "Blob.getRequest");
        try {

            final GlacierMethod method = GlacierMethod.build(provider, GlacierAction.DESCRIBE_JOB)
                    .vaultId(bucket).jobId(requestId).toMethod();

            try {
                final JSONObject jsonObject = method.invokeJson();
                return loadRequestJson(jsonObject, bucket);

            } catch (GlacierException e) {
                if (e.getHttpCode() == 404) {
                    return null;
                }
                throw e;
            } catch (JSONException e) {
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull
    @Override
    public OfflineStoreRequest createListRequest(@Nonnull String bucket) throws CloudException, InternalException {
        APITrace.begin(provider, "Blob.createListRequest");
        try {
            try {

                JSONObject bodyJson = new JSONObject();
                bodyJson.put("Type", "inventory-retrieval");

                final GlacierMethod method = GlacierMethod.build(provider, GlacierAction.CREATE_JOB)
                        .vaultId(bucket).bodyText(bodyJson.toString()).toMethod();

                Map<String,String> responseHeaders = method.invokeHeaders();
                if (!responseHeaders.containsKey(HEADER_JOB_ID)) {
                    throw new CloudException("Glacier response missing " + HEADER_JOB_ID + " header");
                }
                String jobId = responseHeaders.get(HEADER_JOB_ID);

                return new OfflineStoreRequest(jobId, bucket, null, OfflineStoreRequestAction.LIST,
                        ACTION_INVENTORY_RETRIEVAL, null, "", OfflineStoreRequestStatus.IN_PROGRESS, "",
                        System.currentTimeMillis(), -1);

            } catch (JSONException e) {
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull
    @Override
    public OfflineStoreRequest createDownloadRequest(@Nonnull String bucket, @Nonnull String object) throws CloudException, InternalException {
        APITrace.begin(provider, "Blob.createDownloadRequest");
        try {
            // todo
            throw new OperationNotSupportedException("glacier downloads are not yet supported");
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull
    @Override
    public Iterable<Blob> getListRequestResult(@Nonnull String bucket, @Nonnull String requestId)
            throws InternalException, CloudException {
        final ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was specified for this request");
        }
        final String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region ID was specified");
        }
        APITrace.begin(provider, "Blob.getListRequestResult");
        try {

            final GlacierMethod method = GlacierMethod.build(provider, GlacierAction.GET_JOB_OUTPUT)
                    .vaultId(bucket).jobId(requestId).toMethod();

            try {
                JSONObject jsonObject = method.invokeJson();
                JSONArray archives = jsonObject.getJSONArray("ArchiveList");

                // not using thread here because there is no potential for pagination in the API
                List<Blob> blobs = new ArrayList<Blob>(archives.length());
                for (int i=0; i<archives.length(); i++) {
                    blobs.add(loadArchiveJson(archives.getJSONObject(i), bucket, regionId));
                }
                return blobs;

            } catch (JSONException e) {
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    private Blob loadArchiveJson(JSONObject jsonObject, String bucket, String regionId) throws JSONException {
        String archiveId = jsonObject.getString("ArchiveId");
        Storage<Byte> size = new Storage<Byte>(jsonObject.getLong("Size"), Storage.BYTE);
        return Blob.getInstance(regionId, archiveId, bucket, archiveId,
                parseTimestamp(jsonObject.getString("CreationDate")), size);
    }

    @Nonnull
    @Override
    public FileTransfer getDownloadRequestResult(@Nonnull String bucket, @Nonnull String requestId, @Nonnull File toFile) throws InternalException, CloudException {
        APITrace.begin(provider, "Blob.getDownloadRequestResult");
        try {
            throw new OperationNotSupportedException("glacier downloads are not yet supported");
        }
        finally {
            APITrace.end();
        }
    }
}
