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
import org.dasein.cloud.storage.AbstractBlobStoreSupport;
import org.dasein.cloud.storage.Blob;
import org.dasein.cloud.storage.FileTransfer;
import org.dasein.cloud.util.APITrace;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorPopulator;
import org.dasein.util.PopulatorThread;
import org.dasein.util.uom.storage.Megabyte;
import org.dasein.util.uom.storage.Storage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Locale;

/**
 * Implements support for Amazon Glacier using the Dasein Cloud blob storage interface. Dasein Cloud buckets are
 * Glacier vaults and Dasein Cloud objects are Glacier archives. All multi-part transformations are handled within the
 * Dasein Cloud implementation.
 * @author George Reese
 * @version 2013.07 initial implementation (issue #45)
 * @since 2013.07
 */
public class Glacier extends AbstractBlobStoreSupport {
    static private final Logger logger = AWSCloud.getLogger(Glacier.class);

    static public final int                                       MAX_VAULTS       = 1000;
    static public final int                                       MAX_ARCHIVES     = -1;
    static public final Storage<Megabyte>                         MAX_OBJECT_SIZE  = new Storage<Megabyte>(100L, Storage.MEGABYTE);

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
            // TODO: create vault
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean exists(@Nonnull String bucketName) throws InternalException, CloudException {
        APITrace.begin(provider, "Blob.exists");
        try {
            // TODO: implement me
            return false;
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
            // TODO: fetch bucket
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Blob getObject(@Nullable String bucketName, @Nonnull String objectName) throws InternalException, CloudException {
        APITrace.begin(provider, "Blob.getObject");
        try {
            if( bucketName == null ) {
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
            // TODO: get object
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nullable Storage<org.dasein.util.uom.storage.Byte> getObjectSize(@Nullable String bucket, @Nullable String object) throws InternalException, CloudException {
        APITrace.begin(provider, "Blob.getObjectSize");
        try {
            if( bucket == null ) {
                throw new CloudException("Requested object size for object in null bucket");
            }
            if( object == null ) {
                return null;
            }
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            // TODO: return size
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public int getMaxBuckets() throws CloudException, InternalException {
        return MAX_VAULTS;
    }

    @Override
    protected void get(@Nullable String bucket, @Nonnull String object, @Nonnull File toFile, @Nullable FileTransfer transfer) throws InternalException, CloudException {
        APITrace.begin(provider, "Blob.get");
        try {
            if( bucket == null ) {
                throw new CloudException("No bucket was specified");
            }
            IOException lastError = null;
            int attempts = 0;

            while( attempts < 5 ) {
                // todo: FETCH IT
            }
            if( lastError != null ) {
                logger.error(lastError);
                lastError.printStackTrace();
                throw new InternalException(lastError);
            }
            else {
                logger.error("Unable to figure out error");
                throw new InternalException("Unknown error");
            }
        }
        finally {
            APITrace.end();
        }
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
            // TODO: figure this out
            return false;
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
                loadArchives(regionId, bucket, iterator);
            }
        }
        finally {
            APITrace.end();
        }
    }

    private void loadVaults(@Nonnull String regionId, @Nonnull Jiterator<Blob> iterator) throws CloudException, InternalException {
        // TODO: IMPLEMENT ME
        /*
    	S3Method method = new S3Method(provider, S3Action.LIST_BUCKETS);
		S3Response response;
		NodeList blocks;		
		
		try {
			response = method.invoke(null, null);
		}
		catch( S3Exception e ) {
			logger.error(e.getSummary());
			throw new CloudException(e);
		}
		blocks = response.document.getElementsByTagName("Bucket");
		for( int i=0; i<blocks.getLength(); i++ ) {
			Node object = blocks.item(i);
            String name = null;
            NodeList attrs;
            long ts = 0L;

            attrs = object.getChildNodes();
			for( int j=0; j<attrs.getLength(); j++ ) {
				Node attr = attrs.item(j);
				
				if( attr.getNodeName().equals("Name") ) {
					name = attr.getFirstChild().getNodeValue().trim();
				}
				else if( attr.getNodeName().equals("CreationDate") ) {
                    ts = provider.parseTime(attr.getFirstChild().getNodeValue().trim());
				}
			}
			if( name == null ) {
				throw new CloudException("Bad response from server.");
            }
            if( provider.getEC2Provider().isAWS() ) {
                String location = null;

                method = new S3Method(provider, S3Action.LOCATE_BUCKET);
                try {
                    response = method.invoke(name, "?location");
                }
                catch( S3Exception e ) {
                    response = null;
                }
                if( response != null ) {
                    NodeList constraints = response.document.getElementsByTagName("LocationConstraint");

                    if( constraints.getLength() > 0 ) {
                        Node constraint = constraints.item(0);

                        if( constraint != null && constraint.hasChildNodes() ) {
                            location = constraint.getFirstChild().getNodeValue().trim();
                        }
                    }
                }
                if( toRegion(location).equals(regionId) ) {
                    iterator.push(Blob.getInstance(regionId, getLocation(name, null), name, ts));
                }
            }
            else {
                iterator.push(Blob.getInstance(regionId, getLocation(name, null), name, ts));
            }
		}
		*/
    }
    
    private void loadArchives(@Nonnull String regionId, @Nonnull String bucket, @Nonnull Jiterator<Blob> iterator) throws CloudException, InternalException {
        // TODO: IMPLEMENT ME
        /*
		HashMap<String,String> parameters = new HashMap<String,String>();
		S3Response response;
		String marker = null;
		boolean done = false;
		S3Method method;
		
		while( !done ) {
			NodeList blocks;
			
			parameters.clear();
			if( marker != null ) {
				parameters.put("marker", marker);
			}
			parameters.put("max-keys", String.valueOf(30));
			method = new S3Method(provider, S3Action.LIST_CONTENTS, parameters, null);
			try {
				response = method.invoke(bucket, null);
			}
			catch( S3Exception e ) {
			    String code = e.getCode();

			    if( code == null || !code.equals("SignatureDoesNotMatch") ) {
			        throw new CloudException(e);
			    }
				logger.error(e.getSummary());
				throw new CloudException(e);
			}
			blocks = response.document.getElementsByTagName("IsTruncated");
			if( blocks.getLength() > 0 ) {
				done = blocks.item(0).getFirstChild().getNodeValue().trim().equalsIgnoreCase("false");
			}
			blocks = response.document.getElementsByTagName("Contents");
			for( int i=0; i<blocks.getLength(); i++ ) {
				Node object = blocks.item(i);
                Storage<org.dasein.util.uom.storage.Byte> size = null;
                String name = null;
                long ts = -1L;

                if( object.hasChildNodes() ) {
                    NodeList attrs = object.getChildNodes();

                    for( int j=0; j<attrs.getLength(); j++ ) {
                        Node attr = attrs.item(j);

                        if( attr.getNodeName().equalsIgnoreCase("Key") ) {
                            String key = attr.getFirstChild().getNodeValue().trim();

                            name = key;
                            marker = key;
                        }
                        else if( attr.getNodeName().equalsIgnoreCase("Size") ) {
                            size = new Storage<org.dasein.util.uom.storage.Byte>(Long.parseLong(attr.getFirstChild().getNodeValue().trim()), Storage.BYTE);
                        }
                        else if( attr.getNodeName().equalsIgnoreCase("LastModified") ) {
                            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                            String dateString = attr.getFirstChild().getNodeValue().trim();

                            try {
                                ts = fmt.parse(dateString).getTime();
                            }
                            catch( ParseException e ) {
                                logger.error(e);
                                e.printStackTrace();
                                throw new CloudException(e);
                            }
                        }
                    }
                }
                if( name == null || size == null ) {
                    continue;
                }
                iterator.push(Blob.getInstance(regionId, getLocation(bucket, name), bucket, name, ts, size));
			}
		}
		 	*/

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
        // TODO: implement me
        return new String[0]; 
    }

    @Override
    public void move(@Nullable String sourceBucket, @Nullable String object, @Nullable String targetBucket) throws InternalException, CloudException {
        APITrace.begin(provider, "Blob.move");
        try {
            if( sourceBucket == null ) {
                throw new CloudException("No source bucket was specified");
            }
            if( targetBucket == null ) {
                throw new CloudException("No target bucket was specified");
            }
            if( object == null ) {
                throw new CloudException("No source object was specified");
            }
            copy(sourceBucket, object, targetBucket, object);
            removeObject(sourceBucket, object);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    protected void put(@Nullable String bucket, @Nonnull String object, @Nonnull File file) throws CloudException, InternalException {
        APITrace.begin(provider, "Blob.putFile");
        try {
            // TODO: upload
        }
        finally {
            APITrace.end();
        }
    }

    @Override
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
            // TODO: delete
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
            // TODO: delete
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String renameBucket(@Nonnull String oldName, @Nonnull String newName, boolean findFreeName) throws CloudException, InternalException {
        APITrace.begin(provider, "Blob.renameBucket");
        try {
            Blob bucket = createBucket(newName, findFreeName);

            for( Blob file : list(oldName) ) {
                int retries = 10;

                while( true ) {
                    retries--;
                    try {
                        move(oldName, file.getObjectName(), bucket.getBucketName());
                        break;
                    }
                    catch( CloudException e ) {
                        if( retries < 1 ) {
                            throw e;
                        }
                    }
                    try { Thread.sleep(retries * 10000L); }
                    catch( InterruptedException ignore ) { }
                }
            }
            boolean ok = true;
            for( Blob file : list(oldName ) ) {
                if( file != null ) {
                    ok = false;
                }
            }
            if( ok ) {
                removeBucket(oldName);
            }
            return newName;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void renameObject(@Nullable String bucket, @Nonnull String object, @Nonnull String newName) throws CloudException, InternalException {
        APITrace.begin(provider, "Blob.renameObject");
        try {
            if( bucket == null ) {
                throw new CloudException("No bucket was specified");
            }
            copy(bucket, object, bucket, newName);
            removeObject(bucket, object);
        }
        finally {
            APITrace.end();
        }
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
}
