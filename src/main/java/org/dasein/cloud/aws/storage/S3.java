/**
 * Copyright (C) 2009-2015 Dell, Inc.
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

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.storage.S3Method.S3Response;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.storage.AbstractBlobStoreSupport;
import org.dasein.cloud.storage.Blob;
import org.dasein.cloud.storage.BlobStoreCapabilities;
import org.dasein.cloud.storage.BlobStoreSupport;
import org.dasein.cloud.storage.FileTransfer;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.cloud.util.NamingConstraints;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorPopulator;
import org.dasein.util.PopulatorThread;
import org.dasein.util.uom.storage.Storage;
import org.dasein.util.uom.time.Day;
import org.dasein.util.uom.time.TimePeriod;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class S3 extends AbstractBlobStoreSupport<AWSCloud> {
    static private final Logger                                    logger              = AWSCloud.getLogger(S3.class);
    static private final String                                    HMAC_SHA1_ALGORITHM = "HmacSHA1";


    static private final Random random = new Random();
    private static final int MAX_RETRIES = 0;

    public S3( AWSCloud provider ) {
        super(provider);
    }

    private transient volatile S3Capabilities capabilities;

    @Nonnull
    @Override
    public BlobStoreCapabilities getCapabilities() throws CloudException, InternalException {
        if( capabilities == null ) {
            capabilities = new S3Capabilities(getProvider());
        }
        return capabilities;
    }

    @Override
    public @Nonnull Blob createBucket( @Nonnull String bucketName, boolean findFreeName ) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Blob.createBucket");
        try {
            if( bucketName.contains("/") ) {
                throw new OperationNotSupportedException("Nested buckets are not supported");
            }
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new InternalException("No context was set for this request");
            }
            String regionId = ctx.getRegionId();

            if( regionId == null ) {
                throw new InternalException("No region ID was specified for this request");
            }
            StringBuilder body = null;
            boolean success;

            success = false;
            if( getProvider().getEC2Provider().isAWS() ) {
                if( regionId.equals("eu-west-1") ) {
                    body = new StringBuilder();
                    body.append("<CreateBucketConfiguration>\r\n");
                    body.append("<LocationConstraint>");
                    body.append("EU");
                    body.append("</LocationConstraint>\r\n");
                    body.append("</CreateBucketConfiguration>\r\n");
                }
                else if( regionId.equals("us-west-1") ) {
                    body = new StringBuilder();
                    body.append("<CreateBucketConfiguration>\r\n");
                    body.append("<LocationConstraint>");
                    body.append("us-west-1");
                    body.append("</LocationConstraint>\r\n");
                    body.append("</CreateBucketConfiguration>\r\n");
                }
                else if( regionId.equals("ap-southeast-1") ) {
                    body = new StringBuilder();
                    body.append("<CreateBucketConfiguration>\r\n");
                    body.append("<LocationConstraint>");
                    body.append("ap-southeast-1");
                    body.append("</LocationConstraint>\r\n");
                    body.append("</CreateBucketConfiguration>\r\n");
                }
                else if( !regionId.equals("us-east-1") ) {
                    body = new StringBuilder();
                    body.append("<CreateBucketConfiguration>\r\n");
                    body.append("<LocationConstraint>");
                    body.append(regionId);
                    body.append("</LocationConstraint>\r\n");
                    body.append("</CreateBucketConfiguration>\r\n");
                }
            }
            while( !success ) {
                String ct = ( body == null ? null : "text/xml; charset=utf-8" );
                S3Method method;

                method = new S3Method(getProvider(), S3Action.CREATE_BUCKET, null, null, ct, body == null ? null : body.toString());
                try {
                    method.invoke(bucketName, null);
                    success = true;
                }
                catch( S3Exception e ) {
                    String code = e.getCode();

                    if( code != null && ( code.equals("BucketAlreadyExists") || code.equals("BucketAlreadyOwnedByYou") ) ) {
                        if( code.equals("BucketAlreadyOwnedByYou") ) {
                            if( !getRegion(bucketName, false).equals(regionId) ) {
                                bucketName = findFreeName(bucketName);
                            }
                            else {
                                return Blob.getInstance(regionId, getLocation(bucketName, null), bucketName, System.currentTimeMillis());
                            }
                        }
                        else if( findFreeName ) {
                            bucketName = findFreeName(bucketName);
                        }
                        else {
                            throw new CloudException(e);
                        }
                    }
                    else {
                        logger.error(e.getSummary());
                        throw new CloudException(e);
                    }
                }
            }
            // set tags
            List<Tag> tags = new ArrayList<Tag>();
            tags.add(new Tag("Name", bucketName));
            updateTags( 1, bucketName, S3Action.PUT_BUCKET_TAG, tags.toArray(new Tag[tags.size()]));

            return Blob.getInstance(regionId, "http://" + bucketName + ".s3.amazonaws.com", bucketName, System.currentTimeMillis());
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean exists( @Nonnull String bucketName ) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Blob.exists");
        try {
            S3Method method = new S3Method(getProvider(), S3Action.LOCATE_BUCKET);

            try {
                method.invoke(bucketName, "?location");
                return true;
            }
            catch( S3Exception e ) {
                if( e.getStatus() != HttpStatus.SC_NOT_FOUND ) {
                    String code = e.getCode();

                    if( e.getStatus() == HttpStatus.SC_FORBIDDEN ) {
                        return true;
                    }
                    if( code == null || !code.equals("NoSuchBucket") ) {
                        logger.error(e.getSummary());
                        throw new CloudException(e);
                    }
                }
            }
            return false;
        }
        finally {
            APITrace.end();
        }
    }

    private void updateTags ( int attempt, String bucketName, S3Action action, Tag... keyValuePairs ) throws CloudException, InternalException{
    	APITrace.begin(getProvider(),  "Cloud.updateTags");
    	try {
    		try {
    			StringBuilder body = new StringBuilder();

    			if(action.equals(S3Action.PUT_BUCKET_TAG)) {
    				body.append("<Tagging>");
    				body.append("<TagSet>"); 

    				for (int i = 0; i < keyValuePairs.length; i++) {
    					body.append("<Tag>");
    					body.append("<Key>").append(keyValuePairs[i].getKey()).append("</Key>");
    					body.append("<Value>").append(keyValuePairs[i].getValue() != null ? keyValuePairs[i].getValue() : "" ).append("</Value>");
    					body.append("</Tag>");
    				}

    				body.append("</TagSet>");
    				body.append("</Tagging>");
    			}
    			else body = null; // DELETE_BUCKET_TAG

    			String ct = ( body == null ? null : "text/xml; charset=utf-8" );
    			S3Method method = new S3Method(getProvider(), action, null, null, ct, body == null ? null : body.toString());
    			try {
    				method.invoke(bucketName, "?tagging");
    				return;
    			}
    			catch( S3Exception e ) {
    				if( attempt > MAX_RETRIES ) {
    					logger.error("S3 error setting tags for " + bucketName + ": " + e.getSummary());
    					return;
    				}
    				try { Thread.sleep(5000L); } 
    				catch( InterruptedException ignore ) { }
    				logger.warn("Retry attempt "+ (attempt + 1) + " to create tags for ["+bucketName+"]");
    				updateTags( attempt + 1, bucketName, action, keyValuePairs);
    			} 
    		}catch( Throwable ignore ) {
    			logger.error("Error while creating tags for " + bucketName + ".", ignore);
    		}
    	}
    	finally {
    		APITrace.end();
    	}
    }

    private List<Tag> getTags ( String bucketName )throws CloudException, InternalException {
    	APITrace.begin(getProvider(),  "Cloud.getTags");
    	try {
    		try {
    			S3Method method = new S3Method(getProvider(), S3Action.GET_BUCKET_TAG, null, null);
    			S3Response response = method.invoke(bucketName, "?tagging");
    			List<Tag> tags = new ArrayList<Tag>();
    			String key = null , val = null;

    			NodeList blocks = response.document.getElementsByTagName("Tag");

    			for( int i = 0; i < blocks.getLength(); i++ ) {
    				Node object = blocks.item(i);
    				NodeList child = object.getChildNodes();
    				for( int j = 0; j < child.getLength(); j++ ) {
    					Node attr = child.item(j);
    					if( attr.getNodeName().equals("Key") ) 
    						key = attr.getFirstChild().getNodeValue().trim();
    					if( attr.getNodeName().equals("Value") ) 
    						val = attr.getFirstChild().getNodeValue().trim();    
    				}
    				if (key != null) tags.add(new Tag(key, val != null ? val : ""));
    			}
    			return tags;
    		}
    		catch( S3Exception e ) {
    			logger.error(e.getSummary());
    		}   
    		return null;
    	}
    	finally {
    		APITrace.end();
    	}
    }

    private String getRegion( @Nonnull String bucket, boolean reload ) throws CloudException, InternalException {
        ProviderContext ctx = getProvider().getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        Cache<Affinity> cache = Cache.getInstance(getProvider(), "affinity", Affinity.class, CacheLevel.CLOUD_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
        Iterable<Affinity> affinities = cache.get(ctx);
        Affinity affinity;

        if( affinities == null ) {
            affinity = new Affinity();
            cache.put(ctx, Collections.singletonList(affinity));
        }
        else {
            affinity = affinities.iterator().next();
        }
        Constraint c = affinity.constraints.get(bucket);

        if( reload || c == null || c.timeout <= System.currentTimeMillis() ) {
            S3Method method = new S3Method(getProvider(), S3Action.LOCATE_BUCKET);
            String location = null;
            S3Response response;

            method = new S3Method(getProvider(), S3Action.LOCATE_BUCKET);
            try {
                response = method.invoke(bucket, "?location");
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
            c = new Constraint(toRegion(location));
            affinity.constraints.put(bucket, c);
        }
        return c.regionId;
    }

    @Override
    public Blob getBucket( @Nonnull String bucketName ) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Blob.getBucket");
        try {
            if( bucketName.contains("/") ) {
                return null;
            }
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            String regionId = ctx.getRegionId();

            if( regionId == null ) {
                throw new CloudException("No region was set for this request");
            }
            S3Method method = new S3Method(getProvider(), S3Action.LIST_BUCKETS);
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
            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node object = blocks.item(i);
                String name = null;
                NodeList attrs;
                long ts = 0L;

                attrs = object.getChildNodes();
                for( int j = 0; j < attrs.getLength(); j++ ) {
                    Node attr = attrs.item(j);

                    if( attr.getNodeName().equals("Name") ) {
                        name = attr.getFirstChild().getNodeValue().trim();
                    }
                    else if( attr.getNodeName().equals("CreationDate") ) {
                        ts = getProvider().parseTime(attr.getFirstChild().getNodeValue().trim());
                    }
                }
                if( !bucketName.equals(name) ) {
                    continue;
                }
                if( getProvider().getEC2Provider().isAWS() ) {
                    if( getRegion(name, true).equals(regionId) ) {
                        return Blob.getInstance(regionId, getLocation(name, null), name, ts);
                    }
                }
                else {
                    return Blob.getInstance(regionId, getLocation(name, null), name, ts);
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Blob getObject( @Nullable String bucketName, @Nonnull String objectName ) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Blob.getObject");
        try {
            if( bucketName == null ) {
                return null;
            }
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            String regionId = ctx.getRegionId();

            if( regionId == null ) {
                throw new CloudException("No region was set for this request");
            }
            String myRegion = getRegion(bucketName, false);

            if( !myRegion.equals(regionId) ) {
                return null;
            }

            HashMap<String, String> parameters = new HashMap<String, String>();
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
                method = new S3Method(getProvider(), S3Action.LIST_CONTENTS, parameters, null);
                try {
                    response = method.invoke(bucketName, null);
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
                for( int i = 0; i < blocks.getLength(); i++ ) {
                    Node object = blocks.item(i);
                    Storage<org.dasein.util.uom.storage.Byte> size = null;
                    String name = null;
                    long ts = -1L;

                    if( object.hasChildNodes() ) {
                        NodeList attrs = object.getChildNodes();

                        for( int j = 0; j < attrs.getLength(); j++ ) {
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
                    if( !objectName.equals(name) || size == null ) {
                        continue;
                    }
                    return Blob.getInstance(regionId, getLocation(bucketName, name), bucketName, name, ts, size);
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public String getSignedObjectUrl( @Nonnull String bucket, @Nonnull String object, @Nonnull String expiresEpochInSeconds ) throws InternalException, CloudException {
        String signedUrl;
        try {
            SecretKeySpec signingKey = new SecretKeySpec(getProvider().getAccessKey()[1], HMAC_SHA1_ALGORITHM);
            Mac mac = Mac.getInstance(HMAC_SHA1_ALGORITHM);
            mac.init(signingKey);
            String data = "GET\n\n\n" + expiresEpochInSeconds + "\n/" + bucket + "/" + object;
            byte[] rawHmac = mac.doFinal(data.getBytes());
            String signature = URLEncoder.encode(DatatypeConverter.printBase64Binary(rawHmac), "UTF-8");
            signedUrl = "https://" + bucket + ".s3.amazonaws.com/" + object + "?AWSAccessKeyId=" +
                    new String(getProvider().getAccessKey()[0], "UTF-8") + "&Signature=" + signature + "&Expires=" + expiresEpochInSeconds;
        }
        catch( NullPointerException e ) {
            logger.error(e);
            e.printStackTrace();
            throw new InternalException(e);
        }
        catch( NoSuchAlgorithmException e ) {
            logger.error(e);
            e.printStackTrace();
            throw new InternalException(e);
        }
        catch( InvalidKeyException e ) {
            logger.error(e);
            e.printStackTrace();
            throw new InternalException(e);
        }
        catch( UnsupportedEncodingException e ) {
            logger.error(e);
            e.printStackTrace();
            throw new InternalException(e);
        }
        return signedUrl;
    }

    private boolean belongsToAnother( @Nonnull String bucketName ) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Blob.belongsToAnother");
        try {
            S3Method method = new S3Method(getProvider(), S3Action.LOCATE_BUCKET);

            try {
                method.invoke(bucketName, "?location");
                return false;
            }
            catch( S3Exception e ) {
                if( e.getStatus() != HttpStatus.SC_NOT_FOUND ) {
                    String code = e.getCode();

                    if( e.getStatus() == HttpStatus.SC_FORBIDDEN ) {
                        return true;
                    }
                    if( code == null || !code.equals("NoSuchBucket") ) {
                        String message = e.getMessage();

                        if( message != null ) {
                            if( message.contains("Access forbidden") ) {
                                return true;
                            }
                        }
                        logger.error(e.getSummary() + " (" + e.getCode() + ")");
                        throw new CloudException(e);
                    }
                }
            }
            return false;
        }
        finally {
            APITrace.end();
        }
    }

    private @Nonnull String getLocation( @Nonnull String bucketName, @Nullable String objectName ) {
        if( objectName == null ) {
            return ( "http://" + bucketName + ".s3.amazonaws.com" );
        }
        return ( "http://" + bucketName + ".s3.amazonaws.com/" + objectName );
    }

    @Override
    public @Nullable Storage<org.dasein.util.uom.storage.Byte> getObjectSize( @Nullable String bucket, @Nullable String object ) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Blob.getObjectSize");
        try {
            if( bucket == null ) {
                throw new CloudException("Requested object size for object in null bucket");
            }
            if( object == null ) {
                return null;
            }
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            if( !getRegion(bucket, false).equals(ctx.getRegionId()) ) {
                return null;
            }
            S3Method method = new S3Method(getProvider(), S3Action.OBJECT_EXISTS);
            S3Response response;

            try {
                response = method.invoke(bucket, object);
                if( response != null && response.headers != null ) {
                    for( Header header : response.headers ) {
                        if( header.getName().equalsIgnoreCase("Content-Length") ) {
                            return new Storage<org.dasein.util.uom.storage.Byte>(Long.parseLong(header.getValue()), Storage.BYTE);
                        }
                    }
                }
                return null;
            }
            catch( S3Exception e ) {
                if( e.getStatus() != HttpStatus.SC_NOT_FOUND ) {
                    String code = e.getCode();

                    if( code == null || ( !code.equals("NoSuchBucket") && !code.equals("NoSuchKey") ) ) {
                        logger.error(e.getSummary());
                        throw new CloudException(e);
                    }
                }
                return null;
            }
        }
        finally {
            APITrace.end();
        }
    }

    private @Nonnull String findFreeName( @Nonnull String bucket ) throws InternalException, CloudException {
        ProviderContext ctx = getProvider().getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        int idx = bucket.lastIndexOf(".");
        String prefix, rawName;

        if( idx == -1 ) {
            prefix = null;
            rawName = bucket;
            bucket = rawName;
        }
        else {
            prefix = bucket.substring(0, idx);
            rawName = bucket.substring(idx + 1);
            bucket = prefix + "." + rawName;
        }
        while( belongsToAnother(bucket) || ( exists(bucket) && !getRegion(bucket, false).equals(ctx.getRegionId()) ) ) {
            idx = rawName.lastIndexOf("-");
            if( idx == -1 ) {
                rawName = rawName + "-1";
            }
            else if( idx == rawName.length() - 1 ) {
                rawName = rawName + "1";
            }
            else {
                String postfix = rawName.substring(idx + 1);
                int x;

                try {
                    x = Integer.parseInt(postfix) + 1;
                    rawName = rawName.substring(0, idx) + "-" + x;
                }
                catch( NumberFormatException e ) {
                    rawName = rawName + "-1";
                }
            }
            if( prefix == null ) {
                bucket = rawName;
            }
            else {
                bucket = prefix + "." + rawName;
            }
        }
        return bucket;
    }

    @Override
    protected void get( @Nullable String bucket, @Nonnull String object, @Nonnull File toFile, @Nullable FileTransfer transfer ) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Blob.get");
        try {
            if( bucket == null ) {
                throw new CloudException("No bucket was specified");
            }
            IOException lastError = null;
            int attempts = 0;

            while( attempts < 5 ) {
                S3Method method = new S3Method(getProvider(), S3Action.GET_OBJECT);
                S3Response response;

                try {
                    response = method.invoke(bucket, object);
                    try {
                        copy(response.input, new FileOutputStream(toFile), transfer);
                        return;
                    }
                    catch( FileNotFoundException e ) {
                        logger.error(e);
                        e.printStackTrace();
                        throw new InternalException(e);
                    }
                    catch( IOException e ) {
                        lastError = e;
                        logger.warn(e);
                        try {
                            Thread.sleep(10000L);
                        }
                        catch( InterruptedException ignore ) {
                        }
                    }
                    finally {
                        response.close();
                    }
                }
                catch( S3Exception e ) {
                    logger.error(e.getSummary());
                    throw new CloudException(e);
                }
                attempts++;
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

    private @Nullable Document getAcl( @Nonnull String bucket, @Nullable String object ) throws CloudException, InternalException {
        S3Method method;

        method = new S3Method(getProvider(), S3Action.GET_ACL);
        try {
            S3Response response = method.invoke(bucket, object == null ? "?acl" : object + "?acl");

            return ( response == null ? null : response.document );
        }
        catch( S3Exception e ) {
            logger.error(e.getSummary());
            throw new CloudException(e);
        }
    }

    @Override
    public boolean isPublic( @Nullable String bucket, @Nullable String object ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Blob.isPublic");
        try {
            if( bucket == null ) {
                throw new CloudException("A bucket name was not specified");
            }
            Document acl = getAcl(bucket, object);

            if( acl == null ) {
                return false;
            }
            NodeList grants;

            grants = acl.getElementsByTagName("Grant");
            for( int i = 0; i < grants.getLength(); i++ ) {
                boolean isAll = false, isRead = false;
                Node grant = grants.item(i);
                NodeList grantData;

                grantData = grant.getChildNodes();
                for( int j = 0; j < grantData.getLength(); j++ ) {
                    Node item = grantData.item(j);

                    if( item.getNodeName().equals("Grantee") ) {
                        String type = item.getAttributes().getNamedItem("xsi:type").getNodeValue();

                        if( type.equals("Group") ) {
                            NodeList items = item.getChildNodes();

                            for( int k = 0; k < items.getLength(); k++ ) {
                                Node n = items.item(k);

                                if( n.getNodeName().equals("URI") ) {
                                    if( n.hasChildNodes() ) {
                                        String uri = n.getFirstChild().getNodeValue();

                                        if( uri.equals("http://acs.amazonaws.com/groups/global/AllUsers") ) {
                                            isAll = true;
                                            break;
                                        }
                                    }
                                }
                                if( isAll ) {
                                    break;
                                }
                            }
                        }
                    }
                    else if( item.getNodeName().equals("Permission") ) {
                        if( item.hasChildNodes() ) {
                            String perm = item.getFirstChild().getNodeValue();

                            isRead = ( perm.equals("READ") || perm.equals("FULL_CONTROL") );
                        }
                    }
                }
                if( isAll ) {
                    return isRead;
                }
            }
            return false;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Blob.isSubscribed");
        try {
            S3Method method = new S3Method(getProvider(), S3Action.LIST_BUCKETS);

            try {
                method.invoke(null, null);
                return true;
            }
            catch( S3Exception e ) {
                return false;
            }
        }
        finally {
            APITrace.end();
        }
    }

    /*
    private boolean isLocation(@Nonnull String bucket) throws CloudException, InternalException {
        S3Method method = new S3Method(getProvider(), S3Action.LOCATE_BUCKET);
        ProviderContext ctx = getProvider().getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        if( !getProvider().getEC2Provider().isAWS() ) {
            return true;
        }
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            return false;
        }
        S3Response response;

        try {
            response = method.invoke(bucket, "?location");
        }
        catch( S3Exception e ) {
            if( e.getStatus() == HttpServletResponse.SC_NOT_FOUND ) {
                response = null;
            }
            else {
                String code = e.getCode();

                if( code == null || !code.equals("NoSuchBucket") ) {
                    logger.error(e.getStatus() + "/" + code + ": " + e.getSummary());
                    throw new CloudException(e);
                }
                response = null;
            }
        }
        if( response != null ) {
            NodeList constraints = response.document.getElementsByTagName("LocationConstraint");
            if( constraints.getLength() > 0 ) {
                Node constraint = constraints.item(0);

                if( constraint != null && constraint.hasChildNodes() ) {
                    String location = constraint.getFirstChild().getNodeValue().trim();

                    if( location.equals("EU") && !regionId.equals("eu-west-1") ) {
                        return false;
                    }
                    else if( location.equals("us-west-1") && !regionId.equals("us-west-1") ) {
                        return false;
                    }
                    else if( location.startsWith("ap-") && !regionId.equals(location) ) {
                        return false;
                    }
                    else if( location.equals("US") && !regionId.equals("us-east-1") ) {
                        return false;
                    }
                }
                else {
                    return regionId.equals("us-east-1");
                }
            }
            else {
                return regionId.equals("us-east-1");
            }
        }
        return true;
    }
    */

    @Override
    public @Nonnull Collection<Blob> list( final @Nullable String bucket ) throws CloudException, InternalException {
        final ProviderContext ctx = getProvider().getContext();
        PopulatorThread<Blob> populator;

        if( ctx == null ) {
            throw new CloudException("No context was specified for this request");
        }
        final String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region ID was specified");
        }
        if( bucket != null && !getRegion(bucket, false).equals(regionId) ) {
            throw new CloudException("No such bucket in target region: " + bucket + " in " + regionId);
        }
        getProvider().hold();
        populator = new PopulatorThread<Blob>(new JiteratorPopulator<Blob>() {
            public void populate( @Nonnull Jiterator<Blob> iterator ) throws CloudException, InternalException {
                try {
                    list(regionId, bucket, iterator);
                }
                finally {
                    getProvider().release();
                }
            }
        });
        populator.populate();
        return populator.getResult();
    }

    private void list( @Nonnull String regionId, @Nullable String bucket, @Nonnull Jiterator<Blob> iterator ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Blob.list");
        try {
            if( bucket == null ) {
                loadBuckets(regionId, iterator);
            }
            else {
                loadObjects(regionId, bucket, iterator);
            }
        }
        finally {
            APITrace.end();
        }
    }

    private @Nonnull String toRegion( @Nullable String locationConstraint ) {
        if( locationConstraint == null ) {
            return "us-east-1";
        }
        else if( locationConstraint.equals("EU") ) {
            return "eu-west-1";
        }
        else if( locationConstraint.equals("US") ) {
            return "us-east-1";
        }
        return locationConstraint;
    }

    private void loadBuckets( @Nonnull String regionId, @Nonnull Jiterator<Blob> iterator ) throws CloudException, InternalException {
        S3Method method = new S3Method(getProvider(), S3Action.LIST_BUCKETS);
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
        for( int i = 0; i < blocks.getLength(); i++ ) {
            Node object = blocks.item(i);
            String name = null;
            NodeList attrs;
            long ts = 0L;

            attrs = object.getChildNodes();
            for( int j = 0; j < attrs.getLength(); j++ ) {
                Node attr = attrs.item(j);

                if( attr.getNodeName().equals("Name") ) {
                    name = attr.getFirstChild().getNodeValue().trim();
                }
                else if( attr.getNodeName().equals("CreationDate") ) {
                    ts = getProvider().parseTime(attr.getFirstChild().getNodeValue().trim());
                }
            }
            if( name == null ) {
                throw new CloudException("Bad response from server.");
            }
            if( getProvider().getEC2Provider().isAWS() ) {
                String location = null;

                method = new S3Method(getProvider(), S3Action.LOCATE_BUCKET);
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
    }

    private void loadObjects( @Nonnull String regionId, @Nonnull String bucket, @Nonnull Jiterator<Blob> iterator ) throws CloudException, InternalException {
        HashMap<String, String> parameters = new HashMap<String, String>();
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
            method = new S3Method(getProvider(), S3Action.LIST_CONTENTS, parameters, null);
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
            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node object = blocks.item(i);
                Storage<org.dasein.util.uom.storage.Byte> size = null;
                String name = null;
                long ts = -1L;

                if( object.hasChildNodes() ) {
                    NodeList attrs = object.getChildNodes();

                    for( int j = 0; j < attrs.getLength(); j++ ) {
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
    }

    @Override
    public void makePublic( @Nonnull String bucket ) throws InternalException, CloudException {
        makePublic(bucket, null);
    }

    @Override
    public void makePublic( @Nullable String bucket, @Nullable String object ) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Blob.makePublic");
        try {
            if( bucket == null ) {
                throw new CloudException("No bucket was specified for this request");
            }
            Document current = getAcl(bucket, object);

            if( current == null ) {
                throw new CloudException("Target does not exist");
            }
            StringBuilder xml = new StringBuilder();
            NodeList blocks;

            blocks = current.getDocumentElement().getChildNodes();
            xml.append("<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node n = blocks.item(i);

                if( n.getNodeName().equals("Owner") ) {
                    NodeList attrs = n.getChildNodes();

                    xml.append("<Owner>");
                    for( int j = 0; j < attrs.getLength(); j++ ) {
                        Node attr = attrs.item(j);

                        if( attr.getNodeName().equals("ID") ) {
                            xml.append("<ID>");
                            xml.append(attr.getFirstChild().getNodeValue().trim());
                            xml.append("</ID>");
                        }
                        else if( attr.getNodeName().equals("DisplayName") ) {
                            xml.append("<DisplayName>");
                            xml.append(attr.getFirstChild().getNodeValue().trim());
                            xml.append("</DisplayName>");
                        }
                    }
                    xml.append("</Owner>");
                }
                else if( n.getNodeName().equals("AccessControlList") ) {
                    NodeList attrs = n.getChildNodes();
                    boolean found = false;

                    xml.append("<AccessControlList>");
                    for( int j = 0; j < attrs.getLength(); j++ ) {
                        Node attr = attrs.item(j);

                        if( attr.getNodeName().equals("Grant") ) {
                            NodeList subList = attr.getChildNodes();
                            boolean isAll = false;

                            xml.append("<Grant>");
                            for( int k = 0; k < subList.getLength(); k++ ) {
                                Node sub = subList.item(k);

                                if( sub.getNodeName().equals("Grantee") ) {
                                    String type = sub.getAttributes().getNamedItem("xsi:type").getNodeValue();
                                    NodeList agentInfo = sub.getChildNodes();

                                    xml.append("<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"");
                                    xml.append(type);
                                    xml.append("\">");
                                    for( int l = 0; l < agentInfo.getLength(); l++ ) {
                                        Node item = agentInfo.item(l);

                                        xml.append("<");
                                        xml.append(item.getNodeName());
                                        if( item.hasChildNodes() ) {
                                            String val = item.getFirstChild().getNodeValue();

                                            if( type.equals("Group") && item.getNodeName().equals("URI") && val.equals("http://acs.amazonaws.com/groups/global/AllUsers") ) {
                                                found = true;
                                                isAll = true;
                                            }
                                            xml.append(">");
                                            xml.append(item.getFirstChild().getNodeValue());
                                            xml.append("</");
                                            xml.append(item.getNodeName());
                                            xml.append(">");
                                        }
                                        else {
                                            xml.append("/>");
                                        }
                                    }
                                    xml.append("</Grantee>");
                                }
                                else if( sub.getNodeName().equals("Permission") ) {
                                    if( isAll ) {
                                        xml.append("<Permission>READ</Permission>");
                                    }
                                    else {
                                        xml.append("<Permission");
                                        if( sub.hasChildNodes() ) {
                                            xml.append(">");
                                            xml.append(sub.getFirstChild().getNodeValue());
                                            xml.append("</Permission>");
                                        }
                                        else {
                                            xml.append("/>");
                                        }
                                    }
                                }
                            }
                            xml.append("</Grant>");
                        }
                    }
                    if( !found ) {
                        xml.append("<Grant>");
                        xml.append("<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\">");
                        xml.append("<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>");
                        xml.append("</Grantee>");
                        xml.append("<Permission>READ</Permission>");
                        xml.append("</Grant>");
                    }
                    xml.append("</AccessControlList>");
                }
            }
            xml.append("</AccessControlPolicy>\r\n");
            setAcl(bucket, object, xml.toString());
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction( @Nonnull ServiceAction action ) {
        if( action.equals(BlobStoreSupport.ANY) ) {
            return new String[]{S3Method.S3_PREFIX + "*"};
        }
        else if( action.equals(BlobStoreSupport.CREATE_BUCKET) ) {
            return new String[]{S3Method.S3_PREFIX + "CreateBucket"};
        }
        else if( action.equals(BlobStoreSupport.DOWNLOAD) ) {
            return new String[]{S3Method.S3_PREFIX + "GetObject"};
        }
        else if( action.equals(BlobStoreSupport.GET_BUCKET) ) {
            return new String[]{S3Method.S3_PREFIX + "GetBucket"};
        }
        else if( action.equals(BlobStoreSupport.LIST_BUCKET) ) {
            return new String[]{S3Method.S3_PREFIX + "ListBucket"};
        }
        else if( action.equals(BlobStoreSupport.LIST_BUCKET_CONTENTS) ) {
            return new String[]{S3Method.S3_PREFIX + "ListBucket"};
        }
        else if( action.equals(BlobStoreSupport.MAKE_PUBLIC) ) {
            return new String[]{S3Method.S3_PREFIX + "PutAccessControlPolicy"};
        }
        else if( action.equals(BlobStoreSupport.REMOVE_BUCKET) ) {
            return new String[]{S3Method.S3_PREFIX + "DeleteBucket"};
        }
        else if( action.equals(BlobStoreSupport.UPLOAD) ) {
            return new String[]{S3Method.S3_PREFIX + "PutObject"};
        }
        return new String[0];
    }

    @Override
    public void move( @Nullable String sourceBucket, @Nullable String object, @Nullable String targetBucket ) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Blob.move");
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
    protected void put( @Nullable String bucket, @Nonnull String object, @Nonnull File file ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Blob.putFile");
        try {
            boolean bucketIsPublic = isPublic(bucket, null);
            HashMap<String, String> headers = null;
            S3Method method;

            if( bucketIsPublic ) {
                headers = new HashMap<String, String>();
                headers.put("x-amz-acl", "public-read");
            }
            method = new S3Method(getProvider(), S3Action.PUT_OBJECT, null, headers, "application/octet-stream", file);
            try {
                method.invoke(bucket, object);
            }
            catch( S3Exception e ) {
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    protected void put( @Nullable String bucket, @Nonnull String object, @Nonnull String content ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Blob.putString");
        try {
            boolean bucketIsPublic = isPublic(bucket, null);
            HashMap<String, String> headers = null;
            S3Method method;

            if( bucketIsPublic ) {
                headers = new HashMap<String, String>();
                headers.put("x-amz-acl", "public-read");
            }
            File file = null;
            try {
                try {
                    file = File.createTempFile(object, ".txt");
                    PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file)));
                    writer.print(content);
                    writer.flush();
                    writer.close();
                }
                catch( IOException e ) {
                    logger.error(e);
                    e.printStackTrace();
                    throw new InternalException(e);
                }
                method = new S3Method(getProvider(), S3Action.PUT_OBJECT, null, headers, "text/plain", file);
                try {
                    method.invoke(bucket, object);
                }
                catch( S3Exception e ) {
                    logger.error(e.getSummary());
                    throw new CloudException(e);
                }
            }
            finally {
                if( file != null ) {
                    //noinspection ResultOfMethodCallIgnored
                    file.delete();
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeBucket( @Nonnull String bucket ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Blob.removeBucket");
        try {
            S3Method method = new S3Method(getProvider(), S3Action.DELETE_BUCKET);

            try {
                method.invoke(bucket, null);
            }
            catch( S3Exception e ) {
                String code = e.getCode();

                if( code != null && ( code.equals("NoSuchBucket") ) ) {
                    return;
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeObject( @Nullable String bucket, @Nonnull String name ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Blob.removeObject");
        try {
            if( bucket == null ) {
                throw new CloudException("No bucket was specified for this request");
            }
            S3Method method = new S3Method(getProvider(), S3Action.DELETE_OBJECT);

            try {
                method.invoke(bucket, name);
            }
            catch( S3Exception e ) {
                String code = e.getCode();

                if( code != null && ( code.equals("NoSuchBucket") || code.equals("NoSuchKey") ) ) {
                    return;
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String renameBucket( @Nonnull String oldName, @Nonnull String newName, boolean findFreeName ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Blob.renameBucket");
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
                    try {
                        Thread.sleep(retries * 10000L);
                    }
                    catch( InterruptedException ignore ) {
                    }
                }
            }
            boolean ok = true;
            for( Blob file : list(oldName) ) {
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
    public void renameObject( @Nullable String bucket, @Nonnull String object, @Nonnull String newName ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Blob.renameObject");
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

    private void setAcl( @Nonnull String bucket, @Nullable String object, @Nonnull String body ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Blob.setAcl");
        try {
            //String ct = "text/xml; charset=utf-8";
            S3Method method;

            method = new S3Method(getProvider(), S3Action.SET_ACL, null, null, null /* ct */, body);
            try {
                method.invoke(bucket, object == null ? "?acl" : object + "?acl");
            }
            catch( S3Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Blob upload( @Nonnull File source, @Nullable String bucket, @Nonnull String fileName ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Blob.upload");
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
    public void updateTags(@Nonnull String bucketName, @Nonnull Tag ... tags) throws CloudException, InternalException {
    	APITrace.begin(getProvider(), "Bucket.updateTags");
    	try {
            List<Tag> tagsList = getTags( bucketName);
    		if (tagsList == null) tagsList = new ArrayList<Tag>();

    		for (int i = 0; i < tags.length ; i++ )
    			tagsList.add(new Tag (tags[i].getKey(), tags[i].getValue()));
    		updateTags(1, bucketName, S3Action.PUT_BUCKET_TAG, tagsList.toArray(new Tag[tagsList.size()]));
    	}
    	finally {
    		APITrace.end();
    	}
    }

    @Override
    public void updateTags(@Nonnull String[] bucketNames, @Nonnull Tag ... tags) throws CloudException, InternalException {
    	for( String id : bucketNames ) {
    		updateTags(id, tags);
    	}
    }

    @Override
    public void removeTags(@Nonnull String bucketName, @Nonnull Tag ... tags) throws CloudException, InternalException {
    	APITrace.begin(getProvider(), "Bucket.removeTags");
    	try {
    		List<Tag> existTags = getTags( bucketName);

    		if (existTags != null) 
    			for (int i = 0; i < tags.length ; i++ )
    				for (int j = 0; j < existTags.size(); j++ ){
    					if (tags[i].getKey().equals(existTags.get(j).getKey())){
    						existTags.remove(j);
    						break;
    					}
    				}
    		updateTags(1, bucketName, S3Action.DELETE_BUCKET_TAG);
    		updateTags(1, bucketName, S3Action.PUT_BUCKET_TAG, existTags.toArray(new Tag[existTags.size()]));
    	}
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeTags(@Nonnull String[] bucketNames, @Nonnull Tag ... tags) throws CloudException, InternalException {
    	for( String id : bucketNames ) {
    		removeTags(id, tags);
    	}
    }

    static private class Constraint {
        public String regionId;
        public long   timeout;

        public Constraint( String regionId ) {
            this.regionId = regionId;
            this.timeout = System.currentTimeMillis() + ( CalendarWrapper.MINUTE * 30L ) + random.nextInt(( int ) ( CalendarWrapper.MINUTE * 5L ));
        }
    }

    static private class Affinity {
        public HashMap<String, Constraint> constraints = new HashMap<String, Constraint>();
    }
}
