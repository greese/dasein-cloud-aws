/**
 * Copyright (C) 2009-2012 enStratus Networks Inc
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

package org.dasein.cloud.aws;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.TreeSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.dasein.cloud.AbstractCloud;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Tag;
import org.dasein.cloud.Taggable;
import org.dasein.cloud.aws.admin.AWSAdminServices;
import org.dasein.cloud.aws.compute.EC2ComputeServices;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.aws.compute.EC2Method;
import org.dasein.cloud.aws.identity.AWSIdentityServices;
import org.dasein.cloud.aws.network.EC2NetworkServices;
import org.dasein.cloud.aws.platform.AWSPlatformServices;
import org.dasein.cloud.aws.storage.AWSCloudStorageServices;
import org.dasein.cloud.compute.ComputeServices;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.storage.BlobStoreSupport;
import org.dasein.cloud.storage.StorageServices;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class AWSCloud extends AbstractCloud {
    static private String getLastItem(String name) {
        int idx = name.lastIndexOf('.');
        
        if( idx < 0 ) {
            return name;
        }
        else if( idx == (name.length()-1) ) {
            return "";
        }
        return name.substring(idx+1);
    }
    
    static public Logger getLogger(Class<?> cls) {
        String pkg = getLastItem(cls.getPackage().getName());
        
        if( pkg.equals("aws") ) {
            pkg = "";
        }
        else {
            pkg = pkg + ".";
        }
        return Logger.getLogger("dasein.cloud.aws.std." + pkg + getLastItem(cls.getName()));
    }
    
    static public Logger getWireLogger(Class<?> cls) {
        return Logger.getLogger("dasein.cloud.aws.wire." + getLastItem(cls.getPackage().getName()) + "." + getLastItem(cls.getName()));
    }
    
	static private final Logger logger = getLogger(AWSCloud.class);
	
	static public final String P_ACCESS            = "AWSAccessKeyId";
	static public final String P_ACTION            = "Action";
	static public final String P_CFAUTH            = "Authorization";
	static public final String P_AWS_DATE          = "x-amz-date";
    static public final String P_GOOG_DATE         = "x-goog-date";
	static public final String P_SIGNATURE         = "Signature";
	static public final String P_SIGNATURE_METHOD  = "SignatureMethod";
	static public final String P_SIGNATURE_VERSION = "SignatureVersion";
	static public final String P_TIMESTAMP         = "Timestamp";
	static public final String P_VERSION           = "Version";
	
	static public final String CLOUD_FRONT_ALGORITHM = "HmacSHA1";
    static public final String EC2_ALGORITHM         = "HmacSHA256";
	static public final String S3_ALGORITHM          = "HmacSHA1";
    static public final String SIGNATURE             = "2";

    static public String encode(String value, boolean encodePath) throws InternalException {
        String encoded;
        
        try {
            encoded = URLEncoder.encode(value, "utf-8").replace("+", "%20").replace("*", "%2A").replace("%7E","~");
            if( encodePath ) {
                encoded = encoded.replace("%2F", "/");
            }
        } 
        catch( UnsupportedEncodingException e ) {
        	logger.error(e);
        	e.printStackTrace();
        	throw new InternalException(e);
        }
        return encoded;    	
    }
    
    static public String escapeXml(String nonxml) {
        StringBuilder str = new StringBuilder();
        
        for( int i=0; i<nonxml.length(); i++ ) {
            char c = nonxml.charAt(i);
            
            switch( c ) {
                case '&': str.append("&amp;"); break;
                case '>': str.append("&gt;"); break;
                case '<': str.append("&lt;"); break;
                case '"': str.append("&quot;"); break;
                case '[': str.append("&#091;"); break;
                case ']': str.append("&#093;"); break;
                case '!': str.append("&#033;"); break;
                default: str.append(c);
            }
        }
        return str.toString();
    }
    
	public AWSCloud() { }

    private String buildEc2AuthString(String method, String serviceUrl, Map<String, String> parameters) throws InternalException {
    	StringBuilder authString = new StringBuilder();
    	TreeSet<String> sortedKeys;
    	URI endpoint;
    	String tmp;
    	
    	authString.append(method);
    	authString.append("\n");
		try {
		    endpoint = new URI(serviceUrl);
		} 
		catch( URISyntaxException e ) {
			logger.error(e);
			e.printStackTrace();
			throw new InternalException(e);
		}
		authString.append(endpoint.getHost().toLowerCase());
		authString.append("\n");
		tmp = endpoint.getPath();
		if( tmp == null || tmp.length() == 0) {
		    tmp = "/";
		}
		authString.append(encode(tmp, true));
		authString.append("\n");
		sortedKeys = new TreeSet<String>();
		sortedKeys.addAll(parameters.keySet());
		boolean first = true;
		for( String key : sortedKeys ) {
			String value = parameters.get(key);
			
			if( !first ) {
				authString.append("&");
			}
			else {
				first = false;
			}
			authString.append(encode(key, false));
			authString.append("=");
			authString.append(encode(value, false));
		}
		return authString.toString();
    }


    public boolean createTags(String resourceId, Tag... keyValuePairs) {
        return createTags(new String[]{resourceId}, keyValuePairs);
    }

    public boolean createTags(String[] resourceIds, Tag... keyValuePairs) {
        try {
            Map<String,String> parameters = getStandardParameters(getContext(), "CreateTags");
            EC2Method method;

            for (int i = 0; i < resourceIds.length; i++) {
                parameters.put("ResourceId." + (i + 1), resourceIds[i]);
            }

            Map<String,String> tagParameters = new HashMap<String, String>( );
            for( int i=0; i<keyValuePairs.length; i++ ) {
                String key = keyValuePairs[i].getKey();
                String value = keyValuePairs[i].getValue();

                if ( value != null ) {
                    tagParameters.put("Tag." + (i + 1) + ".Key", key);
                    tagParameters.put("Tag." + (i + 1) + ".Value", value);
                }
            }
            if ( tagParameters.size() == 0 ) {
                return true;
            }
            putExtraParameters( parameters, tagParameters );
            method = new EC2Method(this, getEc2Url(), parameters);
            try {
                method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && code.equals("InvalidInstanceID.NotFound") ) {
                    try { Thread.sleep(5000L); }
                    catch( InterruptedException ignore ) { }
                    parameters = getStandardParameters(getContext(), "CreateTags");

                    for (int i = 0; i < resourceIds.length; i++) {
                        parameters.put("ResourceId." + (i + 1), resourceIds[i]);
                    }

                    for( int i=0; i<keyValuePairs.length; i++ ) {
                        String key = keyValuePairs[i].getKey();
                        String value = keyValuePairs[i].getValue();

                        parameters.put("Tag." + (i + 1) + ".Key", key);
                        parameters.put("Tag." + (i + 1) + ".Value", value);
                    }
                    method = new EC2Method(this, getEc2Url(), parameters);
                    try {
                        method.invoke();
                        return true;
                    }
                    catch( EC2Exception ignore ) {
                        // ignore me
                    }
                }
                logger.error("EC2 error settings tags for " + resourceIds + ": " + e.getSummary());
                return false;
            }
            return true;
        }
        catch( Throwable ignore ) {
            logger.error("Error while creating tags for " + resourceIds + ".", ignore);
            return false;
        }
    }

    public boolean removeTags(String resourceId, Tag... keyValuePairs) {
        return removeTags(new String[]{resourceId}, keyValuePairs);
    }

    public boolean removeTags(String[] resourceIds, Tag... keyValuePairs) {
        try {
            Map<String, String> parameters = getStandardParameters(getContext(), "DeleteTags");
            EC2Method method;

            for (int i = 0; i < resourceIds.length; i++) {
                parameters.put("ResourceId." + (i + 1), resourceIds[i]);
            }

            for (int i = 0; i < keyValuePairs.length; i++) {
                String key = keyValuePairs[i].getKey();
                String value = keyValuePairs[i].getValue();

                parameters.put("Tag." + (i + 1) + ".Key", key);
                if (value != null) {
                    parameters.put("Tag." + (i + 1) + ".Value", value);
                }
            }
            method = new EC2Method(this, getEc2Url(), parameters);
            method.invoke();
            return true;
        }
        catch (Throwable ignore) {
            logger.error("Error while removing tags for " + resourceIds + ".", ignore);
            return false;
        }
    }

    public Map<String, String> getTagsFromTagSet(Node attr) {
        if ( attr == null || !attr.hasChildNodes() ) {
            return null;
        }
        Map<String, String> tags = new HashMap<String, String>();
        NodeList tagNodes = attr.getChildNodes();
        for ( int j = 0; j < tagNodes.getLength(); j++ ) {
            Node tag = tagNodes.item( j );

            if ( tag.getNodeName().equals( "item" ) && tag.hasChildNodes() ) {
                NodeList parts = tag.getChildNodes();
                String key = null, value = null;

                for ( int k = 0; k < parts.getLength(); k++ ) {
                    Node part = parts.item( k );

                    if ( part.getNodeName().equalsIgnoreCase( "key" ) ) {
                        if ( part.hasChildNodes() ) {
                            key = part.getFirstChild().getNodeValue().trim();
                        }
                    }
                    else if ( part.getNodeName().equalsIgnoreCase( "value" ) ) {
                        if ( part.hasChildNodes() ) {
                            value = part.getFirstChild().getNodeValue().trim();
                        }
                    }
                    if ( key != null && value != null ) {
                        tags.put( key, value );
                    }
                }
            }
        }
        return tags;
    }

    @Override
    public AWSAdminServices getAdminServices() {
        EC2Provider p = getEC2Provider();

        if( p.isAWS() || p.isEnStratus() || p.isOpenStack() || p.isEucalyptus() ) {
            return new AWSAdminServices(this);
        }
        return null;
    }
    
	private @Nonnull String[] getBootstrapUrls(@Nullable ProviderContext ctx) {
	    String endpoint = (ctx == null ? null : ctx.getEndpoint());
        
        if( endpoint == null ) {
            return new String[0];
        }
        if( !endpoint.contains(",") ) {
            return new String[] { endpoint };
        }
        String[] endpoints = endpoint.split(",");

        if( endpoints == null ) {
            endpoints = new String[0];
        }
        if( endpoints.length > 1 ) {
            String second = endpoints[1];
            
            if( !second.startsWith("http") ) {
                if( endpoints[0].startsWith("http") ) {
                    // likely a URL with a , in it
                    return new String[] { endpoint + (getEC2Provider().isEucalyptus() ? "/Eucalyptus" : "") };
                }
            }
        }
        for( int i=0; i<endpoints.length; i++ ) {
            if( !endpoints[i].startsWith("http") ) {
                endpoints[i] = "https://" + endpoints[i] + (getEC2Provider().isEucalyptus() ? "/Eucalyptus" : "");
            }
        }
        return endpoints;
    }
	   
	@Override
	public @Nonnull String getCloudName() {
        ProviderContext ctx = getContext();
		String name = (ctx == null ? null : ctx.getCloudName());
		
		return ((name == null ) ? "AWS" : name);
	}

	@Override
	public EC2ComputeServices getComputeServices() {
        if( getEC2Provider().isStorage() ) {
            return null;
        }
	    return new EC2ComputeServices(this);
	}
	
	@Override
	public @Nonnull RegionsAndZones getDataCenterServices() {
	    return new RegionsAndZones(this);
	}

    private transient volatile EC2Provider provider;

    public @Nonnull EC2Provider getEC2Provider() {
        if( provider == null ) {
            provider = EC2Provider.valueOf(getProviderName());
        }
        return provider;
    }

    public @Nullable String getEc2Url() throws InternalException, CloudException {
        ProviderContext ctx = getContext();
        String url = getEc2Url(ctx == null ? null : ctx.getRegionId());
        
        if( getEC2Provider().isEucalyptus() ) {
            return url + "/Eucalyptus";
        }
        else {
            return url;
        }
    }
    
    @Nullable String getEc2Url(@Nullable String regionId) throws InternalException, CloudException {
        ProviderContext ctx = getContext();
        String url;
        
        if( regionId == null ) {
            return getBootstrapUrls(ctx)[0];
        }
        if( getEC2Provider().isAWS() ) {

            url = (ctx == null ? null : ctx.getEndpoint());
            if( url != null && url.endsWith("amazonaws.com") ) {
                return "https://ec2." + regionId + ".amazonaws.com";
            }
            return "https://ec2." + regionId + ".amazonaws.com";
        }
        else if( !getEC2Provider().isEucalyptus() ) {
            url = (ctx == null ? null : ctx.getEndpoint());
            if( url == null ) {
                return null;
            }
            if( !url.startsWith("http") ) {
                String cloudUrl = ctx.getEndpoint();

                if( cloudUrl != null && cloudUrl.startsWith("http:") ) {
                    return "http://" + url + "/" + regionId;
                }
                return "https://" + url + "/" + regionId;
            }
            else {
                return url + "/" + regionId;
            }
        }
        url = (ctx == null ? null : ctx.getEndpoint());
        if( url == null ) {
            return null;
        }
        if( !url.startsWith("http") ) {
            String cloudUrl = ctx.getEndpoint();
            
            if( cloudUrl != null && cloudUrl.startsWith("http:") ) {
                return "http://" + url;         
            }
            return "https://" + url;
        }
        else {
            return url;
        }
    }

    public String getAutoScaleVersion() {
        return "2009-05-15";
    }

    public String getCloudWatchVersion() {
        return "2009-05-15";
    }

    public String getEc2Version() {
        if( getEC2Provider().isAWS() ) {
            return "2012-07-20";
        }
        else if( getEC2Provider().isEucalyptus() ) {
            return "2010-11-15";
        }
        else if( getEC2Provider().isOpenStack() ) {
            return "2009-11-30";
        }
        return "2012-07-20";
    }

    public String getElbVersion() {
        return "2009-05-15";
    }

    public String getRdsVersion() {
        return "2011-04-01";
    }

    public String getRoute53Version() {
        return "2010-10-01";
    }

    public String getSdbVersion() {
        return "2009-04-15";
    }

    public String getSnsVersion() {
        return "2010-03-31";
    }

    public String getSqsVersion() {
        return "2009-02-01";
    }

	@Override
	public AWSIdentityServices getIdentityServices() {
        if( getEC2Provider().isStorage() ) {
            return null;
        }
        return new AWSIdentityServices(this);
	}
    
	@Override
	public EC2NetworkServices getNetworkServices() {
        if( getEC2Provider().isStorage() ) {
            return null;
        }
        return new EC2NetworkServices(this);
	}
	
	@Override
	public @Nullable AWSPlatformServices getPlatformServices() {
        EC2Provider p = getEC2Provider();

        if( p.isAWS() || p.isEnStratus() ) {
            return new AWSPlatformServices(this);
        }
        return null;
	}
    
	@Override
	public @Nonnull String getProviderName() {
        ProviderContext ctx = getContext();
		String name = (ctx == null ? null : ctx.getProviderName());
		
		return ((name == null) ? EC2Provider.AWS.getName() : name);
	}
	
	public @Nullable String getProxyHost() {
        ProviderContext ctx = getContext();

        if( ctx == null ) {
            return null;
        }
        Properties props = ctx.getCustomProperties();

        return (props == null ? null : props.getProperty("proxyHost"));
	}
	
	public int getProxyPort() {
        ProviderContext ctx = getContext();

        if( ctx == null ) {
            return -1;
        }
        Properties props = ctx.getCustomProperties();

        if( props == null ) {
            return -1;
        }
	    String port = props.getProperty("proxyPort");
	    
	    if( port != null ) {
	        return Integer.parseInt(port);
	    }
	    return -1;
	}
	
	@Override
	public @Nonnull AWSCloudStorageServices getStorageServices() {
	    return new AWSCloudStorageServices(this);
	}
	
	public Map<String,String> getStandardParameters(ProviderContext ctx, String action) throws InternalException {
        return getStandardParameters(ctx, action, getEc2Version());
	}

    public Map<String,String> getStandardParameters(ProviderContext ctx, String action, String version) throws InternalException {
        HashMap<String,String> parameters = new HashMap<String,String>();

        parameters.put(P_ACTION, action);
        parameters.put(P_SIGNATURE_VERSION, SIGNATURE);
        try {
            parameters.put(P_ACCESS, new String(ctx.getAccessPublic(), "utf-8"));
        }
        catch( UnsupportedEncodingException e ) {
            logger.error(e);
            e.printStackTrace();
            throw new InternalException(e);
        }
        parameters.put(P_SIGNATURE_METHOD, EC2_ALGORITHM);
        parameters.put(P_TIMESTAMP, getTimestamp(System.currentTimeMillis(), true));
        parameters.put(P_VERSION, version);
        return parameters;
    }
    
	public Map<String,String> getStandardCloudWatchParameters(ProviderContext ctx, String action) throws InternalException {
        Map<String,String> parameters = getStandardParameters(ctx, action);
        
        parameters.put(P_VERSION, getCloudWatchVersion());
        return parameters;
    }
	   
	public Map<String,String> getStandardRdsParameters(ProviderContext ctx, String action) throws InternalException {
       Map<String,String> parameters = getStandardParameters(ctx, action);
       
       parameters.put(P_VERSION, getRdsVersion());
       return parameters;
	}  
   
	public Map<String,String> getStandardSimpleDBParameters(ProviderContext ctx, String action) throws InternalException {
       Map<String,String> parameters = getStandardParameters(ctx, action);
       
       parameters.put(P_VERSION, getSdbVersion());
       return parameters;
	} 
   
	public Map<String,String> getStandardSnsParameters(ProviderContext ctx, String action) throws InternalException {
       Map<String,String> parameters = getStandardParameters(ctx, action);
       
       parameters.put(P_VERSION, getSnsVersion());
       return parameters;
	}
	   
	public Map<String,String> getStandardSqsParameters(ProviderContext ctx, String action) throws InternalException {
       Map<String,String> parameters = getStandardParameters(ctx, action);
       
       parameters.put(P_VERSION, getSqsVersion());
       return parameters;
	}

    public void putExtraParameters(Map<String, String> parameters, Map<String, String> extraParameters) {
        if ( extraParameters == null || extraParameters.size() == 0 ) {
            return;
        }
        if ( parameters == null ) {
            parameters = new HashMap<String, String>();
        }
        parameters.putAll( extraParameters );
    }

    public @Nullable Map<String,String> getTagFilterParams(@Nullable Map<String,String> tags) {
        return getTagFilterParams( tags, 1 );
    }

    public @Nullable Map<String,String> getTagFilterParams(@Nullable Map<String,String> tags, int startingFilterIndex) {
        if ( tags == null || tags.size() == 0 ) {
            return null;
        }

        Map<String, String> filterParameters = new HashMap<String, String>();
        int i = startingFilterIndex;

        for ( Map.Entry<String, String> parameter : tags.entrySet() ) {
            String key = parameter.getKey();
            String value = parameter.getValue();
            filterParameters.put( "Filter." + i + ".Name", "tag:" + key );
            filterParameters.put( "Filter." + i + ".Value.1", value );
            i++;
        }
        return filterParameters;
    }

	public @Nonnull String getTimestamp(long timestamp, boolean withMillis) {
        SimpleDateFormat fmt;
        
        if( withMillis ) {
            fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        }
        else {
            fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");            
        }
        fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        return fmt.format(new Date(timestamp));
	}

	public long parseTime(@Nullable String time) throws CloudException {
	    if( time == null ) {
	        return 0L;
	    }
	    SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            
        if( time.length() > 0 ) {
            try {
                return fmt.parse(time).getTime();
            } 
            catch( ParseException e ) {
                fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                try {
                    return fmt.parse(time).getTime();
                } 
                catch( ParseException encore ) {
                    throw new CloudException("Could not parse date: " + time);
                }
            }
        }
        return 0L;
	}
	
    private String sign(byte[] key, String authString, String algorithm) throws InternalException {
        try {
            Mac mac = Mac.getInstance(algorithm);
            
            mac.init(new SecretKeySpec(key, algorithm));
            return new String(Base64.encodeBase64(mac.doFinal(authString.getBytes("utf-8"))));
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
        catch( IllegalStateException e ) {
        	logger.error(e);
        	e.printStackTrace();
        	throw new InternalException(e);
		} 
        catch( UnsupportedEncodingException e ) {
        	logger.error(e);
        	e.printStackTrace();
        	throw new InternalException(e);
		}
    }

    public String signUploadPolicy(String base64Policy) throws InternalException {
        ProviderContext ctx = getContext();

        if( ctx == null ) {
            throw new InternalException("No context for signing the request");
        }
    	return sign(ctx.getAccessPrivate(), base64Policy, S3_ALGORITHM);
    }
    
    public String signCloudFront(String accessKey, byte[] secretKey, String dateString) throws InternalException {
    	String signature = sign(secretKey, dateString, CLOUD_FRONT_ALGORITHM);

        if( getEC2Provider().isStorage() && "google".equalsIgnoreCase(getProviderName()) ) {
            return ("GOOG1 " + accessKey + ":" + signature);
        }
        else {
            return ("AWS " + accessKey + ":" + signature);
        }
    }
    
    public String signEc2(byte[] key, String serviceUrl, Map<String, String> parameters) throws InternalException {
    	return sign(key, buildEc2AuthString("POST", serviceUrl, parameters), EC2_ALGORITHM);
    }
    
    public String signAWS3(String keyId, byte[] key, String dateString) throws InternalException {
        return ("AWS3-HTTPS AWSAccessKeyId=" + keyId + ",Algorithm=" + EC2_ALGORITHM + ",Signature=" + sign(key, dateString, EC2_ALGORITHM));
    }
    
    public String signS3(String accessKey, byte[] secretKey, String action, String hash, String contentType, Map<String,String> headers, String bucket, String object) throws InternalException {
    	StringBuilder toSign = new StringBuilder();
    	
    	toSign.append(action);
    	toSign.append("\n");
    	if( hash != null ) {
    		toSign.append(hash);
    	}
    	toSign.append("\n");
    	if( contentType != null ) {
    		toSign.append(contentType);
    	}
    	toSign.append("\n\n");
    	ArrayList<String> keys = new ArrayList<String>();
    	keys.addAll(headers.keySet());
    	Collections.sort(keys);
    	for( String hkey : keys ) {
    		if( hkey.startsWith("x-amz") || (getEC2Provider().isStorage() && hkey.startsWith("x-goog")) ) {
    			String val = headers.get(hkey);
    			
    			if( val != null ) {
    				toSign.append(hkey);
    				toSign.append(":");
    				toSign.append(headers.get(hkey).trim());
    				toSign.append("\n");
    			}
    		}
    	}
    	toSign.append("/");
    	if( getEC2Provider().isEucalyptus() ) {
    	    toSign.append("services/Walrus/");
    	}
    	if( bucket != null ) {
    		toSign.append(bucket);
    		toSign.append("/");
    	}
    	if( object != null ) {
    		toSign.append(object.toLowerCase());
    	}
    	String signature = sign(secretKey, toSign.toString(), S3_ALGORITHM);

        if( getEC2Provider().isStorage() && "google".equalsIgnoreCase(getProviderName()) ) {
            return ("GOOG1 " + accessKey + ":" + signature);
        }
        else {
            return ("AWS " + accessKey + ":" + signature);
        }
    }
    
    @Override
    public String testContext() {
        ProviderContext ctx = getContext();

        if( ctx == null ) {
            logger.warn("No context exists for testing");
            return null;
        }
        try {
            ComputeServices compute = getComputeServices();

            if( compute != null ) {
                VirtualMachineSupport support = compute.getVirtualMachineSupport();

                if( support == null || !support.isSubscribed() ) {
                    logger.warn("Not subscribed to virtual machine support");
                    return null;
                }
            }
            else {
                StorageServices storage = getStorageServices();
                BlobStoreSupport support = storage.getBlobStoreSupport();

                if( support == null || !support.isSubscribed() ) {
                    logger.warn("No subscribed to storage services");
                    return null;
                }
            }
        }
        catch( Throwable t ) {
            logger.warn("Unable to connect to AWS for " + ctx.getAccountNumber() + ": " + t.getMessage());
            return null;
        }
        return ctx.getAccountNumber();
    }

    public void setTags(@Nonnull Node attr, @Nonnull Taggable item) {
        if( attr.hasChildNodes() ) {
            NodeList tags = attr.getChildNodes();

            for( int j=0; j<tags.getLength(); j++ ) {
                Node tag = tags.item(j);

                if( tag.getNodeName().equals("item") && tag.hasChildNodes() ) {
                    NodeList parts = tag.getChildNodes();
                    String key = null, value = null;

                    for( int k=0; k<parts.getLength(); k++ ) {
                        Node part = parts.item(k);

                        if( part.getNodeName().equalsIgnoreCase("key") ) {
                            if( part.hasChildNodes() ) {
                                key = part.getFirstChild().getNodeValue().trim();
                            }
                        }
                        else if( part.getNodeName().equalsIgnoreCase("value") ) {
                            if( part.hasChildNodes() ) {
                                value = part.getFirstChild().getNodeValue().trim();
                            }
                        }
                    }
                    if( key != null && value != null) {
                        item.setTag(key, value);
                    }
                }
            }
        }        
    }
}
