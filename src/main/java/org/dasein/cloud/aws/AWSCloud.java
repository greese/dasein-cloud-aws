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
import java.util.TimeZone;
import java.util.TreeSet;

import javax.annotation.Nonnull;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.dasein.cloud.AbstractCloud;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Tag;
import org.dasein.cloud.aws.admin.AWSAdminServices;
import org.dasein.cloud.aws.compute.EC2ComputeServices;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.aws.compute.EC2Method;
import org.dasein.cloud.aws.identity.AWSIdentityServices;
import org.dasein.cloud.aws.network.EC2NetworkServices;
import org.dasein.cloud.aws.platform.AWSPlatformServices;
import org.dasein.cloud.aws.storage.AWSCloudStorageServices;

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
	static public final String P_DATE              = "x-amz-date";
	static public final String P_SIGNATURE         = "Signature";
	static public final String P_SIGNATURE_METHOD  = "SignatureMethod";
	static public final String P_SIGNATURE_VERSION = "SignatureVersion";
	static public final String P_TIMESTAMP         = "Timestamp";
	static public final String P_VERSION           = "Version";
	
	static public final String CLOUD_FRONT_ALGORITHM = "HmacSHA1";
    static public final String EC2_ALGORITHM         = "HmacSHA256";
	static public final String S3_ALGORITHM          = "HmacSHA1";
    static public final String SIGNATURE             = "2";
    static public final String VERSION               = "2011-07-15";
    static public final String AUTO_SCALE_VERSION    = "2009-05-15";
    static public final String ELB_VERSION           = "2009-05-15";
    static public final String CLOUD_WATCH_VERSION   = "2009-05-15";
    static public final String RDS_VERSION           = "2011-04-01";
    static public final String ROUTE53_VERSION       = "2010-10-01";
    static public final String SDB_VERSION           = "2009-04-15";
    static public final String SNS_VERSION           = "2010-03-31";
    static public final String SQS_VERSION           = "2009-02-01";
    
    static public String encode(String value, boolean encodePath) throws InternalException {
        String encoded = null;
        
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
    	URI endpoint = null;
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
    
    public boolean createTags(String resourceId, Tag ... keyValuePairs) {
        try {
            Map<String,String> parameters = getStandardParameters(getContext(), "CreateTags");
            EC2Method method;
            
            parameters.put("ResourceId.1", resourceId);
            for( int i=0; i<keyValuePairs.length; i++ ) {
                String key = keyValuePairs[i].getKey();
                String value = keyValuePairs[i].getValue();
                
                parameters.put("Tag." + i + ".Key", key);
                parameters.put("Tag." + i + ".Value", value);
            }
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
                    parameters.put("ResourceId.1", resourceId);
                    for( int i=0; i<keyValuePairs.length; i++ ) {
                        String key = keyValuePairs[i].getKey();
                        String value = keyValuePairs[i].getValue();
                        
                        parameters.put("Tag." + i + ".Key", key);
                        parameters.put("Tag." + i + ".Value", value);
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
                logger.error("EC2 error settings tags for " + resourceId + ": " + e.getSummary());
                return false;
            }   
            return true;
        }
        catch( Throwable ignore ) {
            return false;
        }
    }
    
    @Override
    public AWSAdminServices getAdminServices() {
        return new AWSAdminServices(this);
    }
    
	private String[] getBootstrapUrls(ProviderContext ctx) {
	    String endpoint = ctx.getEndpoint();
        
        if( endpoint == null ) {
            return new String[0];
        }
        if( endpoint.indexOf(",") == -1 ) {
            return new String[] { endpoint };
        }
        String[] endpoints = endpoint.split(",");
        if( endpoints != null && endpoints.length > 1 ) {
            String second = endpoints[1];
            
            if( !second.startsWith("http") ) {
                if( endpoints[0].startsWith("http") ) {
                    // likely a URL with a , in it
                    return new String[] { endpoint + (isAmazon() ? "" : "/Eucalyptus") };
                }
            }
        }
        for( int i=0; i<endpoints.length; i++ ) {
            if( !endpoints[i].startsWith("http") ) {
                endpoints[i] = "https://" + endpoints[i] + (isAmazon() ? "" : "/Eucalyptus");        
            }
        }
        return endpoints;
    }
	   
	@Override
	public String getCloudName() {
		String name = getContext().getCloudName();
		
		return ((name == null ) ? "AWS" : name);
	}

	@Override
	public EC2ComputeServices getComputeServices() {
	    return new EC2ComputeServices(this);
	}
	
	@Override
	public RegionsAndZones getDataCenterServices() {
	    return new RegionsAndZones(this);
	}
    
    public String getEc2Url() throws InternalException, CloudException {
        String url = getEc2Url(getContext().getRegionId());
        
        if( isAmazon() ) {
            return url;
        }
        else {
            return url + "/Eucalyptus";
        }
    }
    
    String getEc2Url(String regionId) throws InternalException, CloudException {
        String url;
        
        if( regionId == null ) {
            return getBootstrapUrls(getContext())[0];
        }
        if( isAmazon() ) {
            if( !getContext().getEndpoint().contains("amazon") ) {
                url = getContext().getEndpoint();
                if( url == null ) {
                    return null;
                }
                if( !url.startsWith("http") ) {
                    String cloudUrl = getContext().getEndpoint();
                    
                    if( cloudUrl != null && cloudUrl.startsWith("http:") ) {
                        return "http://" + url + "/" + regionId;         
                    }
                    return "https://" + url + "/" + regionId;
                }
                else {
                    return url + "/" + regionId;
                }               
            }
            else {
                url = getContext().getEndpoint();
                if( url != null && url.endsWith("amazonaws.com") ) {
                    return "https://ec2." + regionId + ".amazonaws.com";
                }
                return "https://ec2." + regionId + ".amazonaws.com";
            }
        }
        url = getContext().getEndpoint();
        if( url == null ) {
            return null;
        }
        if( !url.startsWith("http") ) {
            String cloudUrl = getContext().getEndpoint();
            
            if( cloudUrl != null && cloudUrl.startsWith("http:") ) {
                return "http://" + url;         
            }
            return "https://" + url;
        }
        else {
            return url;
        }
    }
    
	@Override
	public AWSIdentityServices getIdentityServices() {
	    return new AWSIdentityServices(this);
	}
    
	@Override
	public EC2NetworkServices getNetworkServices() {
	    return new EC2NetworkServices(this);
	}
	
	@Override
	public AWSPlatformServices getPlatformServices() {
	    return new AWSPlatformServices(this);
	}
    
	@Override
	public String getProviderName() {
		String name = getContext().getProviderName();
		
		return ((name == null) ? "Amazon" : name);
	}
	
	public String getProxyHost() {
	    return getContext().getCustomProperties().getProperty("proxyHost");
	}
	
	public int getProxyPort() {
	    String port = getContext().getCustomProperties().getProperty("proxyPort");
	    
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
        return getStandardParameters(ctx, action, VERSION);
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
        
        parameters.put(P_VERSION, CLOUD_WATCH_VERSION);
        return parameters;
    }
	   
	public Map<String,String> getStandardRdsParameters(ProviderContext ctx, String action) throws InternalException {
       Map<String,String> parameters = getStandardParameters(ctx, action);
       
       parameters.put(P_VERSION, RDS_VERSION);
       return parameters;
	}  
   
	public Map<String,String> getStandardSimpleDBParameters(ProviderContext ctx, String action) throws InternalException {
       Map<String,String> parameters = getStandardParameters(ctx, action);
       
       parameters.put(P_VERSION, SDB_VERSION);
       return parameters;
	} 
   
	public Map<String,String> getStandardSnsParameters(ProviderContext ctx, String action) throws InternalException {
       Map<String,String> parameters = getStandardParameters(ctx, action);
       
       parameters.put(P_VERSION, SNS_VERSION);
       return parameters;
	}
	   
	public Map<String,String> getStandardSqsParameters(ProviderContext ctx, String action) throws InternalException {
       Map<String,String> parameters = getStandardParameters(ctx, action);
       
       parameters.put(P_VERSION, SQS_VERSION);
       return parameters;
	}   
   
	public String getTimestamp(long timestamp, boolean withMillis) {
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
	
	public boolean isAmazon() {
	    return (getContext().getEndpoint().contains("amazon") || getContext().getEndpoint().contains("enstratus")); 
	}
	
	public long parseTime(String time) throws CloudException {
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
    	return sign(getContext().getAccessPrivate(), base64Policy, S3_ALGORITHM);
    }
    
    public String signCloudFront(String accessKey, byte[] secretKey, String dateString) throws InternalException {
    	String signature = sign(secretKey, dateString, CLOUD_FRONT_ALGORITHM);
    	
    	return ("AWS" + " " + accessKey + ":" + signature);
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
    		if( hkey.startsWith("x-amz") ) {
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
    	if( !isAmazon() ) {
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
    	
    	return ("AWS" + " " + accessKey + ":" + signature);
    }
    
    @Override
    public String testContext() {
        try {
            if( !getComputeServices().getVirtualMachineSupport().isSubscribed() ) {
                return null;
            }
        }
        catch( Throwable t ) {
            logger.warn("Unable to connect to AWS for " + getContext().getAccountNumber() + ": " + t.getMessage());
            return null;
        }
        return getContext().getAccountNumber();
    }
}
