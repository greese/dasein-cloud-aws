/**
 * Copyright (C) 2009-2013 Enstratius, Inc.
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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.SimpleTimeZone;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.storage.BlobStoreSupport;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.XMLParser;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class S3Method {
    static private final Logger logger = Logger.getLogger(S3Method.class);

    static public final String S3_PREFIX     = "s3:";

    static public @Nonnull ServiceAction[] asS3ServiceAction(@Nonnull String action) {
        if( action.equals("CreateBucket") ) {
            return new ServiceAction[] { BlobStoreSupport.CREATE_BUCKET };
        }
        else if( action.equals("GetObject") ) {
            return new ServiceAction[] { BlobStoreSupport.DOWNLOAD };
        }
        else if( action.equals("GetBucket") ) {
            return new ServiceAction[] { BlobStoreSupport.GET_BUCKET };
        }
        else if( action.equals("ListBucket") ) {
            return new ServiceAction[] { BlobStoreSupport.LIST_BUCKET, BlobStoreSupport.LIST_BUCKET_CONTENTS };
        }
        else if( action.equals("PutAccessControlPolicy") ) {
            return new ServiceAction[] { BlobStoreSupport.MAKE_PUBLIC };
        }
        else if( action.equals("DeleteBucket") ) {
            return new ServiceAction[] { BlobStoreSupport.REMOVE_BUCKET };
        }
        else if( action.equals("PutObject") ) {
            return new ServiceAction[] { BlobStoreSupport.UPLOAD };
        }
        return new ServiceAction[0]; 
    }
    
	static public class S3Response {
		public long        contentLength;
		public String      contentType;
		public Document    document;
		public Header[]    headers;
		public InputStream input;
		public HttpRequestBase method;
		
		public void close() {
			try { input.close(); } catch( Throwable ignore ) { }
		}
	}
	
	static public byte[] computeMD5Hash(String str) throws NoSuchAlgorithmException, IOException {
		ByteArrayInputStream input = new ByteArrayInputStream(str.getBytes("utf-8"));
		
		return computeMD5Hash(input);
	}
	
    static public byte[] computeMD5Hash(InputStream is) throws NoSuchAlgorithmException, IOException {
        BufferedInputStream bis = new BufferedInputStream(is);
        
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[16384];
            int bytesRead = -1;
            while ((bytesRead = bis.read(buffer, 0, buffer.length)) != -1) {
                messageDigest.update(buffer, 0, bytesRead);
            }
            return messageDigest.digest();
        } 
        finally {
            try {
                bis.close();
            } 
            catch (Exception e) {
                System.err.println("Unable to close input stream of hash candidate: " + e);
            }
        }
    }
    
    static public String getChecksum(File file) throws NoSuchAlgorithmException, FileNotFoundException, IOException {
        return toBase64(computeMD5Hash(new FileInputStream(file)));
    }
   
	
    static public String toBase64(byte[] data) {
        byte[] b64 = Base64.encodeBase64(data);
        
        return new String(b64);
    }
    
	private S3Action           action      = null;
	private int                attempts    = 0;
	private String             body        = null;
	private String             contentType = null;
	private Map<String,String> headers     = null;
	private Map<String,String> parameters  = null;
	private AWSCloud           provider    = null;
	private File               uploadFile  = null;

	public S3Method(AWSCloud provider, S3Action action) {
		this.action = action;
		this.headers = new HashMap<String,String>();
		this.provider = provider;
	}
	
	public S3Method(AWSCloud provider, S3Action action, Map<String,String> parameters, Map<String,String> headers) {
		this.action = action;
		this.headers = (headers == null ? new HashMap<String,String>() : headers);
		this.provider = provider;
		this.parameters = parameters;
	}
	
	public S3Method(AWSCloud provider, S3Action action, Map<String,String> parameters, Map<String,String> headers, String contentType, String body) {
		this.action = action;
		this.headers = (headers == null ? new HashMap<String,String>() : headers);
		this.contentType = contentType;
		this.body = body;
		this.provider = provider;
		this.parameters = parameters;
	}
    
	public S3Method(AWSCloud provider, S3Action action, Map<String,String> parameters, Map<String,String> headers, String contentType, File uploadFile) {
		this.action = action;
		this.headers = (headers == null ? new HashMap<String,String>() : headers);
		this.contentType = contentType;
		this.uploadFile = uploadFile;
		this.provider = provider;
		this.parameters = parameters;
	}
	
	private String getDate() throws CloudException {
        if( provider.getEC2Provider().isStorage() && "google".equalsIgnoreCase(provider.getProviderName()) ) {
            SimpleDateFormat format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ssz", new Locale("US"));
            Calendar cal = Calendar.getInstance(new SimpleTimeZone(0, "GMT"));

            format.setCalendar(cal);
            return format.format(new Date());
        }
        else {
            SimpleDateFormat fmt = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");
            Calendar cal = Calendar.getInstance(new SimpleTimeZone(0, "GMT"));

            fmt.setCalendar(cal);
            return fmt.format(new Date());
        }
	}

	S3Response invoke(String bucket, String object) throws S3Exception, CloudException, InternalException {
	    return invoke(bucket, object, null);
	}

    protected @Nonnull HttpClient getClient(String url, boolean multipart) throws InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new InternalException("No context was specified for this request");
        }
        boolean ssl = url.startsWith("https");
        HttpParams params = new BasicHttpParams();

        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        if( !multipart ) {
            HttpProtocolParams.setContentCharset(params, HTTP.UTF_8);
        }
        HttpProtocolParams.setUserAgent(params, "Dasein Cloud");

        Properties p = ctx.getCustomProperties();

        if( p != null ) {
            String proxyHost = p.getProperty("proxyHost");
            String proxyPort = p.getProperty("proxyPort");

            if( proxyHost != null ) {
                int port = 0;

                if( proxyPort != null && proxyPort.length() > 0 ) {
                    port = Integer.parseInt(proxyPort);
                }
                params.setParameter(ConnRoutePNames.DEFAULT_PROXY, new HttpHost(proxyHost, port, ssl ? "https" : "http"));
            }
        }
        return new DefaultHttpClient(params);
    }

    static private final Logger wire = AWSCloud.getWireLogger(S3.class);

	S3Response invoke(@Nullable String bucket, @Nullable String object, @Nullable String temporaryEndpoint) throws S3Exception, CloudException, InternalException {
        if( wire.isDebugEnabled() ) {
            wire.debug("");
            wire.debug("----------------------------------------------------------------------------------");
        }
        try {
            StringBuilder url = new StringBuilder();
            boolean leaveOpen = false;
            HttpRequestBase method;
            HttpClient client;
            int status;

            if( provider.getEC2Provider().isAWS() ) {
                url.append("https://");
                String regionId = provider.getContext().getRegionId();

                if( temporaryEndpoint == null ) {
                    boolean validDomainName = isValidDomainName(bucket);
                    if( bucket != null && validDomainName ) {
                        url.append(bucket);
                        if (regionId != null && !"us-east-1".equals(regionId)) {
                            url.append(".s3-");
                            url.append(regionId);
                            url.append(".amazonaws.com/");
                        }
                        else {
                            url.append(".s3.amazonaws.com/");
                        }
                    }
                    else if ( bucket != null && !validDomainName) {

                        if (regionId != null && !"us-east-1".equals(regionId)) {
                            url.append("s3-");
                            url.append(regionId);
                            url.append(".amazonaws.com/");
                        }
                        else {
                            url.append("s3.amazonaws.com/");
                        }
                    }
                    else {
                        url.append("s3.amazonaws.com/");
                    }
                    if ( bucket != null && !validDomainName) {
                        url.append(bucket);
                        url.append("/");
                    }
                }
                else {
                    url.append(temporaryEndpoint);
                    url.append("/");
                }
            }
            else if( provider.getEC2Provider().isStorage() && "google".equalsIgnoreCase(provider.getProviderName()) ) {
                url.append("https://");
                if( temporaryEndpoint == null ) {
                    if( bucket != null ) {
                        url.append(bucket);
                        url.append(".");
                    }
                    url.append("commondatastorage.googleapis.com/");
                }
                else {
                    url.append(temporaryEndpoint);
                    url.append("/");
                }
            }
            else {
                int idx = 0;
                
                if( !provider.getContext().getEndpoint().startsWith("http") ) {
                    url.append("https://");
                }
                else {
                    idx = provider.getContext().getEndpoint().indexOf("https://");
                    if( idx == -1 ) {
                        idx = "http://".length();
                        url.append("http://");
                    }
                    else {
                        idx = "https://".length();
                        url.append("https://");
                    }
                }
                String service = "";
                if( provider.getEC2Provider().isEucalyptus() ) {
                    service = "Walrus/";
                }

                if( temporaryEndpoint == null ) {
                    url.append(provider.getContext().getEndpoint().substring(idx));
                    if( !provider.getContext().getEndpoint().endsWith("/") ) {
                        url.append("/").append(service);
                    }
                    else {
                        url.append(service);
                    }
                }
                else {
                    url.append(temporaryEndpoint);
                    url.append("/");
                    url.append(service);
                }
                if( bucket != null ) {
                    url.append(bucket);
                    url.append("/");
                }
            }
            if( object != null ) {
                url.append(object);
            }
            else if( parameters != null ) {
                boolean first = true;

                if( object != null && object.indexOf('?') != -1 ) {
                    first = false;
                }
                for( Map.Entry<String,String> entry : parameters.entrySet() ) {
                    String key = entry.getKey();
                    String val = entry.getValue();

                    if( first ) {
                        url.append("?");
                        first = false;
                    }
                    else {
                        url.append("&");
                    }
                    if( val != null ) {
                        url.append(AWSCloud.encode(key, false));
                        url.append("=");
                        url.append(AWSCloud.encode(val, false));
                    }
                    else {
                        url.append(AWSCloud.encode(key, false));
                    }
                }
            }            

            if( provider.getEC2Provider().isStorage() && provider.getProviderName().equalsIgnoreCase("Google") ) {
                headers.put(AWSCloud.P_GOOG_DATE, getDate());
            }
            else {
                headers.put(AWSCloud.P_AWS_DATE, getDate());
            }
            method = action.getMethod(url.toString());
            if( headers != null ) {
                for( Map.Entry<String, String> entry : headers.entrySet() ) {
                    method.addHeader(entry.getKey(), entry.getValue());
                }
            }
            if( contentType == null && body != null ) {
                contentType = "application/xml";
                method.addHeader("Content-Type", contentType);
            }
            else if( contentType != null ) {
                method.addHeader("Content-Type", contentType);
            }
            try {
                String hash = null;
                String signature;
                
                signature = provider.signS3(new String(provider.getContext().getAccessPublic(), "utf-8"), provider.getContext().getAccessPrivate(), method.getMethod(), hash, contentType, headers, bucket, object);
                method.addHeader(AWSCloud.P_CFAUTH, signature);
            } 
            catch (UnsupportedEncodingException e) {
                logger.error(e);
                e.printStackTrace();
                throw new InternalException(e);
            }
            if( body != null ) {
                try {
                    ((HttpEntityEnclosingRequestBase)method).setEntity(new StringEntity(body, "application/xml", "utf-8"));
                }
                catch( UnsupportedEncodingException e ) {
                    throw new InternalException(e);
                }
            }
            else if( uploadFile != null ) {
                ((HttpEntityEnclosingRequestBase)method).setEntity(new FileEntity(uploadFile, contentType));
            }
            attempts++;
            client = getClient(url.toString(), body == null && uploadFile == null);
            
            if( wire.isDebugEnabled() ) {
                wire.debug("[" + url.toString() + "]");
                wire.debug(method.getRequestLine().toString());
                for( Header header : method.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
                if( body != null ) {
                    try { wire.debug(EntityUtils.toString(((HttpEntityEnclosingRequestBase)method).getEntity())); }
                    catch( IOException ignore ) { }

                    wire.debug("");                    
                }
                else if( uploadFile != null ) {
                    wire.debug("-- file upload --");
                    wire.debug("");
                }
            }
            S3Response response = new S3Response();
            HttpResponse httpResponse;
            
            try {
                APITrace.trace(provider, action.toString());
                httpResponse = client.execute(method);
                if( wire.isDebugEnabled() ) {
                    wire.debug(httpResponse.getStatusLine().toString());
                    for( Header header : httpResponse.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }             
                    wire.debug("");
                }
                status = httpResponse.getStatusLine().getStatusCode();
            } 
            catch( IOException e ) {
                logger.error(url + ": " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            response.headers = httpResponse.getAllHeaders();
    
            HttpEntity entity = httpResponse.getEntity();
            InputStream input = null;

            if( entity != null ) {
                try {
                    input = entity.getContent();
                }
                catch( IOException e ) {
                    throw new CloudException(e);
                }
            }
            try {
                if( status == HttpServletResponse.SC_OK || status == HttpServletResponse.SC_CREATED || status == HttpServletResponse.SC_ACCEPTED ) {
                    Header clen = httpResponse.getFirstHeader("Content-Length");
                    long len = -1L;
                    
                    if( clen != null ) {
                        len = Long.parseLong(clen.getValue());
                    }
                    if( len != 0L ) {
                        try {
                            Header ct = httpResponse.getFirstHeader("Content-Type");
    
                            if( ct != null && (ct.getValue().startsWith("application/xml") || ct.getValue().startsWith("text/xml")) ) {
                                try {
                                    response.document = parseResponse(input);
                                    return response;
                                }
                                finally {
                                    input.close();
                                }
                            }
                            else if( ct != null && ct.getValue().startsWith("application/octet-stream") && len < 1 ) {
                                return null;
                            }
                            else {
                                response.contentLength = len;
                                if( ct != null ) {
                                    response.contentType = ct.getValue();
                                }
                                response.input = input;
                                response.method = method;
                                leaveOpen = true;
                                return response;
                            }
                        }
                        catch( IOException e ) {
                            logger.error(e);
                            e.printStackTrace();
                            throw new CloudException(e);
                        }
                    }
                    else {
                        return response;
                    }
                }
                else if( status == HttpServletResponse.SC_NO_CONTENT ) {
                    return response;
                }
                else if( status == HttpServletResponse.SC_NOT_FOUND ) {
                    throw new S3Exception(status, null, null, "Object not found.");
                }
                else {
                    if( status == HttpServletResponse.SC_SERVICE_UNAVAILABLE || status == HttpServletResponse.SC_INTERNAL_SERVER_ERROR ) {
                        if( attempts >= 5 ) {
                            String msg;
                            
                            if( status == HttpServletResponse.SC_SERVICE_UNAVAILABLE ) {
                                msg = "Cloud service is currently unavailable.";
                            }
                            else {
                                msg = "The cloud service encountered a server error while processing your request.";
                            }
                            logger.error(msg);
                            throw new CloudException(msg);
                        }
                        else {
                            leaveOpen = true;
                            if( input != null ) {
                                try { input.close(); }
                                catch( IOException ignore ) { }
                            }
                            try { Thread.sleep(5000L); }
                            catch( InterruptedException ignore ) { }
                            return invoke(bucket, object);
                        }
                    }
                    try {
                        Document doc;
                        
                        try {
                            logger.warn("Received error code: " + status);
                            doc = parseResponse(input);
                        }
                        finally {
                            input.close();
                        }
                        if( doc != null ) {
                            String endpoint = null, code = null, message = null, requestId = null;
                            NodeList blocks = doc.getElementsByTagName("Error");
        
                            if( blocks.getLength() > 0 ) {
                                Node error = blocks.item(0);
                                NodeList attrs;
                                
                                attrs = error.getChildNodes();
                                for( int i=0; i<attrs.getLength(); i++ ) {
                                    Node attr = attrs.item(i);
                                    
                                    if( attr.getNodeName().equals("Code") && attr.hasChildNodes() ) {
                                        code = attr.getFirstChild().getNodeValue().trim();
                                    }
                                    else if( attr.getNodeName().equals("Message") && attr.hasChildNodes() ) {
                                        message = attr.getFirstChild().getNodeValue().trim();
                                    }
                                    else if( attr.getNodeName().equals("RequestId") && attr.hasChildNodes() ) {
                                        requestId = attr.getFirstChild().getNodeValue().trim();
                                    }
                                    else if( attr.getNodeName().equals("Endpoint") && attr.hasChildNodes() ) {
                                        endpoint = attr.getFirstChild().getNodeValue().trim();
                                    }
                                }
                                
                            }
                            if( endpoint != null && code.equals("TemporaryRedirect") ) {
                                if( temporaryEndpoint != null ) {
                                    throw new CloudException("Too deep redirect to " + endpoint);
                                }
                                else {
                                    return invoke(bucket, object, endpoint);
                                }
                            }
                            else {
                                if( message == null ) {
                                    throw new CloudException("Unable to identify error condition: " + status + "/" + requestId + "/" + code);
                                }
                                throw new S3Exception(status, requestId, code, message);
                            }
                        }
                        else {
                            throw new CloudException("Unable to parse error.");
                        }
                    }
                    catch( IOException e ) {
                        if( status == HttpServletResponse.SC_FORBIDDEN ) {
                            throw new S3Exception(status, "", "AccessForbidden", "Access was denied without explanation.");
                        }                              
                        throw new CloudException(e);
                    }
                    catch( RuntimeException e ) {
                        throw new CloudException(e);
                    }
                    catch( Error e ) {
                        throw new CloudException(e);
                    }					
                }
            }
            finally {
                if( !leaveOpen ) {
                    if( input != null ) {
                        try { input.close(); }
                        catch( IOException ignore ) { }
                    }
                }
            }
        }
        finally {
            if( wire.isDebugEnabled() ) {
                wire.debug("----------------------------------------------------------------------------------");
                wire.debug("");
            }
        }
    }
	
	private boolean isValidDomainName(String bucket) {
        return (bucket != null && Pattern.matches("^[a-z0-9](-*[a-z0-9]){2,62}$", bucket));
    }

	private Document parseResponse(InputStream responseBodyAsStream) throws CloudException, InternalException {
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(responseBodyAsStream));
			StringBuilder sb = new StringBuilder();
			String line;
	            
			while( (line = in.readLine()) != null ) {
				sb.append(line);
			}
			in.close();
	            
            wire.debug(sb.toString());

			return XMLParser.parse(new ByteArrayInputStream(sb.toString().getBytes()));
		}
		catch( IOException e ) {
			logger.error(e);
			e.printStackTrace();
			throw new CloudException(e);
		}
		catch( ParserConfigurationException e ) {
			logger.error(e);
			e.printStackTrace();
			throw new CloudException(e);
		}
		catch( SAXException e ) {
			logger.error(e);
			e.printStackTrace();
			throw new CloudException(e);
	    }				
	}
}
