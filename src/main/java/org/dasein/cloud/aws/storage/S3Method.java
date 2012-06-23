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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.FileRequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.storage.BlobStoreSupport;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class S3Method {
static private final Logger logger = Logger.getLogger(S3Method.class);
	
	static public final String CLOUD_FRONT_URL = "https://cloudfront.amazonaws.com";
		
	static public final String CF_VERSION      = "2009-04-02";

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
		public HttpMethod  method;
		
		public void close() {
			try { input.close(); } catch( Throwable ignore ) { }
			try { method.releaseConnection(); } catch( Throwable ignore ) { }
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
		SimpleDateFormat fmt = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");

		// TODO: sync regularly with CloudFront
		return fmt.format(new Date());
	}
	S3Response invoke(String bucket, String object) throws S3Exception, CloudException, InternalException {
	    return invoke(bucket, object, null);
	}
	
	S3Response invoke(String bucket, String object, String temporaryEndpoint) throws S3Exception, CloudException, InternalException {
		StringBuilder url = new StringBuilder();
		boolean leaveOpen = false;
        HttpMethod method;
		HttpClient client;
		int status;

		if( provider.isAmazon() ) {
		    url.append("https://");
	        if( temporaryEndpoint == null ) {
            	boolean validDomainName = isValidDomainName(bucket);
	            if( bucket != null && validDomainName ) {
	            	url.append(bucket);
		            url.append(".");
	            }
	            url.append("s3.amazonaws.com/");
	            if ( bucket != null && !validDomainName) {
	            	url.append(bucket+"/");
	            }
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
		    if( temporaryEndpoint == null ) {
		        url.append(provider.getContext().getEndpoint().substring(idx));
	            if( !provider.getContext().getEndpoint().endsWith("/") ) {
	                url.append("/Walrus/");
	            }
	            else {
	                url.append("Walrus/");                
	            }
		    }
		    else {
		        url.append(temporaryEndpoint);
		        url.append("/Walrus/");
		    }
            if( bucket != null ) {
                url.append(bucket);
                url.append("/");
            }
		}
		if( object != null ) {
			url.append(object);
		}
		if( parameters != null ) {
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
		headers.put(AWSCloud.P_DATE, getDate());
		method = action.getMethod(url.toString());
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
		if( headers != null ) {
			for( Map.Entry<String, String> entry : headers.entrySet() ) {
				method.addRequestHeader(entry.getKey(), entry.getValue());
			}
		}
		try {
			String hash = null;
			String signature;
			
			signature = provider.signS3(new String(provider.getContext().getAccessPublic(), "utf-8"), provider.getContext().getAccessPrivate(), method.getName().toUpperCase(), hash, contentType, headers, bucket, object);
			method.addRequestHeader(AWSCloud.P_CFAUTH, signature);
		} 
		catch (UnsupportedEncodingException e) {
			logger.error(e);
			e.printStackTrace();
			throw new InternalException(e);
		}
		if( body != null) {
			if( method instanceof EntityEnclosingMethod ) {
				try {
					((EntityEnclosingMethod)method).setRequestEntity(new StringRequestEntity(body, contentType, "utf-8"));
				} 
				catch( UnsupportedEncodingException e ) {
					logger.error(e);
					e.printStackTrace();
					throw new InternalException(e);
				}
			}
		}
		else if( uploadFile != null ) {
			if( method instanceof EntityEnclosingMethod ){
				((EntityEnclosingMethod)method).setRequestEntity(new FileRequestEntity(uploadFile, contentType));
			}
		}
		attempts++;
        client = new HttpClient();
        if( provider.getProxyHost() != null ) {
            client.getHostConfiguration().setProxy(provider.getProxyHost(), provider.getProxyPort());
        }
        S3Response response = new S3Response();
        try {
			try {
				status =  client.executeMethod(method);
			} 
			catch( HttpException e ) {
				logger.error(url + ": " + e.getMessage());
				e.printStackTrace();
				throw new CloudException(e);
			} 
			catch( IOException e ) {
				logger.error(url + ": " + e.getMessage());
				e.printStackTrace();
				throw new InternalException(e);
			}
			response.headers = method.getResponseHeaders();
			InputStream input;
			try {
				input = method.getResponseBodyAsStream();
			}
			catch( IOException e ) {
				if( attempts >= 5 ) {
					logger.error(e);
					e.printStackTrace();
					throw new InternalException(e);
				}
				try { Thread.sleep(5000L); }
				catch( InterruptedException ignore ) { }
				return invoke(bucket, object);
			}
			try {
				if( status == HttpStatus.SC_OK || status == HttpStatus.SC_CREATED || status == HttpStatus.SC_ACCEPTED ) {
					Header clen = method.getResponseHeader("Content-Length");
					long len = -1L;
					
					if( clen != null ) {
						len = Long.parseLong(clen.getValue());
					}
					if( len != 0L ) {
						try {
							Header ct = method.getResponseHeader("Content-Type");
	
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
				else if( status == HttpStatus.SC_NO_CONTENT ) {
					return response;
				}
				else if( status == HttpStatus.SC_NOT_FOUND ) {
					throw new S3Exception(status, null, null, "Object not found.");
				}
				else {
					if( status == HttpStatus.SC_SERVICE_UNAVAILABLE || status == HttpStatus.SC_INTERNAL_SERVER_ERROR ) {
						if( attempts >= 5 ) {
							String msg;
							
							if( status == HttpStatus.SC_SERVICE_UNAVAILABLE ) {
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
							method.releaseConnection();
							try { Thread.sleep(5000L); }
							catch( InterruptedException e ) { }
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
                                    
                                    if( attr.getNodeName().equals("Code") ) {
                                        code = attr.getFirstChild().getNodeValue().trim();
                                    }
                                    else if( attr.getNodeName().equals("Message") ) {
                                        message = attr.getFirstChild().getNodeValue().trim();
                                    }
                                    else if( attr.getNodeName().equals("RequestId") ) {
                                        requestId = attr.getFirstChild().getNodeValue().trim();
                                    }
                                    else if( attr.getNodeName().equals("Endpoint") ) {
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
                        if( status == HttpStatus.SC_FORBIDDEN ) {
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
			if( !leaveOpen ) {
				method.releaseConnection();
			}
		}
	}
	
	private boolean isValidDomainName(String bucket) {
		if (bucket != null && Pattern.matches("^[a-z0-9](-*[a-z0-9]){2,62}$", bucket)) {
			return true;
		}
		return false;
	}

	private Document parseResponse(InputStream responseBodyAsStream) throws CloudException, InternalException {
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(responseBodyAsStream));
			StringBuffer sb = new StringBuffer();
			String line;
	            
			while( (line = in.readLine()) != null ) {
				sb.append(line);
			}
			in.close();
	            
			//System.out.println(sb);
			ByteArrayInputStream bas = new ByteArrayInputStream(sb.toString().getBytes());
	            
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder parser = factory.newDocumentBuilder();
			Document doc = parser.parse(bas);

			bas.close();
			return doc;
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
