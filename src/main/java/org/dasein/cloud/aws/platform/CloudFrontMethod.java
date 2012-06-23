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

package org.dasein.cloud.aws.platform;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.platform.CDNSupport;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class CloudFrontMethod {
	static private final Logger logger = Logger.getLogger(CloudFrontMethod.class);
	
    static public final String CF_PREFIX = "cloudfront:";

    static public @Nonnull ServiceAction[] asCloudFrontServiceAction(@Nonnull String action) {
        if( action.equals("CreateDistribution") ) {
            return new ServiceAction[] { CDNSupport.CREATE_DISTRIBUTION };
        }
        else if( action.equals("GetDistribution") ) {
            return new ServiceAction[] { CDNSupport.GET_DISTRIBUTION };
        }
        else if( action.equals("ListDistributions") ) {
            return new ServiceAction[] { CDNSupport.LIST_DISTRIBUTION };
        }
        else if( action.equals("DeleteDistribution") ) {
            return new ServiceAction[] { CDNSupport.REMOVE_DISTRIBUTION };
        }
        return new ServiceAction[0];
    }

	static public final String CLOUD_FRONT_URL = "https://cloudfront.amazonaws.com";
		
	static public final String CF_VERSION      = "2010-05-01";
	
	static public class CloudFrontResponse {
		public Document document;
		public String   etag;
	}
	
	private CloudFrontAction   action      = null;
	private int                attempts    = 0;
	private String             body        = null;
	private String             contentType = null;
	private Map<String,String> headers     = null;
	private AWSCloud           provider    = null;
	
	public CloudFrontMethod(AWSCloud provider, CloudFrontAction action, Map<String,String> headers, String contentType, String body) {
		this.action = action;
		this.headers = headers;
		this.contentType = contentType;
		this.body = body;
		this.provider = provider;
	}
	
	private String getDate() throws CloudException {
		SimpleDateFormat fmt = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");

		// TODO: sync regularly with CloudFront
		return fmt.format(new Date());
	}
	
	CloudFrontResponse invoke(String ... args) throws CloudFrontException, CloudException, InternalException {
		StringBuilder url = new StringBuilder();
		String dateString = getDate();
        HttpMethod method;
		HttpClient client;
		int status;

		url.append(CLOUD_FRONT_URL + "/" + CF_VERSION + "/distribution");
		if( args != null && args.length > 0 ) {
			for( String arg : args ) {
				url.append("/");
				url.append(arg);
			}
		}
		method = action.getMethod(url.toString());
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
		method.addRequestHeader(AWSCloud.P_DATE, dateString);
		try {
			String signature = provider.signCloudFront(new String(provider.getContext().getAccessPublic(), "utf-8"), provider.getContext().getAccessPrivate(), dateString);
			
			method.addRequestHeader(AWSCloud.P_CFAUTH, signature);
		} 
		catch (UnsupportedEncodingException e) {
			logger.error(e);
			e.printStackTrace();
			throw new InternalException(e);
		}
		if( headers != null ) {
			for( Map.Entry<String, String> entry : headers.entrySet() ) {
				method.addRequestHeader(entry.getKey(), entry.getValue());
			}
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
		attempts++;
        client = new HttpClient();
        if( provider.getProxyHost() != null ) {
            client.getHostConfiguration().setProxy(provider.getProxyHost(), provider.getProxyPort());
        }
        CloudFrontResponse response = new CloudFrontResponse();
        try {
			try {
				status =  client.executeMethod(method);
			} 
			catch( HttpException e ) {
				logger.error(e);
				e.printStackTrace();
				throw new CloudException(e);
			} 
			catch( IOException e ) {
				logger.error(e);
				e.printStackTrace();
				throw new InternalException(e);
			}
			Header header = method.getResponseHeader("ETag");
			if( header != null ) {
				response.etag = header.getValue();
			}
			else {
				response.etag = null;
			}
			if( status == HttpStatus.SC_OK || status == HttpStatus.SC_CREATED || status == HttpStatus.SC_ACCEPTED ) {
				try {
					InputStream input = method.getResponseBodyAsStream();
	
					try {
						response.document = parseResponse(input);
						return response;
					}
					finally {
						input.close();
					}
				} 
				catch( IOException e ) {
					logger.error(e);
					e.printStackTrace();
					throw new CloudException(e);
				}
			}
			else if( status == HttpStatus.SC_NO_CONTENT ) {
				return null;
			}
			else {
				if( status == HttpStatus.SC_SERVICE_UNAVAILABLE || status == HttpStatus.SC_INTERNAL_SERVER_ERROR ) {
					try {
						InputStream input = method.getResponseBodyAsStream();
						
						if( input != null ) {
							input.close();
						}
					} 
					catch( IOException ignore ) {
						logger.warn("IO Exception trying to close error connection: " + ignore.getMessage());
					}
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
						try { Thread.sleep(5000L); }
						catch( InterruptedException e ) { }
						return invoke(args);
					}
				}
				try {
					InputStream input = method.getResponseBodyAsStream();
					Document doc;
	
					try {
						doc = parseResponse(input);
					}
					finally {
						input.close();
					}
					if( doc != null ) {
						String code = null, message = null, requestId = null, type = null;
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
								else if( attr.getNodeName().equals("Type") ) {
									type = attr.getFirstChild().getNodeValue().trim();
								}
								else if( attr.getNodeName().equals("Message") ) {
									message = attr.getFirstChild().getNodeValue().trim();
								}
							}
							
						}
						blocks = doc.getElementsByTagName("RequestId");
						if( blocks.getLength() > 0 ) {
							Node id = blocks.item(0);
							
							requestId = id.getFirstChild().getNodeValue().trim();
						}
						if( message == null ) {
							throw new CloudException("Unable to identify error condition: " + status + "/" + requestId + "/" + code);
						}
						throw new CloudFrontException(status, requestId, type, code, message);
					}
					throw new CloudException("Unable to parse error.");
				} 
				catch( IOException e ) {
					logger.error(e);
					e.printStackTrace();
					throw new CloudException(e);
				}			
			}
        }
		finally {
			method.releaseConnection();
		}
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
	            
			//System.out.println(sb.toString());
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
