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

package org.dasein.cloud.aws.platform;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.platform.CDNSupport;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.XMLParser;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.annotation.Nonnull;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.SimpleTimeZone;

public class CloudFrontMethod {
	static private final Logger logger = Logger.getLogger(CloudFrontMethod.class);
    static SimpleDateFormat fmt = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");
    static {
        fmt.setCalendar(Calendar.getInstance(new SimpleTimeZone(0, "GMT")));
    }
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
	private Map<String,String> headers     = null;
	private AWSCloud           provider    = null;
	
	public CloudFrontMethod(AWSCloud provider, CloudFrontAction action, Map<String,String> headers, String body) {
		this.action = action;
		this.headers = headers;
		this.body = body;
		this.provider = provider;
	}
	
	private static String getDate() throws CloudException {
		// TODO: sync regularly with CloudFront
		return fmt.format(new Date());
	}

	CloudFrontResponse invoke(String ... args) throws CloudFrontException, CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new InternalException("Context not specified for this request");
        }
		StringBuilder url = new StringBuilder();
		String dateString = getDate();
        HttpRequestBase method;
		HttpClient client;

		url.append(CLOUD_FRONT_URL + "/" + CF_VERSION + "/distribution");
		if( args != null && args.length > 0 ) {
			for( String arg : args ) {
				url.append("/");
				url.append(arg);
			}
		}
		method = action.getMethod(url.toString());
		method.addHeader(AWSCloud.P_AWS_DATE, dateString);
		try {
			String signature = provider.signCloudFront(new String(ctx.getAccessPublic(), "utf-8"), ctx.getAccessPrivate(), dateString);
			
			method.addHeader(AWSCloud.P_CFAUTH, signature);
		} 
		catch (UnsupportedEncodingException e) {
			logger.error(e);
			e.printStackTrace();
			throw new InternalException(e);
		}
		if( headers != null ) {
			for( Map.Entry<String, String> entry : headers.entrySet() ) {
				method.addHeader(entry.getKey(), entry.getValue());
			}
		}
        if( body != null ) {
            try {
                ((HttpEntityEnclosingRequestBase)method).setEntity(new StringEntity(body, "application/xml", "utf-8"));
            }
            catch( UnsupportedEncodingException e ) {
                throw new InternalException(e);
            }
        }
		attempts++;
        client = provider.getClient();
        CloudFrontResponse response = new CloudFrontResponse();

        HttpResponse httpResponse;
        int status;

        try {
            try {
                APITrace.trace(provider, action.toString());
                httpResponse = client.execute(method);
                status = httpResponse.getStatusLine().getStatusCode();

            }
            catch( IOException e ) {
                logger.error(e);
                e.printStackTrace();
                throw new InternalException(e);
            }
            Header header = httpResponse.getFirstHeader("ETag");

            if( header != null ) {
                response.etag = header.getValue();
            }
            else {
                response.etag = null;
            }
            if( status == HttpStatus.SC_OK || status == HttpStatus.SC_CREATED || status == HttpStatus.SC_ACCEPTED ) {
                try {
                    HttpEntity entity = httpResponse.getEntity();

                    if( entity == null ) {
                        throw new CloudFrontException(status, null, null, "NoResponse", "No response body was specified");
                    }
                    InputStream input;

                    try {
                        input = entity.getContent();
                    }
                    catch( IOException e ) {
                        throw new CloudException(e);
                    }
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
                        catch( InterruptedException ignore ) { }
                        return invoke(args);
                    }
                }
                try {
                    HttpEntity entity = httpResponse.getEntity();

                    if( entity == null ) {
                        throw new CloudFrontException(status, null, null, "NoResponse", "No response body was specified");
                    }
                    InputStream input;

                    try {
                        input = entity.getContent();
                    }
                    catch( IOException e ) {
                        throw new CloudException(e);
                    }
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
        } finally {
            if (client != null) {
                client.getConnectionManager().shutdown();
            }
        }
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
	            
			//System.out.println(sb.toString());
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
