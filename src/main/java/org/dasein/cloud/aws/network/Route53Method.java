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

package org.dasein.cloud.aws.network;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.DNSSupport;
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
import java.util.Date;
import java.util.TimeZone;

public class Route53Method {
    static private final Logger logger = AWSCloud.getLogger(Route53Method.class);
    static private final Logger wire = AWSCloud.getWireLogger(Route53Method.class);

    static public final String R53_PREFIX = "route53";

    static public final String CREATE_HOSTED_ZONE = "CreateHostedZone";
    static public final String DELETE_HOSTED_ZONE = "DeleteHostedZone";
    static public final String GET_HOSTED_ZONE    = "GetHostedZone";
    static public final String LIST_HOSTED_ZONES  = "ListHostedZones";
    
    static public final String CHANGE_RESOURCE_RECORD_SETS = "ChangeResourceRecordSets";
    static public final String GET_CHANGE                  = "GetChange";
    static public final String LIST_RESOURCE_RECORD_SETS   = "ListResourceRecordSets";
    
    static public @Nonnull ServiceAction[] asRoute53ServiceAction(@Nonnull String action) {
        if( action.equals(CREATE_HOSTED_ZONE) ) {
            return new ServiceAction[] { DNSSupport.CREATE_ZONE };
        }
        else if( action.equals(DELETE_HOSTED_ZONE) ) {
            return new ServiceAction[] { DNSSupport.REMOVE_ZONE };
        }
        else if( action.equals(GET_HOSTED_ZONE) ) {
            return new ServiceAction[] { DNSSupport.GET_ZONE };
        }
        else if( action.equals(LIST_HOSTED_ZONES) ) {
            return new ServiceAction[] { DNSSupport.LIST_ZONE };
        }
        else if( action.equals(CHANGE_RESOURCE_RECORD_SETS) ) {
            return new ServiceAction[] { DNSSupport.ADD_RECORD, DNSSupport.REMOVE_RECORD };
        }
        else if( action.equals(GET_CHANGE) ) {
            return new ServiceAction[0];
        }
        else if( action.equals(LIST_RESOURCE_RECORD_SETS) ) {
            return new ServiceAction[] { DNSSupport.LIST_RECORD };
        }
        return new ServiceAction[0];
    }

    private int                attempts    = 0;
	private String             dateString  = null;
	private String             method      = null;
    private String             operation   = null;
	private AWSCloud           provider    = null;
	private String             signature   = null;
	private String             url         = null;
	
	public Route53Method(String operation, AWSCloud provider, String url) throws InternalException {
		this.url = url;
		this.provider = provider;
        this.operation = operation;
		this.method = translateMethod(operation);
		dateString = getTimestamp(System.currentTimeMillis());

        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new InternalException("No context was specified for this request");
        }
		try {
		    signature = provider.signAWS3(new String(ctx.getAccessPublic(), "utf-8"), ctx.getAccessPrivate(), dateString);
		}
		catch( UnsupportedEncodingException e ) {
		    throw new InternalException(e);
		}
	}

    public String getTimestamp(long timestamp) {
        SimpleDateFormat fmt = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");

        fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        return fmt.format(new Date(timestamp));
    }

    private Document delete() throws EC2Exception, CloudException, InternalException {
        return invokeMethod(new HttpDelete(url));
    }
	   
    private Document get() throws EC2Exception, CloudException, InternalException {
        return invokeMethod(new HttpGet(url));
    }
	   
	private Document post(String body) throws EC2Exception, CloudException, InternalException {
	    HttpPost post = new HttpPost(url);

        if( body != null ) {
            post.setEntity(new StringEntity(body, ContentType.TEXT_XML));
        }
	    return invokeMethod(post);
	}
	
	public Document invoke(String body) throws EC2Exception, CloudException, InternalException {
	    if( method.equals("GET") ) {
	        return get();
	    }
	    else if( method.equals("DELETE") ) {
	        return delete();
	    }
	    else if( method.equals("POST") ) {
	        return post(body);
	    }
	    throw new InternalException("No such method: " + method);
	}
	
	public Document invoke() throws EC2Exception, CloudException, InternalException {
	    return invoke( null );
	}

	private Document invokeMethod(HttpRequestBase method) throws EC2Exception, CloudException, InternalException {
		if( logger.isDebugEnabled() ) {
			logger.debug("Talking to server at " + url);
		}
        HttpClient client = null;
		try {
            client = provider.getClient();
            HttpResponse response;
    		int status;
    
    		attempts++;
    		method.addHeader("Content-Type", "text/xml");
    		method.addHeader("x-amz-date", dateString);
    		method.addHeader("Date", dateString);
    		method.addHeader("X-Amzn-Authorization", signature);
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug(">>> [(" + (new Date()) + ")] -> " + method.getRequestLine() + " >--------------------------------------------------------------------------------------");
                wire.debug(method.getRequestLine().toString());
                for( Header header : method.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            String xml;

            try {
                try {
                    APITrace.trace(provider, operation);
                    response = client.execute(method);
                    if( wire.isDebugEnabled() ) {
                        wire.debug(response.getStatusLine().toString());
                        for( Header header : response.getAllHeaders() ) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                    }
                    status = response.getStatusLine().getStatusCode();
                }
                catch( IOException e ) {
                    logger.error(e);
                    throw new InternalException(e);
                }
                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        xml = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(xml);
                            wire.debug("");
                        }
                    }
                    else {
                        xml = null;
                    }
                }
                catch( IOException e ) {
                    logger.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    throw new CloudException(e);
                }
            }
            finally {
                if( wire.isDebugEnabled() ) {
                    wire.debug("<<< [(" + (new Date()) + ")] -> " + method.getRequestLine() + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
    		if( status == HttpStatus.SC_OK || status == HttpStatus.SC_ACCEPTED || status == HttpStatus.SC_CREATED ) {
                return parseResponse(xml, false);
    		}
    		else if( status == HttpStatus.SC_FORBIDDEN ) {
    		    String msg = "API Access Denied (403)";
    		    
                try {
                    try {
                        Document doc = parseResponse(xml, false);

                        if( doc != null ) {
                            NodeList blocks = doc.getElementsByTagName("Error");
                            String code = null, message = null, requestId = null;

                            if( blocks.getLength() > 0 ) {
                                Node error = blocks.item(0);
                                NodeList attrs = error.getChildNodes();
                                for( int i=0; i<attrs.getLength(); i++ ) {
                                    Node attr = attrs.item(i);

                                    if( attr.getNodeName().equals("Code") ) {
                                        code = attr.getFirstChild().getNodeValue().trim();
                                    }
                                    else if( attr.getNodeName().equals("Message") ) {
                                        message = attr.getFirstChild().getNodeValue().trim();
                                    }
                                }

                            }
                            blocks = doc.getElementsByTagName("RequestID");
                            if( blocks.getLength() > 0 ) {
                                Node id = blocks.item(0);

                                requestId = id.getFirstChild().getNodeValue().trim();
                            }
                            if( message == null ) {
                                throw new CloudException("Unable to identify error condition: " + status + "/" + requestId + "/" + code);
                            }
                            throw EC2Exception.create(status, requestId, code, message);
                        }
                    }
                    catch( RuntimeException ignore  ) {
                        // ignore me
                    }
                    catch( Error ignore  ) {
                        // ignore me
                    }
                    msg = msg + ": " + xml.trim().replaceAll("\n", " / ");
                } 
                catch( RuntimeException ignore ) {
                    // ignore me
                }
                catch( Error ignore ) {
                    // ignore me
                }
    		    throw new CloudException(msg);
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
                            msg = msg + "Response from server was:\n" + xml;
    					}
    					logger.error(msg);
    					throw new CloudException(msg);
    				}
    				else {
    					try { Thread.sleep(5000L); }
    					catch( InterruptedException ignore ) { }
    					try {
    					    return invokeMethod(method.getClass().newInstance());
    					}
    					catch( Throwable t ) {
    					    throw new InternalException(t);
    					}
    				}
    			}
                Document doc;

                doc = parseResponse(xml, false);
                if( doc != null ) {
                    NodeList blocks = doc.getElementsByTagName("Error");
                    String code = null, message = null, requestId = null;

                    if( blocks.getLength() > 0 ) {
                        Node error = blocks.item(0);
                        NodeList attrs = error.getChildNodes();
                        for( int i=0; i<attrs.getLength(); i++ ) {
                            Node attr = attrs.item(i);

                            if( attr.getNodeName().equals("Code") ) {
                                code = attr.getFirstChild().getNodeValue().trim();
                            }
                            else if( attr.getNodeName().equals("Message") ) {
                                message = attr.getFirstChild().getNodeValue().trim();
                            }
                        }

                    }
                    blocks = doc.getElementsByTagName("RequestID");
                    if( blocks.getLength() > 0 ) {
                        Node id = blocks.item(0);

                        requestId = id.getFirstChild().getNodeValue().trim();
                    }
                    if( message == null ) {
                        throw new CloudException("Unable to identify error condition: " + status + "/" + requestId + "/" + code);
                    }
                    throw EC2Exception.create(status, requestId, code, message);
                }
                throw new CloudException("Unable to parse error.");
    		}
        }
        finally {
            if (client != null) {
                client.getConnectionManager().shutdown();
            }
            logger.debug("Done");
        }
	}
	
	private Document parseResponse(String responseBody, boolean debug) throws CloudException, InternalException {
	    try {
	        if( debug ) { System.out.println(responseBody); }
            return XMLParser.parse(new ByteArrayInputStream(responseBody.getBytes("UTF-8")));
	    }
	    catch( IOException e ) {
	        throw new CloudException(e);
	    }
	    catch( ParserConfigurationException e ) {
            throw new CloudException(e);
        }
        catch( SAXException e ) {
            throw new CloudException(e);
        }   
	}
	
	private Document parseResponse(InputStream responseBodyAsStream, boolean debug) throws CloudException, InternalException {
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(responseBodyAsStream));
			StringBuilder sb = new StringBuilder();
			String line;
	            
			while( (line = in.readLine()) != null ) {
				sb.append(line);
				sb.append("\n");
			}
			in.close();
	          
			//System.out.println(sb.toString());
			return parseResponse(sb.toString(), debug);
		}
		catch( IOException e ) {
			throw new CloudException(e);
		}			
	}
	
	private String translateMethod(String operation) {
	    if( operation.equalsIgnoreCase(CREATE_HOSTED_ZONE) ) {
	        return "POST";
	    }
	    else if( operation.equalsIgnoreCase(GET_HOSTED_ZONE) || operation.equalsIgnoreCase(LIST_HOSTED_ZONES) ) {
	        return "GET";
	    }
	    else if( operation.equalsIgnoreCase(DELETE_HOSTED_ZONE) ) {
	        return "DELETE";
	    }
	    else if( operation.equalsIgnoreCase(CHANGE_RESOURCE_RECORD_SETS) ) {
	        return "POST";
	    }
	    else if( operation.equalsIgnoreCase(LIST_RESOURCE_RECORD_SETS) ) {
	        return "GET";
	    }
	    else if( operation.equalsIgnoreCase(GET_CHANGE) ) {
	        return "GET";
	    }
	    return "POST";
	}
}
