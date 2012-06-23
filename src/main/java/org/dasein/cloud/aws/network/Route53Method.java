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

package org.dasein.cloud.aws.network;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import javax.annotation.Nonnull;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.DNSSupport;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Route53Method {
    static private final Logger logger = AWSCloud.getLogger(Route53Method.class);

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
	private AWSCloud           provider    = null;
	private String             signature   = null;
	private String             url         = null;
	
	public Route53Method(String operation, AWSCloud provider, String url) throws InternalException {
		this.url = url;
		this.provider = provider;
		this.method = translateMethod(operation);
		dateString = getTimestamp(System.currentTimeMillis());
		try {
		    signature = provider.signAWS3(new String(provider.getContext().getAccessPublic(), "utf-8"), provider.getContext().getAccessPrivate(), dateString);
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

    private Document delete(boolean debug) throws EC2Exception, CloudException, InternalException {
        return invoke(new DeleteMethod(url), debug);
    }
	   
   private Document get(boolean debug) throws EC2Exception, CloudException, InternalException {
        return invoke(new GetMethod(url), debug);
    }
	   
	private Document post(String body, boolean debug) throws EC2Exception, CloudException, InternalException {
	    PostMethod post = new PostMethod(url);
	    
	    if( body != null ) {
	        try {
                post.setRequestEntity(new StringRequestEntity(body, "text/xml", "UTF-8"));
            }
            catch( UnsupportedEncodingException e ) {
                throw new InternalException(e);
            }
	    }
	    return invoke(post, debug);
	}
	
	public Document invoke(String body, boolean debug) throws EC2Exception, CloudException, InternalException {
	    if( method.equals("GET") ) {
	        return get(debug);
	    }
	    else if( method.equals("DELETE") ) {
	        return delete(debug);
	    }
	    else if( method.equals("POST") ) {
	        return post(body, debug);
	    }
	    throw new InternalException("No such method: " + method);
	}
	
	public Document invoke(boolean debug) throws EC2Exception, CloudException, InternalException {
	    if( method.equals("GET") ) {
	        return get(debug);
	    }
	    else if( method.equals("DELETE") ) {
	        return delete(debug);
	    }
	    else if( method.equals("POST") ) {
	        return post(null, debug);
	    }
	    throw new InternalException("No such method: " + method);
	}
	   
	private Document invoke(HttpMethod method, boolean debug) throws EC2Exception, CloudException, InternalException {
		if( logger.isDebugEnabled() ) {
			logger.debug("Talking to server at " + url);
		}
        HttpClientParams httpClientParams = new HttpClientParams();
		
		try {
    		HttpClient client;
    		int status;
    
    		attempts++;
            httpClientParams.setParameter(HttpMethodParams.USER_AGENT, "Dasein Cloud");
            client = new HttpClient(httpClientParams);
            if( provider.getProxyHost() != null ) {
                client.getHostConfiguration().setProxy(provider.getProxyHost(), provider.getProxyPort());
            }
    		method.addRequestHeader("Content-Type", "text/xml");
    		method.addRequestHeader("x-amz-date", dateString);
    		method.addRequestHeader("Date", dateString);
    		method.addRequestHeader("X-Amzn-Authorization", signature);
    		try {
    			status = client.executeMethod(method);
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
    		if( status == HttpStatus.SC_OK || status == HttpStatus.SC_ACCEPTED || status == HttpStatus.SC_CREATED ) {
    			try {
    				InputStream input = method.getResponseBodyAsStream();
    
    				try {
    					return parseResponse(input, debug);
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
    		else if( status == HttpStatus.SC_FORBIDDEN ) {
    		    String msg = "API Access Denied (403)";
    		    
                try {
                    InputStream input = method.getResponseBodyAsStream();
    
                    try {
                        BufferedReader in = new BufferedReader(new InputStreamReader(input));
                        StringBuffer sb = new StringBuffer();
                        String line;
                            
                        while( (line = in.readLine()) != null ) {
                            sb.append(line);
                            sb.append("\n");
                        }
                        //System.out.println(sb);
                        try {
                            Document doc = parseResponse(sb.toString(), debug);
                            
                            if( doc != null ) {
                                NodeList blocks = doc.getElementsByTagName("Error");
                                String code = null, message = null, requestId = null;
            
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
                                throw new EC2Exception(status, requestId, code, message);
                            }
                        }
                        catch( RuntimeException ignore  ) {
                            // ignore me
                        }
                        catch( Error ignore  ) {
                            // ignore me
                        }
                        msg = msg + ": " + sb.toString().trim().replaceAll("\n", " / ");
                    }
                    finally {
                        input.close();
                    }
                } 
                catch( IOException ignore ) {
                    // ignore me
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
    						try {
			                    InputStream input = method.getResponseBodyAsStream();
			    
			                    try {
			                        BufferedReader in = new BufferedReader(new InputStreamReader(input));
			                        StringBuffer sb = new StringBuffer();
			                        String line;
			                            
			                        while( (line = in.readLine()) != null ) {
			                            sb.append(line);
			                            sb.append("\n");
			                        }
			                        msg = msg + "Response from server was:\n" + sb.toString();
			                    }
			                    finally {
			                        input.close();
			                    }
			                } 
			                catch( IOException ignore ) {
			                    // ignore me
			                }
			                catch( RuntimeException ignore ) {
			                    // ignore me
			                }
			                catch( Error ignore ) {
			                    // ignore me
			                }
    					}
    					logger.error(msg);
    					throw new CloudException(msg);
    				}
    				else {
    					try { Thread.sleep(5000L); }
    					catch( InterruptedException e ) { }
    					try {
    					    return invoke((HttpMethod)method.getClass().newInstance(), false);
    					}
    					catch( Throwable t ) {
    					    throw new InternalException(t);
    					}
    				}
    			}
    			try {
    				InputStream input = method.getResponseBodyAsStream();
    				Document doc;
    
    				try {
    					doc = parseResponse(input, debug);
    				}
    				finally {
    					input.close();
    				}
    				if( doc != null ) {
    					NodeList blocks = doc.getElementsByTagName("Error");
    					String code = null, message = null, requestId = null;
    
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
    					throw new EC2Exception(status, requestId, code, message);
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
	
	private Document parseResponse(String responseBody, boolean debug) throws CloudException, InternalException {
	    try {
	        if( debug ) { System.out.println(responseBody); }
            ByteArrayInputStream bas = new ByteArrayInputStream(responseBody.getBytes());
            
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder parser = factory.newDocumentBuilder();
            Document doc = parser.parse(bas);

            bas.close();
            return doc;
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
			StringBuffer sb = new StringBuffer();
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
