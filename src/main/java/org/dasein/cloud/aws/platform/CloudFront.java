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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.platform.CloudFrontMethod.CloudFrontResponse;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.platform.CDNCapabilities;
import org.dasein.cloud.platform.CDNSupport;
import org.dasein.cloud.platform.Distribution;
import org.dasein.cloud.storage.Blob;
import org.dasein.cloud.util.APITrace;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class CloudFront implements CDNSupport {
	static private final Logger logger = AWSCloud.getLogger(CloudFront.class);
    private volatile transient CloudFrontCapabilities capabilities;
	private AWSCloud provider = null;
	
	CloudFront(AWSCloud provider) {
		this.provider = provider;
	}
	
	@Override
	public @Nonnull String create(@Nonnull String bucket, @Nonnull String name, boolean active, @CheckForNull String ... cnames) throws InternalException, CloudException {
        APITrace.begin(provider, "CDN.create");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was established for this request");
            }
            CloudFrontResponse response;
            CloudFrontMethod method;
            NodeList blocks;

            provider.getStorageServices().getBlobStoreSupport().makePublic(bucket);
            for( Blob file : provider.getStorageServices().getBlobStoreSupport().list(bucket) ) {
                if( !file.isContainer() ) {
                    provider.getStorageServices().getBlobStoreSupport().makePublic(file.getBucketName(), file.getObjectName());
                }
            }
            method = new CloudFrontMethod(provider, CloudFrontAction.CREATE_DISTRIBUTION, null, toConfigXml(bucket, name, null, null, null, active, cnames));
            try {
                response = method.invoke();
            }
            catch( CloudFrontException e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = response.document.getElementsByTagName("Distribution");
            for( int i=0; i<blocks.getLength(); i++ ) {
                Distribution dist = toDistributionFromInfo(ctx, blocks.item(i));

                if( dist != null ) {
                    String id =  dist.getProviderDistributionId();

                    if( id != null ) {
                        return id;
                    }
                }
            }
            throw new CloudException("No CDN distribution was created and no error was reported");
        }
        finally {
            APITrace.end();
        }
	}

	@Override
	public void delete(@Nonnull String distributionId) throws InternalException, CloudException {
        APITrace.begin(provider, "CDN.delete");
        try {
            Distribution distribution = getDistribution(distributionId);

            if( distribution == null ) {
                throw new CloudException("No such distribution: " + distributionId);
            }
            if( distribution.isActive() ) {
                String name = distribution.getName();

                if( name == null ) {
                    name = distributionId;
                }
                update(distributionId, name, false, distribution.getAliases());
            }
            while( true ) {
                try { Thread.sleep(10000L); }
                catch( InterruptedException e ) { /* ignore */ }
                distribution = getDistribution(distributionId);
                if( distribution == null || !distribution.isActive() ) {
                    break;
                }
            }
            while( true ) {
                HashMap<String,String> headers = new HashMap<String,String>();
                CloudFrontMethod method;
                String etag;

                etag = (String)getDistributionWithEtag(distributionId)[1];
                headers.put("If-Match", etag);
                method = new CloudFrontMethod(provider, CloudFrontAction.DELETE_DISTRIBUTION, headers, null);
                try {
                    method.invoke(distributionId);
                    return;
                }
                catch( CloudFrontException e ) {
                    String code = e.getCode();

                    if( code != null && code.equals("DistributionNotDisabled") ) {
                        try { Thread.sleep(10000L); }
                        catch( InterruptedException interrupt ) { /* ignore */ }
                    }
                    else {
                        logger.error(e.getSummary());
                        throw new CloudException(e);
                    }
                }
            }
        }
        finally {
            APITrace.end();
        }
	}

    @Override
    public @Nonnull CDNCapabilities getCapabilities() throws InternalException, CloudException {
        if( capabilities == null ) {
            capabilities = new CloudFrontCapabilities(provider);
        }
        return capabilities;
    }

    @Override
	public @Nullable Distribution getDistribution(@Nonnull String distributionId) throws InternalException, CloudException {
        APITrace.begin(provider, "CDN.getDistribution");
        try {
            Object[] parts = getDistributionWithEtag(distributionId);
        
            if( parts.length < 1 ) {
                return null;
            }
            return (Distribution)parts[0];
        }
        finally {
            APITrace.end();
        }
	}
	
	private @Nonnull Object[] getDistributionWithEtag(@Nonnull String distributionId) throws InternalException, CloudException {
        APITrace.begin(provider, "CDN.getDistributionWithEtag");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was established for this request");
            }
            CloudFrontMethod method = new CloudFrontMethod(provider, CloudFrontAction.GET_DISTRIBUTION, null, null);
            CloudFrontResponse response;
            NodeList blocks;

            try {
                response = method.invoke(distributionId);
            }
            catch( CloudFrontException e ) {
                String code = e.getCode();

                if( code != null && code.equals("NoSuchDistribution") ) {
                    return new Object[] { null, null, null };
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = response.document.getElementsByTagName("Distribution");
            for( int i=0; i<blocks.getLength(); i++ ) {
                Distribution dist = toDistributionFromInfo(ctx, blocks.item(i));

                if( dist != null && distributionId.equals(dist.getProviderDistributionId()) ) {
                    String callerReference = null;

                    blocks = response.document.getElementsByTagName("CallerReference");
                    if( blocks.getLength() > 0 ) {
                        callerReference = blocks.item(0).getFirstChild().getNodeValue();
                    }
                    return new Object[] { dist, response.etag, callerReference };
                }
            }
            return new Object[0];
        }
        finally {
            APITrace.end();
        }
	}

	@Override
    @Deprecated
	public @Nonnull String getProviderTermForDistribution(@Nonnull Locale locale) {
        try {
            return getCapabilities().getProviderTermForDistribution(locale);
        } catch( InternalException e ) {
        } catch( CloudException e ) {
        }
        return "distribution"; // legacy
    }

	@Override
	public boolean isSubscribed() throws InternalException, CloudException {
        APITrace.begin(provider, "CDN.isSubscribed");
        try {
            CloudFrontMethod method = new CloudFrontMethod(provider, CloudFrontAction.LIST_DISTRIBUTIONS, null, null);

            try {
                method.invoke();
                return true;
            }
            catch( CloudFrontException e ) {
                if( e.getStatus() == HttpStatus.SC_UNAUTHORIZED || e.getStatus() == HttpStatus.SC_FORBIDDEN ) {
                    return false;
                }
                String code = e.getCode();

                if( code != null && (code.equals("SubscriptionCheckFailed") || code.equals("AuthFailure") || code.equals("SignatureDoesNotMatch") || code.equals("InvalidClientTokenId") || code.equals("OptInRequired")) ) {
                    return false;
                }
                logger.warn(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
	}
	
	@Override
	public @Nonnull Collection<Distribution> list() throws InternalException, CloudException {
        APITrace.begin(provider, "CDN.list");
        try {
            CloudFrontMethod method = new CloudFrontMethod(provider, CloudFrontAction.LIST_DISTRIBUTIONS, null, null);
            ArrayList<Distribution> list = new ArrayList<Distribution>();
            CloudFrontResponse response;
            NodeList blocks;

            try {
                response = method.invoke();
            }
            catch( CloudFrontException e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = response.document.getElementsByTagName("DistributionSummary");
            for( int i=0; i<blocks.getLength(); i++ ) {
                Distribution dist = toDistributionFromSummary(blocks.item(i));

                if( dist != null ) {
                    list.add(dist);
                }
            }
            return list;
        }
        finally {
            APITrace.end();
        }
	}

    @Override
    public @Nonnull Collection<ResourceStatus> listDistributionStatus() throws InternalException, CloudException {
        APITrace.begin(provider, "CDN.listDistributionStatus");
        try {
            CloudFrontMethod method = new CloudFrontMethod(provider, CloudFrontAction.LIST_DISTRIBUTIONS, null, null);
            ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();
            CloudFrontResponse response;
            NodeList blocks;

            try {
                response = method.invoke();
            }
            catch( CloudFrontException e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = response.document.getElementsByTagName("DistributionSummary");
            for( int i=0; i<blocks.getLength(); i++ ) {
                ResourceStatus status = toStatus(blocks.item(i));

                if( status != null ) {
                    list.add(status);
                }
            }
            return list;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        if( action.equals(CDNSupport.ANY) ) {
            return new String[] { CloudFrontMethod.CF_PREFIX + "*" };
        }
        else if( action.equals(CDNSupport.CREATE_DISTRIBUTION) ) {
            return new String[] { CloudFrontMethod.CF_PREFIX + "CreateDistribution" };
        }
        else if( action.equals(CDNSupport.GET_DISTRIBUTION) ) {
            return new String[] { CloudFrontMethod.CF_PREFIX + "GetDistribution"};
        }
        else if( action.equals(CDNSupport.LIST_DISTRIBUTION) ) {
            return new String[] { CloudFrontMethod.CF_PREFIX + "ListDistributions" };
        }
        else if( action.equals(CDNSupport.REMOVE_DISTRIBUTION) ) {
            return new String[] { CloudFrontMethod.CF_PREFIX + "DeleteDistribution" };
        }
        return new String[0];
    }

	@Override
	public void update(@Nonnull String distributionId, @Nonnull String name, boolean active, @CheckForNull String ... cnames) throws InternalException, CloudException {
		updateWithReturn(distributionId, name, active, cnames);
	}

	private String updateWithReturn(@Nonnull String distributionId, @Nonnull String name, boolean active, @CheckForNull String ... cnames) throws InternalException, CloudException {
        APITrace.begin(provider, "CDN.update");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was established for this request");
            }
            HashMap<String,String> headers = new HashMap<String,String>();
            Object[] distData = getDistributionWithEtag(distributionId);
            Distribution distribution = (Distribution)distData[0];
            String location = (distribution == null ? null : distribution.getLocation());
            String[] parts = (location == null ? new String[0] : location.split("\\."));
            CloudFrontResponse response;
            String bucket = parts[0];
            CloudFrontMethod method;
            NodeList blocks;

            headers.put("If-Match", (String)distData[1]);
            String logDirectory = (distribution == null ? null : distribution.getLogDirectory());
            String logName = (distribution == null ? null : distribution.getLogName());

            method = new CloudFrontMethod(provider, CloudFrontAction.UPDATE_DISTRIBUTION, headers, toConfigXml(bucket, name, (String)distData[2], logDirectory, logName, active, cnames));
            try {
                response = method.invoke(distributionId, "config");
            }
            catch( CloudFrontException e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = response.document.getElementsByTagName("Distribution");
            for( int i=0; i<blocks.getLength(); i++ ) {
                Distribution dist = toDistributionFromInfo(ctx, blocks.item(i));

                if( dist != null ) {
                    return response.etag;
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
	}
	
	private @Nonnull String toConfigXml(@Nonnull String bucket, @Nonnull String name, @Nullable String callerReference, @Nullable String logDirectory, @Nullable String logName, boolean active, @CheckForNull String ... cnames) {
		StringBuilder xml = new StringBuilder();
	
		xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n");
		xml.append("<DistributionConfig xmlns=\"http://cloudfront.amazonaws.com/doc/2009-04-02/\">\r\n");
		xml.append("<Origin>");
		xml.append(toXml(bucket));
		xml.append(".s3.amazonaws.com");
		xml.append("</Origin>\r\n");
		xml.append("<CallerReference>");
		xml.append(callerReference == null ? String.valueOf(System.currentTimeMillis()) : callerReference);
		xml.append("</CallerReference>\r\n");
		if( cnames != null ) {
			for( String cname : cnames ) {
				xml.append("<CNAME>");
				xml.append(toXml(cname));
				xml.append("</CNAME>\r\n");
			}
		}
		xml.append("<Comment>");
		xml.append(toXml(name));
		xml.append("</Comment>\r\n");
		xml.append("<Enabled>");
		xml.append(String.valueOf(active));
		xml.append("</Enabled>");
		if( logDirectory != null && logName != null ) {
		    xml.append("<Logging>\r\n");
		    xml.append("<Bucket>");
		    xml.append(logDirectory);
		    xml.append("</Bucket>\r\n");
		    xml.append("<Prefix>");
		    xml.append(logName);
		    xml.append("</Prefix>\r\n");
		    xml.append("</Logging>\r\n");
		}
		xml.append("</DistributionConfig>\r\n");
		return xml.toString();
	}
	
	private @Nullable Distribution toDistributionFromInfo(@Nonnull ProviderContext ctx, @Nullable Node node) {
        if( node == null ) {
            return null;
        }
		ArrayList<String> cnames = new ArrayList<String>();
		Distribution distribution = new Distribution();
		NodeList attrs = node.getChildNodes();
		
		distribution.setProviderOwnerId(ctx.getAccountNumber());
		for( int i=0; i<attrs.getLength(); i++ ) {
			Node attr = attrs.item(i);
			String name;
			
			name = attr.getNodeName();
			if( name.equals("Id") ) {
				distribution.setProviderDistributionId(attr.getFirstChild().getNodeValue().trim());
			}
			else if( name.equals("Status") ) {
				String s = attr.getFirstChild().getNodeValue();
				
				distribution.setDeployed(s != null && s.trim().equalsIgnoreCase("deployed"));
			}
			else if( name.equals("DomainName") ) {
				distribution.setDnsName(attr.getFirstChild().getNodeValue().trim());
			}
			else if( name.equals("DistributionConfig") ) {
				NodeList configList = attr.getChildNodes();
				
				for( int j=0; j<configList.getLength(); j++ ) {
					Node config = configList.item(j);
					
					if( config.getNodeName().equals("Enabled") ) {
						String s = config.getFirstChild().getNodeValue();
					
						distribution.setActive(s != null && s.trim().equalsIgnoreCase("true"));
					}
					else if( config.getNodeName().equals("Origin") ) {
						String origin = config.getFirstChild().getNodeValue().trim();
					
						distribution.setLocation(origin);
					}
					else if( config.getNodeName().equals("CNAME") ) {
						cnames.add(config.getFirstChild().getNodeValue().trim());
					}
					else if( config.getNodeName().equals("Logging") ) {
					    if( config.hasChildNodes() ) {
					        NodeList logging = config.getChildNodes();
					        
					        for( int k=0; k<logging.getLength(); k++ ) {
					            Node logInfo = logging.item(k);
					            
                                if( logInfo.getNodeName().equals("Bucket") ) {
                                    if( logInfo.hasChildNodes() ) {
                                        distribution.setLogDirectory(logInfo.getFirstChild().getNodeValue());
                                    }
                                }
                                else if( logInfo.getNodeName().equals("Prefix") ) {
                                    if( logInfo.hasChildNodes() ) {
                                        distribution.setLogName(logInfo.getFirstChild().getNodeValue());
                                    }                                    
                                }
					        }
					    }
					}
					else if( config.getNodeName().equals("Comment") ) {
						if( config.hasChildNodes() ) {
							String comment = config.getFirstChild().getNodeValue();
					
							if( comment != null ) {
								distribution.setName(comment.trim());
							}
						}
					}
				}
			}
		}
		if( distribution.getName() == null ) {
            String name = distribution.getDnsName();

            if( name == null ) {
                name = distribution.getProviderDistributionId();
                if( name == null ) {
                    return null;
                }
            }
	        distribution.setName(name);
		}
		String[] aliases = new String[cnames.size()];
		int i = 0;
		for( String cname : cnames ) {
			aliases[i++] = cname;
		}
		distribution.setAliases(aliases);
		return distribution;
	}
	
	private Distribution toDistributionFromSummary(Node node) {
		ArrayList<String> cnames = new ArrayList<String>();
		Distribution distribution = new Distribution();
		NodeList attrs = node.getChildNodes();

        //noinspection ConstantConditions
        distribution.setProviderOwnerId(provider.getContext().getAccountNumber());
		for( int i=0; i<attrs.getLength(); i++ ) {
			Node attr = attrs.item(i);
			String name;
			
			name = attr.getNodeName();
			if( name.equals("Id") ) {
				distribution.setProviderDistributionId(attr.getFirstChild().getNodeValue().trim());
			}
			else if( name.equals("Status") ) {
				String s = attr.getFirstChild().getNodeValue();
				
				distribution.setDeployed(s != null && s.trim().equalsIgnoreCase("deployed"));
			}
			else if( name.equals("Enabled") ) {
				String s = attr.getFirstChild().getNodeValue();
				
				distribution.setActive(s != null && s.trim().equalsIgnoreCase("true"));
			}
			else if( name.equals("DomainName") ) {
				distribution.setDnsName(attr.getFirstChild().getNodeValue().trim());
			}
			else if( name.equals("Origin") ) {
				String origin = attr.getFirstChild().getNodeValue().trim();
				
				distribution.setLocation(origin);
			}
			else if( name.equals("CNAME") ) {
				cnames.add(attr.getFirstChild().getNodeValue().trim());
			}
			else if( name.equals("Comment") ) {
				if( attr.hasChildNodes() ) {
					String comment = attr.getFirstChild().getNodeValue();
				
					if( comment != null ) {
						distribution.setName(comment.trim());
					}
				}
			}
		}
		if( distribution.getName() == null ) {
            String name = distribution.getDnsName();

            if( name == null ) {
                name = distribution.getProviderDistributionId();
                if( name == null ) {
                    return null;
                }
            }
            distribution.setName(name);
        }
		String[] aliases = new String[cnames.size()];
		int i = 0;
		for( String cname : cnames ) {
			aliases[i++] = cname;
		}
		distribution.setAliases(aliases);
		return distribution;
	}

    private @Nullable ResourceStatus toStatus(@Nullable Node node) {
        if( node == null ) {
            return null;
        }
        NodeList attrs = node.getChildNodes();

        String distributionId = null;
        boolean deployed = false;

        for( int i=0; i<attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;

            name = attr.getNodeName();
            if( name.equals("Id") ) {
                distributionId = attr.getFirstChild().getNodeValue().trim();
            }
            else if( name.equals("Status") ) {
                String s = attr.getFirstChild().getNodeValue();

                deployed = (s != null && s.trim().equalsIgnoreCase("deployed"));
            }
        }
        if( distributionId == null ) {
            return null;
        }
        return new ResourceStatus(distributionId, deployed);
    }

	private String toXml(String value) {
		return value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll("\"", "&quot;").replaceAll("'", "&apos;");
	}
}
