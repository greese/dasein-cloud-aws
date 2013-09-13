/**
 * Copyright (C) 2009-2013 Dell, Inc.
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

package org.dasein.cloud.aws;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.aws.compute.EC2Method;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.dc.DataCenterServices;
import org.dasein.cloud.dc.Region;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

public class RegionsAndZones implements DataCenterServices {
	static private final Logger logger = Logger.getLogger(RegionsAndZones.class);

	static public final String DESCRIBE_AVAILABILITY_ZONES = "DescribeAvailabilityZones";
	static public final String DESCRIBE_REGIONS            = "DescribeRegions";
	
	private AWSCloud provider = null;

    private String oneRegionId;
    private String oneZoneId;

	RegionsAndZones(AWSCloud provider) {
		this.provider = provider;
        if( (provider.getEC2Provider().isStorage() && "google".equalsIgnoreCase(provider.getProviderName())) ) {
            oneRegionId = "us";
            oneZoneId = "us1";
        }
        else {
            oneRegionId = "region-1";
            oneZoneId = "zone-1";
        }
	}

    private @Nonnull DataCenter getZone() {
        DataCenter dc = new DataCenter() ;

        dc.setActive(true);
        dc.setAvailable(true);
        dc.setName(oneZoneId);
        dc.setProviderDataCenterId(oneZoneId);
        dc.setRegionId(oneRegionId);
        return dc;
    }

	@Override
	public @Nullable DataCenter getDataCenter(@Nonnull String zoneId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.getDataCenter");
        try {
            if( provider.getEC2Provider().isStorage() ) {
                return (zoneId.equals(oneZoneId) ? getZone() : null);
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), DESCRIBE_AVAILABILITY_ZONES);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("ZoneName", zoneId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && code.startsWith("InvalidZone") ) {
                    return null;
                }
                if( code != null && code.equals("InvalidParameterValue") ) {
                    String message = e.getMessage();

                    if( message != null && message.startsWith("Invalid availability") ) {
                        return null;
                    }
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("availabilityZoneInfo");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList zones = blocks.item(i).getChildNodes();

                for( int j=0; j<zones.getLength(); j++ ) {
                    Node region = zones.item(j);

                    if( region.getNodeName().equals("item") ) {
                        DataCenter dc = toDataCenter(null, zones.item(j));

                        if( dc != null && dc.getProviderDataCenterId().equals(zoneId) ) {
                            if( dc.getRegionId() == null ) {
                                for( Region r : listRegions() ) {
                                    for( DataCenter d : listDataCenters(r.getProviderRegionId()) ) {
                                        if( d.getProviderDataCenterId().equals(dc.getProviderDataCenterId()) ) {
                                            dc.setRegionId(r.getProviderRegionId());
                                            break;
                                        }
                                    }
                                    if( dc.getRegionId() != null ) {
                                        break;
                                    }
                                }
                            }
                            return dc;
                        }
                    }
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
	}

	@Override
	public String getProviderTermForDataCenter(Locale locale) {
		return "availability zone";
	}

	@Override
	public String getProviderTermForRegion(Locale locale) {
		return "region";
	}

    private @Nonnull Region getRegion() {
        Region region = new Region();

        region.setActive(true);
        region.setAvailable(true);
        region.setJurisdiction("US");
        region.setName(oneRegionId);
        region.setProviderRegionId(oneRegionId);
        return region;
    }

	@Override
	public Region getRegion(String regionId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.getRegion");
        try {
            if( provider.getEC2Provider().isStorage() ) {
                return (regionId.equals(oneRegionId) ? getRegion() : null);
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), DESCRIBE_REGIONS);
            NodeList blocks, regions;
            EC2Method method;
            Document doc;

            parameters.put("RegionName.1", regionId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && code.startsWith("InvalidRegion") ) {
                    return null;
                }
                if( code != null && code.equals("InvalidParameterValue") ) {
                    String message = e.getMessage();

                    if( message != null && message.startsWith("Invalid region") ) {
                        return null;
                    }
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("regionInfo");
            for( int i=0; i<blocks.getLength(); i++ ) {
                regions = blocks.item(i).getChildNodes();
                for( int j=0; j<regions.getLength(); j++ ) {
                    Node region = regions.item(j);

                    if( region.getNodeName().equals("item") ) {
                        Region r = toRegion(region);

                        if( r != null ) {
                            return r;
                        }
                    }
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
	}

	@Override
	public Collection<DataCenter> listDataCenters(String regionId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.listDataCenters");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            Cache<DataCenter> cache = null;
            Collection<DataCenter> dataCenters;
            String originalRegionId = ctx.getRegionId();

            if( regionId.equals(originalRegionId) ) {
                cache = Cache.getInstance(provider, "dataCenters", DataCenter.class, CacheLevel.REGION_ACCOUNT);
                dataCenters = (Collection<DataCenter>)cache.get(ctx);
                if( dataCenters != null ) {
                    return dataCenters;
                }
            }
            if( provider.getEC2Provider().isStorage() ) {
                if( regionId.equals(oneRegionId) ) {
                    return Collections.singletonList(getZone());
                }
                throw new CloudException("No such region: " + regionId);
            }
            try {
                ctx.setRegionId(regionId);
                Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), DESCRIBE_AVAILABILITY_ZONES);
                EC2Method method = new EC2Method(provider, provider.getEc2Url(), parameters);
                NodeList blocks;
                Document doc;

                dataCenters = new ArrayList<DataCenter>();
                try {
                    doc = method.invoke();
                }
                catch( EC2Exception e ) {
                    logger.error(e.getSummary());
                    throw new CloudException(e);
                }
                blocks = doc.getElementsByTagName("availabilityZoneInfo");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    NodeList zones = blocks.item(i).getChildNodes();

                    for( int j=0; j<zones.getLength(); j++ ) {
                        Node region = zones.item(j);

                        if( region.getNodeName().equals("item") ) {
                            dataCenters.add(toDataCenter(regionId, zones.item(j)));
                        }
                    }
                }
                if( cache != null ) {
                    cache.put(ctx, dataCenters);
                }
                return dataCenters;
            }
            finally {
                ctx.setRegionId(originalRegionId);
            }
        }
        finally {
            APITrace.end();
        }
	}

	@Override
	public Collection<Region> listRegions() throws InternalException, CloudException {
        APITrace.begin(provider, "DC.listRegions");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            Cache<Region> cache = Cache.getInstance(provider, "regions", Region.class, CacheLevel.CLOUD_ACCOUNT);
            Collection<Region> regions = (Collection<Region>)cache.get(ctx);

            if( regions != null ) {
                return regions;
            }
            if( provider.getEC2Provider().isStorage() ) {
                return Collections.singletonList(getRegion());
            }
            regions = new ArrayList<Region>();

            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), DESCRIBE_REGIONS);
            EC2Method method = new EC2Method(provider, provider.getEc2Url(), parameters);
            NodeList blocks, nodes;
            Document doc;

            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                e.printStackTrace();
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("regionInfo");
            for( int i=0; i<blocks.getLength(); i++ ) {
                nodes = blocks.item(i).getChildNodes();
                for( int j=0; j<nodes.getLength(); j++ ) {
                    Node region = nodes.item(j);

                    if( region.getNodeName().equals("item") ) {
                        Region r = toRegion(nodes.item(j));

                        if( provider.getEC2Provider().isEucalyptus() ) {
                            if( r.getProviderRegionId().equalsIgnoreCase("eucalyptus") ) {
                                regions.add(r);
                            }
                        }
                        else {
                            regions.add(r);
                        }
                    }
                }
            }
            cache.put(ctx, regions);
            return regions;
        }
        finally {
            APITrace.end();
        }
	}

	Map<String,String> mapRegions(String url) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.mapRegions");
        try {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), DESCRIBE_REGIONS);
            HashMap<String,String> results = new HashMap<String,String>();
            EC2Method method = new EC2Method(provider, url, parameters);
            NodeList blocks, regions;
            Document doc;

            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("regionInfo");
            for( int i=0; i<blocks.getLength(); i++ ) {
                regions = blocks.item(i).getChildNodes();
                for( int j=0; j<regions.getLength(); j++ ) {
                    Node region = regions.item(j);

                    if( region.getNodeName().equals("item") ) {
                        NodeList data = region.getChildNodes();
                        String name = null, endpoint = null;

                        for( int k=0; k<data.getLength(); k++ ) {
                            Node item = data.item(k);

                            if( item.getNodeName().equals("regionName") ) {
                                name = item.getFirstChild().getNodeValue();
                            }
                            else if( item.getNodeName().equals("regionEndpoint") ) {
                                endpoint = item.getFirstChild().getNodeValue();
                            }
                        }
                        if( name != null && endpoint != null ) {
                            logger.debug(name + "=" + endpoint);
                            results.put(name, endpoint);
                        }
                    }
                }
            }
            return results;
        }
        finally {
            APITrace.end();
        }
	}
	
	private DataCenter toDataCenter(String regionId, Node zone) throws CloudException {
		NodeList data = zone.getChildNodes();
		DataCenter dc = new DataCenter();
		
		dc.setActive(true);
		dc.setAvailable(false);
		dc.setRegionId(regionId);
		for( int i=0; i<data.getLength(); i++ ) {
			Node item = data.item(i);
			String name = item.getNodeName();
			
			if( name.equals("zoneName") ) {
				String value = item.getFirstChild().getNodeValue().trim();
				
				dc.setName(value);
				dc.setProviderDataCenterId(value);
			}
			else if( name.equals("zoneState") ) {
				String value = item.getFirstChild().getNodeValue();

				if( !provider.getEC2Provider().isAWS() ) {
				    dc.setAvailable(true);
				}
				else {
				    dc.setAvailable(value != null && value.trim().equalsIgnoreCase("available"));
				}
			}
			else if( name.equals("regionName") ) {
			    if( item.hasChildNodes() ) {
			        String value = item.getFirstChild().getNodeValue();
				
			        if( value != null ) {
			            dc.setRegionId(value.trim());
			        }
				}
			}
		}
		if( dc.getName() == null ) {
			throw new CloudException("Availability zone info is incomplete for " + dc.getProviderDataCenterId() + ".");
		}
		return dc;
	}
	
	private Region toRegion(Node region) throws CloudException {
		String name = null, endpoint = null;
		NodeList data;
            	
		data = region.getChildNodes();
		for( int i=0; i<data.getLength(); i++ ) {
			Node item = data.item(i);
			
			if( item.getNodeName().equals("regionName") ) {
				name = item.getFirstChild().getNodeValue();
			}
			else if( item.getNodeName().equals("regionEndpoint") ) {
				endpoint = item.getFirstChild().getNodeValue();
			}
		}
		if( name == null || endpoint == null ) {
			throw new CloudException("Invalid region data.");
		}
		Region r = new Region();
		r.setActive(true);
		r.setAvailable(true);
		r.setName(name);
		r.setProviderRegionId(name);
		if( name.startsWith("eu") ) {
		    r.setJurisdiction("EU");
		}
		else if( name.startsWith("ap-northeast") ) {
		    r.setJurisdiction("JP");
		}
		else if( name.startsWith("ap-southeast") ) {
            if( name.equals("ap-southeast-1") ) {
                r.setJurisdiction("SG");
            }
            else {
                r.setJurisdiction("AU");
            }
		}
        else if( name.startsWith("sa-east") ) {
            r.setJurisdiction("BR");
        }
		else {
		    r.setJurisdiction("US");
		}
		return r;
	}
}
