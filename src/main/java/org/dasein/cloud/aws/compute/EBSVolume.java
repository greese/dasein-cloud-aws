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

package org.dasein.cloud.aws.compute;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.Volume;
import org.dasein.cloud.compute.VolumeCreateOptions;
import org.dasein.cloud.compute.VolumeProduct;
import org.dasein.cloud.compute.VolumeState;
import org.dasein.cloud.compute.VolumeSupport;
import org.dasein.cloud.compute.VolumeType;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Storage;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class EBSVolume implements VolumeSupport {
	static private final Logger logger = Logger.getLogger(EBSVolume.class);
	
	private AWSCloud provider = null;
	
	EBSVolume(AWSCloud provider) {
		this.provider = provider;
	}
	
	@Override
	public void attach(@Nonnull String volumeId, @Nonnull String toServer, @Nonnull String device) throws InternalException, CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.ATTACH_VOLUME);
		EC2Method method;

		parameters.put("VolumeId", volumeId);
		parameters.put("InstanceId", toServer);
		parameters.put("Device", device);
		method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
        	method.invoke();
        }
        catch( EC2Exception e ) {
        	logger.error(e.getSummary());
        	throw new CloudException(e);
        }
	}
    
	@Override
	public @Nonnull String create(@Nullable String fromSnapshot, @Nonnegative int sizeInGb, @Nonnull String inZone) throws InternalException, CloudException {
        if( fromSnapshot != null ) {
            return createVolume(VolumeCreateOptions.getInstanceForSnapshot(fromSnapshot, new Storage<Gigabyte>(sizeInGb, Storage.GIGABYTE), "dsn-auto-volume", "dsn-auto-volume").inDataCenter(inZone));
        }
        else {
            return createVolume(VolumeCreateOptions.getInstance(new Storage<Gigabyte>(sizeInGb, Storage.GIGABYTE), "dsn-auto-volume", "dsn-auto-volume").inDataCenter(inZone));
        }
	}

    @Override
    public @Nonnull String createVolume(@Nonnull VolumeCreateOptions options) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new InternalException("No context was specified for this request");
        }
        Map<String,String> parameters = provider.getStandardParameters(ctx, EC2Method.CREATE_VOLUME);
        EC2Method method;
        NodeList blocks;
        Document doc;

        if( options.getSnapshotId() != null ) {
            parameters.put("SnapshotId", options.getSnapshotId());
        }
        parameters.put("Size", String.valueOf(options.getVolumeSize().getQuantity().intValue()));
        parameters.put("AvailabilityZone", options.getDataCenterId());
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            doc = method.invoke();
        }
        catch( EC2Exception e ) {
            logger.error(e.getSummary());
            throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("volumeId");
        if( blocks.getLength() > 0 ) {
            return blocks.item(0).getFirstChild().getNodeValue().trim();
        }
        throw new CloudException("Successful POST, but no volume information was provided");
    }

	@Override
	public void detach(@Nonnull String volumeId) throws InternalException, CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DETACH_VOLUME);
		EC2Method method;
		NodeList blocks;
		Document doc;
		
		parameters.put("VolumeId", volumeId);
		method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
        	doc = method.invoke();
        }
        catch( EC2Exception e ) {
        	logger.error(e.getSummary());
        	throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("return");
        if( blocks.getLength() > 0 ) {
        	if( !blocks.item(0).getFirstChild().getNodeValue().equalsIgnoreCase("true") ) {
        		throw new CloudException("Detach of volume denied.");
        	}
        }
	}

    @Override
    public int getMaximumVolumeCount() throws InternalException, CloudException {
        return -2;
    }

    @Override
    public @Nullable Storage<Gigabyte> getMaximumVolumeSize() throws InternalException, CloudException {
        return new Storage<Gigabyte>(1024, Storage.GIGABYTE);
    }

    @Override
    public @Nonnull Storage<Gigabyte> getMinimumVolumeSize() throws InternalException, CloudException {
        return new Storage<Gigabyte>(1, Storage.GIGABYTE);
    }

    @Override
	public @Nonnull String getProviderTermForVolume(@Nonnull Locale locale) {
		return "volume";
	}

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return true;
    }
    
    @Override
    public @Nonnull Iterable<String> listPossibleDeviceIds(@Nonnull Platform platform) throws InternalException, CloudException {
        ArrayList<String> list = new ArrayList<String>();
        
        if( platform.isWindows() ) {
            list.add("xvdf");
            list.add("xvdg");
            list.add("xvdh");
            list.add("xvdi");
            list.add("xvdj");
        }
        else {
            list.add("/dev/sdf");
            list.add("/dev/sdg");
            list.add("/dev/sdh");
            list.add("/dev/sdi");
            list.add("/dev/sdj");
        }
        return list;
    }

    @Override
    public @Nonnull Iterable<VolumeProduct> listVolumeProducts() throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
	public @Nullable Volume getVolume(@Nonnull String volumeId) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context exists for this request.");
        }
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_VOLUMES);
		EC2Method method;
        NodeList blocks;
		Document doc;

		parameters.put("VolumeId.1", volumeId);
		method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
        	doc = method.invoke();
        }
        catch( EC2Exception e ) {
        	String code = e.getCode();
        	
        	if( code != null && (code.startsWith("InvalidVolume.NotFound") || code.equals("InvalidParameterValue")) ) {
        		return null;
        	}
        	logger.error(e.getSummary());
        	throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("volumeSet");
        for( int i=0; i<blocks.getLength(); i++ ) {
        	NodeList items = blocks.item(i).getChildNodes();
        	
            for( int j=0; j<items.getLength(); j++ ) {
            	Node item = items.item(j);
            	
            	if( item.getNodeName().equals("item") ) {
            		Volume volume = toVolume(ctx, item);
            		
                    if( volume !=null ) System.out.println("id=" + volume.getProviderVolumeId());
            		if( volume != null && volume.getProviderVolumeId().equals(volumeId) ) {
            			return volume;
            		}
            	}
            }
        }
        return null;
	}

    @Override
    public @Nonnull Requirement getVolumeProductRequirement() throws InternalException, CloudException {
        return Requirement.NONE;
    }

    @Override
    public boolean isVolumeSizeDeterminedByProduct() throws InternalException, CloudException {
        return false;
    }

    @Override
	public @Nonnull Iterable<Volume> listVolumes() throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();
        
        if( ctx == null ) {
            throw new CloudException("No context exists for this request.");
        }
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_VOLUMES);
		ArrayList<Volume> list = new ArrayList<Volume>();
		EC2Method method;
        NodeList blocks;
		Document doc;

		method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
        	doc = method.invoke();
        }
        catch( EC2Exception e ) {
        	logger.error(e.getSummary());
        	throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("volumeSet");
        for( int i=0; i<blocks.getLength(); i++ ) {
        	NodeList items = blocks.item(i).getChildNodes();
        	
            for( int j=0; j<items.getLength(); j++ ) {
            	Node item = items.item(j);
            	
            	if( item.getNodeName().equals("item") ) {
            		Volume volume = toVolume(ctx, item);
            		
            		if( volume != null ) {
            			list.add(volume);
            		}
            	}
            }
        }
        return list;
	}
	
    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        if( action.equals(VolumeSupport.ANY) ) {
            return new String[] { EC2Method.EC2_PREFIX + "*" };
        }
        else if( action.equals(VolumeSupport.ATTACH) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.ATTACH_VOLUME };
        }
        else if( action.equals(VolumeSupport.CREATE_VOLUME) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.CREATE_VOLUME };
        }
        else if( action.equals(VolumeSupport.DETACH) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DETACH_VOLUME };
        }
        else if( action.equals(VolumeSupport.GET_VOLUME) || action.equals(VolumeSupport.LIST_VOLUME) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_VOLUMES };            
        }
        else if( action.equals(VolumeSupport.REMOVE_VOLUME) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DELETE_VOLUME };
        }
        return new String[0];
    }

    @Override
    public void remove(@Nonnull String volumeId) throws InternalException, CloudException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DELETE_VOLUME);
        EC2Method method;
        NodeList blocks;
        Document doc;

        parameters.put("VolumeId", volumeId);
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            doc = method.invoke();
        }
        catch( EC2Exception e ) {
            logger.error(e.getSummary());
            throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("return");
        if( blocks.getLength() > 0 ) {
            if( !blocks.item(0).getFirstChild().getNodeValue().equalsIgnoreCase("true") ) {
                throw new CloudException("Deletion of volume denied.");
            }
        }
    }
    
	private @Nullable Volume toVolume(@Nonnull ProviderContext ctx, @Nullable Node node) throws CloudException {
        if( node == null ) {
            return null;
        }
		NodeList attrs = node.getChildNodes();
		Volume volume = new Volume();
		
        volume.setType(VolumeType.HDD);
		for( int i=0; i<attrs.getLength(); i++ ) {
			Node attr = attrs.item(i);
			String name;
			
			name = attr.getNodeName();
			if( name.equals("volumeId") ) {
				volume.setProviderVolumeId(attr.getFirstChild().getNodeValue().trim());
				volume.setName(volume.getProviderVolumeId());
			}
			else if( name.equals("size") ) {
				int size = Integer.parseInt(attr.getFirstChild().getNodeValue().trim());
				
                volume.setSize(new Storage<Gigabyte>(size, Storage.GIGABYTE));
			}
			else if( name.equals("snapshotId") ) {
				NodeList values = attr.getChildNodes();
				
				if( values != null && values.getLength() > 0 ) {
					volume.setProviderSnapshotId(values.item(0).getNodeValue().trim());
				}
			}
			else if( name.equals("availabilityZone") ) {
				String zoneId = attr.getFirstChild().getNodeValue().trim();
				
				volume.setProviderDataCenterId(zoneId);
			}
			else if( name.equals("createTime") ) {
				SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
				String value = attr.getFirstChild().getNodeValue().trim();

				try {
					volume.setCreationTimestamp(fmt.parse(value).getTime());
				} 
				catch( ParseException e ) {
					logger.error(e);
					e.printStackTrace();
					throw new CloudException(e);
				}				
			}
			else if( name.equals("status") ) {
				String s = attr.getFirstChild().getNodeValue().trim();
				VolumeState state;
				
		        if( s.equals("creating") || s.equals("attaching") || s.equals("attached") || s.equals("detaching") || s.equals("detached") ) {
		            state = VolumeState.PENDING;
		        }
		        else if( s.equals("available") || s.equals("in-use") ) {
		            state = VolumeState.AVAILABLE;
		        }
		        else {
		            state = VolumeState.DELETED;
		        }
				volume.setCurrentState(state);
			}
			else if( name.equals("attachmentSet") ) {
				NodeList attachments = attr.getChildNodes();
				
				for( int j=0; j<attachments.getLength(); j++ ) {
					Node item = attachments.item(j);
					
					if( item.getNodeName().equals("item") ) {
						NodeList infoList = item.getChildNodes();
						
						for( int k=0; k<infoList.getLength(); k++ ) {
							Node info = infoList.item(k);
							
							name = info.getNodeName();
							if( name.equals("instanceId") ) {
								volume.setProviderVirtualMachineId(info.getFirstChild().getNodeValue().trim());
							}
							else if( name.equals("device") ) {
							    String deviceId = info.getFirstChild().getNodeValue().trim();

							    if( deviceId.startsWith("unknown,requested:") ) {
							        deviceId = deviceId.substring(18);
							    }
								volume.setDeviceId(deviceId);
							}
						}
					}
				}
			}
		}
		volume.setProviderRegionId(ctx.getRegionId());
		return volume;
	}
}
