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

package org.dasein.cloud.aws.compute;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.Tag;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.compute.AbstractVolumeSupport;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.Volume;
import org.dasein.cloud.compute.VolumeCreateOptions;
import org.dasein.cloud.compute.VolumeFilterOptions;
import org.dasein.cloud.compute.VolumeFormat;
import org.dasein.cloud.compute.VolumeProduct;
import org.dasein.cloud.compute.VolumeState;
import org.dasein.cloud.compute.VolumeSupport;
import org.dasein.cloud.compute.VolumeType;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Storage;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class EBSVolume extends AbstractVolumeSupport {
	static private final Logger logger = Logger.getLogger(EBSVolume.class);
	
	private AWSCloud provider = null;
	
	EBSVolume(AWSCloud provider) {
        super(provider);
		this.provider = provider;
	}
	
	@Override
	public void attach(@Nonnull String volumeId, @Nonnull String toServer, @Nonnull String device) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Volume.attach");
        try {
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
        finally {
            APITrace.end();
        }
	}

    @Override
    public @Nonnull String createVolume(@Nonnull VolumeCreateOptions options) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Volume.createVolume");
        try {
            if( !options.getFormat().equals(VolumeFormat.BLOCK)) {
                throw new OperationNotSupportedException("NFS volumes are not currently supported");
            }
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

            String az = options.getDataCenterId();

            if( az == null ) {
                for( DataCenter dc : provider.getDataCenterServices().listDataCenters(ctx.getRegionId()) ) {
                    az = dc.getProviderDataCenterId();
                }
                if( az == null ) {
                    throw new CloudException("Unable to identify a launch data center");
                }
            }
            parameters.put("AvailabilityZone", az);
            if( provider.getEC2Provider().isAWS() || provider.getEC2Provider().isEnStratus() ) {
                if( options.getVolumeProductId() != null ) {
                    VolumeProduct prd = null;

                    for( VolumeProduct p : listVolumeProducts() ) {
                        if( p.getProviderProductId().equals(options.getVolumeProductId()) ) {
                            prd = p;
                            break;
                        }
                    }
                    if( prd != null ) {
                        parameters.put("VolumeType", prd.getProviderProductId());
                        if( prd.getMaxIops() > 0 && options.getIops() > 0 ) {
                            parameters.put("Iops", String.valueOf(options.getIops()));
                        }
                        else if( prd.getMinIops() > 0 ) {
                            parameters.put("Iops", String.valueOf(prd.getMinIops()));
                        }
                    }
                }
            }
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
                String id = blocks.item(0).getFirstChild().getNodeValue().trim();
                Map<String,Object> meta = options.getMetaData();

                meta.put("Name", options.getName());
                meta.put("Description", options.getDescription());
                ArrayList<Tag> tags = new ArrayList<Tag>();

                for( Map.Entry<String,Object> entry : meta.entrySet() ) {
                    Object value = entry.getValue();

                    if( value != null ) {
                        tags.add(new Tag(entry.getKey(), value.toString()));
                    }
                }
                if( !tags.isEmpty() ) {
                    provider.createTags(id, tags.toArray(new Tag[tags.size()]));
                }
                return id;
            }
            throw new CloudException("Successful POST, but no volume information was provided");
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void detach(@Nonnull String volumeId, boolean force) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Volume.detach");
        try {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DETACH_VOLUME);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("VolumeId", volumeId);
            if( force ) {
                parameters.put("Force", "true");
            }
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
        finally {
            APITrace.end();
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
        return new Storage<Gigabyte>(10, Storage.GIGABYTE);
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
    public @Nonnull Iterable<VolumeFormat> listSupportedFormats() throws InternalException, CloudException {
        return Collections.singletonList(VolumeFormat.BLOCK);
    }

    @Override
    public @Nonnull Iterable<VolumeProduct> listVolumeProducts() throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Volume.listVolumeProducts");
        try {
            Cache<VolumeProduct> cache = Cache.getInstance(getProvider(), "volumeProducts", VolumeProduct.class, CacheLevel.REGION);
            Iterable<VolumeProduct> products = cache.get(getContext());

            if( products == null ) {
                ArrayList<VolumeProduct> prds = new ArrayList<VolumeProduct>();
                ProviderContext ctx = provider.getContext();
                float rawPrice = 0.11f;

                if( ctx != null ) {
                    String regionId = ctx.getRegionId();

                    if( regionId != null ) {
                        if( regionId.equals("us-east-1") || regionId.equals("us-west-2") ) {
                            rawPrice = 0.10f;
                        }
                        else if( regionId.equals("ap-northeast-1") ) {
                            rawPrice = 0.12f;
                        }
                        else if( regionId.equals("sa-east-1") ) {
                            rawPrice = 0.19f;
                        }
                    }
                }

                prds.add(VolumeProduct.getInstance("standard", "Standard", "Standard EBS with no IOPS Guarantees", VolumeType.HDD, getMinimumVolumeSize(), "USD", 0, 0, rawPrice, 0f));
                prds.add(VolumeProduct.getInstance("io1", "IOPS EBS", "EBS Volume with IOPS guarantees", VolumeType.HDD, getMinimumVolumeSize(), "USD", 100, 1000, 0.125f, 0.1f));
                cache.put(getContext(), prds);
                products = prds;
            }
            return products;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
	public @Nullable Volume getVolume(@Nonnull String volumeId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Volume.getVolume");
        try {
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

                        if( volume != null && volume.getProviderVolumeId().equals(volumeId) ) {
                            return volume;
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
    public @Nonnull Requirement getVolumeProductRequirement() throws InternalException, CloudException {
        return ((provider.getEC2Provider().isEucalyptus() || provider.getEC2Provider().isOpenStack()) ? Requirement.NONE : Requirement.OPTIONAL);
    }

    @Override
    public boolean isVolumeSizeDeterminedByProduct() throws InternalException, CloudException {
        return false;
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listVolumeStatus() throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Volume.listVolumeStatus");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context exists for this request.");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_VOLUMES);
            ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();
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
                        ResourceStatus status = toStatus(item);

                        if( status != null ) {
                            list.add(status);
                        }
                    }
                }
            }
            return list;
        }
        finally {
            APITrace.end();
        }
    }


    @Override
    public @Nonnull Iterable<Volume> listVolumes() throws InternalException, CloudException {
        return listVolumes( null );
    }

    @Override
    public @Nonnull Iterable<Volume> listVolumes(@Nullable VolumeFilterOptions options) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Volume.listVolumes");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context exists for this request.");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_VOLUMES);
            ArrayList<Volume> list = new ArrayList<Volume>();
            EC2Method method;
            NodeList blocks;
            Document doc;

            if ( options != null ) {
                provider.putExtraParameters( parameters, provider.getTagFilterParams( options.getTags() ) );
            }

            method = new EC2Method( provider, provider.getEc2Url(), parameters );
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
                        Volume volume = toVolume( ctx, item );

                        if( volume != null && (options == null || options.matches(volume)) ) {
                            list.add(volume);
                        }
                    }
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
        APITrace.begin(getProvider(), "Volume.remove");
        try {
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
        finally {
            APITrace.end();
        }
    }

    @Override
    public void updateTags(@Nonnull String volumeId, @Nonnull Tag ... tags) throws CloudException, InternalException {
        updateTags(new String[] { volumeId }, tags);
    }

    @Override
    public void updateTags(@Nonnull String[] volumeIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Volume.updateTags");
        try {
            provider.createTags( volumeIds, tags );
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeTags(@Nonnull String volumeId, @Nonnull Tag ... tags) throws CloudException, InternalException {
        removeTags(new String[] { volumeId }, tags);
    }

    @Override
    public void removeTags(@Nonnull String[] volumeIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Volume.removeTags");
        try {
            provider.removeTags( volumeIds, tags );
        }
        finally {
            APITrace.end();
        }
    }

    private @Nullable ResourceStatus toStatus(@Nullable Node node) throws CloudException {
        if( node == null ) {
            return null;
        }
        NodeList attrs = node.getChildNodes();
        VolumeState state = VolumeState.PENDING;
        String volumeId = null;

        for( int i=0; i<attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;

            name = attr.getNodeName();
            if( name.equals("volumeId") ) {
                volumeId = attr.getFirstChild().getNodeValue().trim();
            }
            else if( name.equals("status") ) {
                String s = attr.getFirstChild().getNodeValue().trim();

                if( s.equals("creating") || s.equals("attaching") || s.equals("attached") || s.equals("detaching") || s.equals("detached") ) {
                    state = VolumeState.PENDING;
                }
                else if( s.equals("available") || s.equals("in-use") ) {
                    state = VolumeState.AVAILABLE;
                }
                else {
                    state = VolumeState.DELETED;
                }
            }
        }
        if( volumeId == null ) {
            return null;
        }
        return new ResourceStatus(volumeId, state);
    }

	private @Nullable Volume toVolume(@Nonnull ProviderContext ctx, @Nullable Node node) throws CloudException {
        if( node == null ) {
            return null;
        }
        NodeList attrs = node.getChildNodes();
        Volume volume = new Volume();

        volume.setProviderProductId("standard");
        volume.setType( VolumeType.HDD );
        volume.setFormat(VolumeFormat.BLOCK);
		for( int i=0; i<attrs.getLength(); i++ ) {
			Node attr = attrs.item(i);
			String name;
			
			name = attr.getNodeName();
			if( name.equals("volumeId") ) {
				volume.setProviderVolumeId(attr.getFirstChild().getNodeValue().trim());
			}
            else if( name.equalsIgnoreCase("name") && attr.hasChildNodes() ) {
                volume.setName(attr.getFirstChild().getNodeName().trim());
            }
            else if( name.equalsIgnoreCase("description") && attr.hasChildNodes() ) {
                volume.setDescription(attr.getFirstChild().getNodeName().trim());
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
            else if( name.equalsIgnoreCase("volumeType") && attr.hasChildNodes() ) {
                volume.setProviderProductId(attr.getFirstChild().getNodeValue().trim());
            }
            else if( name.equalsIgnoreCase("iops") && attr.hasChildNodes() ) {
                volume.setIops(Integer.parseInt(attr.getFirstChild().getNodeValue().trim()));
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
        volume.setCurrentState( toVolumeState( attr ) );
      }
            else if( name.equals("tagSet") ) {
                provider.setTags(attr, volume);

                String s = volume.getTag("Name");

                if( s != null && volume.getName() == null ) {
                    volume.setName(s);
                }
                s = volume.getTag("Description");

                if( s != null && volume.getDescription() == null ) {
                    volume.setDescription(s);
                }
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
        if( volume.getProviderVolumeId() == null ) {
            return null;
        }
        if( volume.getName() == null ) {
            volume.setName(volume.getProviderVolumeId());
        }
        if( volume.getDescription() == null ) {
            volume.setDescription(volume.getName());
        }
		volume.setProviderRegionId(ctx.getRegionId());
		return volume;
	}

  static public @Nullable VolumeState toVolumeState( Node node ) {
    String s = AWSCloud.getTextValue( node );
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
    return state;
  }
}
