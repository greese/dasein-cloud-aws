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
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.Tag;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.compute.AbstractSnapshotSupport;
import org.dasein.cloud.compute.Snapshot;
import org.dasein.cloud.compute.SnapshotCreateOptions;
import org.dasein.cloud.compute.SnapshotFilterOptions;
import org.dasein.cloud.compute.SnapshotState;
import org.dasein.cloud.compute.SnapshotSupport;
import org.dasein.cloud.compute.Volume;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.util.APITrace;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class EBSSnapshot extends AbstractSnapshotSupport {
	static private final Logger logger = AWSCloud.getLogger(EBSSnapshot.class);
	
	private AWSCloud provider = null;
	
	EBSSnapshot(@Nonnull AWSCloud provider) {
        super(provider);
		this.provider = provider;
	}

    @Override
    public void addSnapshotShare(@Nonnull String providerSnapshotId, @Nonnull String accountNumber) throws CloudException, InternalException {
        APITrace.begin(provider, "addSnapshotShare");
        try {
            setPrivateShare(providerSnapshotId, true, accountNumber);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void addPublicShare(@Nonnull String providerSnapshotId) throws CloudException, InternalException {
        APITrace.begin(provider, "addPublicShare");
        try {
            setPublicShare(providerSnapshotId, true);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String createSnapshot(@Nonnull SnapshotCreateOptions options) throws InternalException, CloudException {
        APITrace.begin(provider, "createSnapshot");
        try {
            String volumeId = options.getVolumeId();
            Map<String,String> parameters;
            EC2Method method;
            NodeList blocks;
            Document doc;

            if( volumeId != null ) {
                parameters = provider.getStandardParameters(provider.getContext(), EC2Method.CREATE_SNAPSHOT);
                parameters.put("VolumeId", volumeId);
            }
            else {
                parameters = provider.getStandardParameters(provider.getContext(), EC2Method.COPY_SNAPSHOT);
                parameters.put("SourceSnapshotId", options.getSnapshotId());
                parameters.put("SourceRegion", options.getRegionId());
            }
            parameters.put("Description", options.getDescription());
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("snapshotId");
            if( blocks.getLength() > 0 ) {
                String id = blocks.item(0).getFirstChild().getNodeValue().trim();

                if( id == null ) {
                    throw new CloudException("No error occurred, but no snapshot was provided");
                }
                Map<String,String> meta = options.getMetaData();

                meta.put("Name", options.getName());
                meta.put("Description", options.getDescription());

                ArrayList<Tag> tags = new ArrayList<Tag>();

                for( Map.Entry<String,String> entry : meta.entrySet() ) {
                    String value = entry.getValue();

                    if( value != null ) {
                        tags.add(new Tag(entry.getKey(), value));
                    }
                }
                provider.createTags(id, tags.toArray(new Tag[tags.size()]));
                return id;
            }
            throw new CloudException("No error occurred, but no snapshot was provided");
        }
        finally {
            APITrace.end();
        }
    }

    @Override
	public @Nonnull String getProviderTermForSnapshot(@Nonnull Locale locale) {
		return "snapshot";
	}

    @Override
    public @Nullable Snapshot getSnapshot(@Nonnull String snapshotId) throws InternalException, CloudException {
        APITrace.begin(provider, "getSnapshot");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context exists for this request.");
            }
            if( provider.getEC2Provider().isAWS() ) {
                Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_SNAPSHOTS);
                EC2Method method;
                NodeList blocks;
                Document doc;

                parameters.put("SnapshotId.1", snapshotId);
                method = new EC2Method(provider, provider.getEc2Url(), parameters);
                try {
                    doc = method.invoke();
                }
                catch( EC2Exception e ) {
                    String code = e.getCode();

                    if( code != null && (code.startsWith("InvalidSnapshot.NotFound") || code.equals("InvalidParameterValue")) ) {
                        return null;
                    }
                    logger.error(e.getSummary());
                    throw new CloudException(e);
                }
                blocks = doc.getElementsByTagName("snapshotSet");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    NodeList items = blocks.item(i).getChildNodes();

                    for( int j=0; j<items.getLength(); j++ ) {
                        Node item = items.item(j);

                        if( item.getNodeName().equals("item") ) {
                            Snapshot snapshot = toSnapshot(ctx, item);

                            if( snapshot != null && snapshot.getProviderSnapshotId().equals(snapshotId) ) {
                                return snapshot;
                            }
                        }
                    }
                }
                return null;
            }
            else {
                for( Snapshot snapshot : listSnapshots() ) {
                    if( snapshot.getProviderSnapshotId().equals(snapshotId) ) {
                        return snapshot;
                    }
                }
                return null;
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Requirement identifyAttachmentRequirement() throws InternalException, CloudException {
        return Requirement.OPTIONAL;
    }

    @Override
	public boolean isPublic(@Nonnull String snapshotId) throws InternalException, CloudException {
        APITrace.begin(provider, "isPublic");
        try {
            if( !provider.getEC2Provider().isAWS()) {
                return false;
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_SNAPSHOT_ATTRIBUTE);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("SnapshotId.1", snapshotId);
            parameters.put("Attribute", "createVolumePermission");
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && code.startsWith("InvalidSnapshot.NotFound") ) {
                    return false;
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("createVolumePermission");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("item") ) {
                        NodeList attrs = item.getChildNodes();

                        for( int k=0; k<attrs.getLength(); k++ ) {
                            Node attr = attrs.item(k);

                            if( attr.getNodeName().equals("group") ) {
                                String group = attr.getFirstChild().getNodeValue();

                                if( group != null ) {
                                    return group.equals("all");
                                }
                            }
                        }
                    }
                }
            }
            return false;
        }
        finally {
            APITrace.end();
        }
	}


    @Override
    public boolean isSubscribed() throws InternalException, CloudException {
        return true;
    }


    @Override
    public @Nonnull Iterable<String> listShares(@Nonnull String forSnapshotId) throws InternalException, CloudException {
        APITrace.begin(provider, "listShares");
        try {
            if( !provider.getEC2Provider().isAWS() ) {
                return new ArrayList<String>();
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_SNAPSHOT_ATTRIBUTE);
            ArrayList<String> list = new ArrayList<String>();
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("SnapshotId.1", forSnapshotId);
            parameters.put("Attribute", "createVolumePermission");
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && (code.startsWith("InvalidSnapshotID") || code.equals("InvalidSnapshot.NotFound")) ) {
                    return list;
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("createVolumePermission");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("item") ) {
                        NodeList attrs = item.getChildNodes();

                        for( int k=0; k<attrs.getLength(); k++ ) {
                            Node attr = attrs.item(k);

                            if( attr.getNodeName().equals("userId") ) {
                                String userId = attr.getFirstChild().getNodeValue();

                                if( userId != null ) {
                                    userId = userId.trim();
                                    if( userId.length() > 0 ) {
                                        list.add(userId);
                                    }
                                }
                            }
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
    public @Nonnull Iterable<ResourceStatus> listSnapshotStatus() throws InternalException, CloudException {
        APITrace.begin(provider, "listSnapshotStatus");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context exists for this request.");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_SNAPSHOTS);
            ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("Owner.1", "self");
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("snapshotSet");
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
    public @Nonnull Iterable<Snapshot> listSnapshots() throws InternalException, CloudException {
        return listSnapshots( null );
    }

	@Override
	public @Nonnull Iterable<Snapshot> listSnapshots(SnapshotFilterOptions options) throws InternalException, CloudException {
        APITrace.begin(provider, "listSnapshots");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context exists for this request.");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_SNAPSHOTS);
            ArrayList<Snapshot> list = new ArrayList<Snapshot>();
            EC2Method method;
            NodeList blocks;
            Document doc;

            if ( options != null ) {
                provider.putExtraParameters( parameters, provider.getTagFilterParams( options.getTags() ) );
            }

            parameters.put("Owner.1", "self");
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("snapshotSet");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("item") ) {
                        Snapshot snapshot = toSnapshot(ctx, item);

                        if( snapshot != null ) {
                            list.add(snapshot);
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
        if( action.equals(SnapshotSupport.ANY) ) {
            return new String[] { EC2Method.EC2_PREFIX + "*" };
        }
        if( action.equals(SnapshotSupport.CREATE_SNAPSHOT) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.CREATE_SNAPSHOT };
        }
        else if( action.equals(SnapshotSupport.GET_SNAPSHOT) || action.equals(SnapshotSupport.LIST_SNAPSHOT) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_SNAPSHOTS };            
        }
        else if( action.equals(SnapshotSupport.MAKE_PUBLIC) || action.equals(SnapshotSupport.SHARE_SNAPSHOT) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.MODIFY_SNAPSHOT_ATTRIBUTE };
        }
        else if( action.equals(SnapshotSupport.REMOVE_SNAPSHOT) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DELETE_SNAPSHOT };
        }
        return new String[0];
    }

    @Override
    public void remove(@Nonnull String snapshotId) throws InternalException, CloudException {
        APITrace.begin(provider, "remove");
        try {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DELETE_SNAPSHOT);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("SnapshotId", snapshotId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null ) {
                    if( code.equals("InvalidSnapshot.NotFound") ) {
                        return;
                    }
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("return");
            if( blocks.getLength() > 0 ) {
                if( !blocks.item(0).getFirstChild().getNodeValue().equalsIgnoreCase("true") ) {
                    throw new CloudException("Deletion of snapshot denied.");
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeAllSnapshotShares(@Nonnull String providerSnapshotId) throws CloudException, InternalException {
        APITrace.begin(provider, "removeAllSnapshotShares");
        try {
            List<String> shares = (List<String>)listShares(providerSnapshotId);

            if( shares.isEmpty() ) {
                return;
            }
            setPrivateShare(providerSnapshotId, false, shares.toArray(new String[shares.size()]));
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeSnapshotShare(@Nonnull String providerSnapshotId, @Nonnull String accountNumber) throws CloudException, InternalException {
        APITrace.begin(provider, "removeSnapshotShare");
        try {
            setPrivateShare(providerSnapshotId, false, accountNumber);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removePublicShare(@Nonnull String providerSnapshotId) throws CloudException, InternalException {
        APITrace.begin(provider, "removePublicShare");
        try {
            setPublicShare(providerSnapshotId, false);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeTags(@Nonnull String snapshotId, @Nonnull Tag... tags) throws CloudException, InternalException {
        ((AWSCloud)getProvider()).removeTags(snapshotId, tags);
    }

    @Override
    public void removeTags(@Nonnull String[] snapshotIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        ((AWSCloud)getProvider()).removeTags(snapshotIds, tags);
    }

    @Override
    public @Nonnull Iterable<Snapshot> searchSnapshots(@Nullable String ownerId, @Nullable String keyword) throws InternalException, CloudException {
        APITrace.begin(provider, "searchSnapshots");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context exists for this request.");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_SNAPSHOTS);
            ArrayList<Snapshot> list = new ArrayList<Snapshot>();
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
            blocks = doc.getElementsByTagName("snapshotSet");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("item") ) {
                        Snapshot snapshot = toSnapshot(ctx, item);

                        if( snapshot != null ) {
                            if( ownerId != null && !ownerId.equals(snapshot.getProviderSnapshotId()) ) {
                                continue;
                            }
                            if( keyword != null && !snapshot.getName().contains(keyword) && !snapshot.getDescription().contains(keyword) ) {
                                continue;
                            }
                            list.add(snapshot);
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

    private void setPublicShare(@Nonnull String snapshotId, boolean affirmative) throws InternalException, CloudException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.MODIFY_SNAPSHOT_ATTRIBUTE);
        EC2Method method;
        NodeList blocks;
        Document doc;

        parameters.put("SnapshotId", snapshotId);
        parameters.put("UserGroup.1", "all");
        parameters.put("Attribute", "createVolumePermission");
        parameters.put("OperationType", affirmative ? "add" : "remove");
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            doc = method.invoke();
        }
        catch( EC2Exception e ) {
            String code = e.getCode();

            if( code != null && code.startsWith("InvalidSnapshot.NotFound") ) {
                return;
            }
            logger.error(e.getSummary());
            throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("return");
        if( blocks.getLength() > 0 ) {
            if( !blocks.item(0).getFirstChild().getNodeValue().equalsIgnoreCase("true") ) {
                throw new CloudException("Deletion of snapshot denied.");
            }
        }
    }

    private void setPrivateShare(@Nonnull String snapshotId, boolean affirmative, @Nonnull String ... accountIds) throws InternalException, CloudException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.MODIFY_SNAPSHOT_ATTRIBUTE);
        EC2Method method;
        NodeList blocks;
        Document doc;

        parameters.put("SnapshotId", snapshotId);
        for(int i=0; i<accountIds.length; i++ ) {
            parameters.put("UserId." + i, accountIds[i]);
        }
        parameters.put("Attribute", "createVolumePermission");
        parameters.put("OperationType", affirmative ? "add" : "remove");
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            doc = method.invoke();
        }
        catch( EC2Exception e ) {
            String code = e.getCode();

            if( code != null && code.startsWith("InvalidSnapshot.NotFound") ) {
                return;
            }
            logger.error(e.getSummary());
            throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("return");
        if( blocks.getLength() > 0 ) {
            if( !blocks.item(0).getFirstChild().getNodeValue().equalsIgnoreCase("true") ) {
                throw new CloudException("Deletion of snapshot denied.");
            }
        }
    }

    @Override
    public boolean supportsSnapshotCopying() throws CloudException, InternalException {
        return provider.getEC2Provider().isAWS();
    }

    @Override
    public boolean supportsSnapshotCreation() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean supportsSnapshotSharing() throws InternalException, CloudException {
        return provider.getEC2Provider().isAWS();
    }

    @Override
    public boolean supportsSnapshotSharingWithPublic() throws InternalException, CloudException {
        return provider.getEC2Provider().isAWS();
    }

    private @Nullable Snapshot toSnapshot(@Nonnull ProviderContext ctx, @Nullable Node node) throws CloudException {
        if( node == null ) {
            return null;
        }
		NodeList attrs = node.getChildNodes();
		Snapshot snapshot = new Snapshot();
		
		if( !provider.getEC2Provider().isAWS() ) {
		    snapshot.setOwner(ctx.getAccountNumber());
		}
		for( int i=0; i<attrs.getLength(); i++ ) {
			Node attr = attrs.item(i);
			String name;
			
			name = attr.getNodeName();
			if( name.equals("snapshotId") ) {
				snapshot.setProviderSnapshotId(attr.getFirstChild().getNodeValue().trim());
			}
			else if( name.equals("volumeId") ) {
				NodeList children = attr.getChildNodes();
				
				if( children != null && children.getLength() > 0 ) {
					String vol = children.item(0).getNodeValue();
					
					if( vol != null ) {
						vol = vol.trim();
						if( vol.length() > 0 ) {
							snapshot.setVolumeId(vol);
						}
					}
				}
			}
			else if( name.equals("status") ) {
				String s = attr.getFirstChild().getNodeValue().trim();
				SnapshotState state;
				
		        if( s.equals("completed") ) {
		            state = SnapshotState.AVAILABLE;
		        }
		        else if( s.equals("deleting") || s.equals("deleted") ) {
		            state = SnapshotState.DELETED;
		        }
		        else {
		            state = SnapshotState.PENDING;
		        }
		        snapshot.setCurrentState(state);
			}
			else if( name.equals("startTime") ) {
				NodeList children = attr.getChildNodes();
				long ts = 0L;
				
				if( children != null && children.getLength() > 0 ) {
					String t = children.item(0).getNodeValue();
					
					if( t != null ) {
						SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
						
						t = t.trim();
						if( t.length() > 0 ) {
							try {
								ts = fmt.parse(t).getTime();
							} 
							catch( ParseException e ) {
								logger.error(e);
								e.printStackTrace();
								throw new CloudException(e);
							}
						}
					}
				}
				snapshot.setSnapshotTimestamp(ts);
			}
			else if( name.equals("progress") ) {
				NodeList children = attr.getChildNodes();
				String progress = "100%";
				
				if( children != null && children.getLength() > 0 ) {
					String p = children.item(0).getNodeValue();
					
					if( p != null ) {
						p = p.trim();
						if( p.length() > 0 ) {
							progress = p;
						}
					}
				}
				snapshot.setProgress(progress);
			}
			else if( name.equals("ownerId") ) {
				snapshot.setOwner(attr.getFirstChild().getNodeValue().trim());
			}
			else if( name.equals("volumeSize") ) {
			    String val = attr.getFirstChild().getNodeValue().trim();
			    
			    if( val == null || val.equals("n/a") ) {
			        snapshot.setSizeInGb(0);
			    }
			    else {
			        snapshot.setSizeInGb(Integer.parseInt(val));
			    }
			}
			else if( name.equals("description") ) {
				NodeList children = attr.getChildNodes();
				String description = null;
				
				if( children != null && children.getLength() > 0 ) {
					String d = children.item(0).getNodeValue();
					
					if( d != null ) {
						description = d.trim();
					}
				}
				snapshot.setDescription(description);
			}
            else if( name.equals("tagSet") ) {
                provider.setTags(attr, snapshot);
            }
		}
        String name = snapshot.getName();

        if( name == null ) {
            name = snapshot.getTags().get("Name");
            if( name == null ) {
                name = snapshot.getProviderSnapshotId();
            }
            snapshot.setName(name);
        }

        String description = snapshot.getDescription();

		if( description == null ) {
            description = snapshot.getTags().get("Description");
            if( description == null ) {
                description = (name + " [" + snapshot.getSizeInGb() + " GB]");
            }
            snapshot.setDescription(description);
		}
		snapshot.setRegionId(ctx.getRegionId());
		if( snapshot.getSizeInGb() < 1 ) {
            EC2ComputeServices svc = provider.getComputeServices();

            if( svc != null ) {
                EBSVolume vs = svc.getVolumeSupport();

                try {
                    Volume volume = vs.getVolume(snapshot.getProviderSnapshotId());

                    if( volume != null ) {
                        snapshot.setSizeInGb(volume.getSizeInGigabytes());
                    }
                }
                catch( InternalException ignore ) {
                    // ignore
                }
            }
		}
		return snapshot;
	}

    private @Nullable ResourceStatus toStatus(@Nullable Node node) throws CloudException {
        if( node == null ) {
            return null;
        }
        NodeList attrs = node.getChildNodes();
        SnapshotState state = SnapshotState.PENDING;
        String snapshotId = null;

        for( int i=0; i<attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;

            name = attr.getNodeName();
            if( name.equals("snapshotId") ) {
                snapshotId = attr.getFirstChild().getNodeValue().trim();
            }
            else if( name.equals("status") ) {
                String s = attr.getFirstChild().getNodeValue().trim();

                if( s.equals("completed") ) {
                    state = SnapshotState.AVAILABLE;
                }
                else if( s.equals("deleting") || s.equals("deleted") ) {
                    state = SnapshotState.DELETED;
                }
                else {
                    state = SnapshotState.PENDING;
                }
            }
        }
        if( snapshotId == null ) {
            return null;
        }
        return new ResourceStatus(snapshotId, state);
    }

    @Override
    public void updateTags(@Nonnull String snapshotId, @Nonnull Tag... tags) throws CloudException, InternalException {
        ((AWSCloud)getProvider()).createTags(snapshotId, tags);
    }

    @Override
    public void updateTags(@Nonnull String[] snapshotIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        ((AWSCloud)getProvider()).createTags(snapshotIds, tags);
    }
}
