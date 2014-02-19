/**
 * Copyright (C) 2009-2014 Dell, Inc.
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

import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.aws.compute.EC2Method;
import org.dasein.cloud.compute.ComputeServices;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.*;
import org.dasein.cloud.util.APITrace;
import org.dasein.util.CalendarWrapper;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ElasticIP implements IpAddressSupport {
	static private final Logger logger = AWSCloud.getLogger(ElasticIP.class);

	private AWSCloud provider = null;

  static private final ExecutorService threadPool = Executors.newFixedThreadPool(10);
	
	ElasticIP(AWSCloud provider) {
		this.provider = provider;
	}

    private @Nullable VirtualMachine getInstance(@Nonnull String instanceId) throws InternalException, CloudException {
        ComputeServices services = provider.getComputeServices();

        if( services != null ) {
            VirtualMachineSupport support = services.getVirtualMachineSupport();

            if( support != null ) {
                return support.getVirtualMachine(instanceId);
            }
        }
        throw new CloudException("Instances are not supported in " + provider.getCloudName());
    }

    @Override
    public void assign(@Nonnull String addressId, @Nonnull String instanceId) throws InternalException,	CloudException {
        APITrace.begin(provider, "IpAddress.assignAddressToServer");
        try {
            long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 20L);
            VirtualMachine vm = getInstance(instanceId);

            while( System.currentTimeMillis() < timeout ) {
                if( vm == null || VmState.TERMINATED.equals(vm.getCurrentState()) ) {
                    throw new CloudException("No such virtual machine " + instanceId);
                }
                VmState s = vm.getCurrentState();

                if( VmState.RUNNING.equals(s) || VmState.STOPPED.equals(s) || VmState.PAUSED.equals(s) || VmState.SUSPENDED.equals(s) ) {
                    break;
                }
                try { Thread.sleep(20000L); }
                catch( InterruptedException ignore ) { }
                try { vm = getInstance(instanceId); }
                catch( Throwable ignore ) { }
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.ASSOCIATE_ADDRESS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            setId("", parameters, getIpAddress(addressId), addressId, false);
            parameters.put("InstanceId", instanceId);
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
                    throw new CloudException("Association of address denied.");
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void assignToNetworkInterface(@Nonnull String addressId, @Nonnull String nicId) throws InternalException, CloudException {
        APITrace.begin(provider, "IpAddress.assignAddressToNetworkInterface");
        try {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.ASSOCIATE_ADDRESS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("AllocationId", addressId);
            parameters.put("NetworkInterfaceId", nicId);
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
                    throw new CloudException("Association of address denied.");
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String forward(@Nonnull String addressId, int publicPort, @Nonnull Protocol protocol, int privatePort, @Nonnull String serverId) throws InternalException, CloudException {
        throw new OperationNotSupportedException();
    }

    @Override
    public @Nullable IpAddress getIpAddress(@Nonnull String addressId) throws InternalException, CloudException {
        APITrace.begin(provider, "IpAddress.getIpAddress");
        try {
            IpAddress address = getEC2Address(addressId);
        
            return (((address == null || address.isForVlan()) && provider.getEC2Provider().isAWS()) ? getVPCAddress(addressId) : address);
        }
        finally {
            APITrace.end();
        }
    }
    
    private @Nullable IpAddress getEC2Address(@Nonnull String addressId) throws InternalException, CloudException {
        APITrace.begin(provider, "IpAddress.getEC2Address");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_ADDRESSES);
            IpAddress address = null;
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("PublicIp.1", addressId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && code.equals("InvalidAddress.NotFound") || e.getMessage().contains("Invalid value") ) {
                    return null;
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("addressesSet");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("item") ) {
                        address = toAddress(ctx, item);
                        if( address != null && addressId.equals(address.getProviderIpAddressId())) {
                            return address;
                        }
                    }
                }
            }
            return address;
        }
        finally {
            APITrace.end();
        }
	}

    private @Nullable IpAddress getVPCAddress(@Nonnull String addressId) throws InternalException, CloudException {
        APITrace.begin(provider, "IpAddress.getVPCAddress");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_ADDRESSES);
            IpAddress address = null;
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("AllocationId.1", addressId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && (code.equals("InvalidAllocationID.NotFound") || code.equals("InvalidAddress.NotFound") || e.getMessage().contains("Invalid value") || e.getMessage().startsWith("InvalidAllocation")) ) {
                    return null;
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("addressesSet");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("item") ) {
                        address = toAddress(ctx, item);
                        if( address != null && addressId.equals(address.getProviderIpAddressId())) {
                            return address;
                        }
                    }
                }
            }
            return address;
        }
        finally {
            APITrace.end();
        }
    }
    
    @Override
    public @Nonnull String getProviderTermForIpAddress(@Nonnull Locale locale) {
		return "elastic IP";
	}

    @Override
    public @Nonnull Requirement identifyVlanForVlanIPRequirement() {
        return Requirement.NONE;
    }

    @Override
    public boolean isAssigned(@Nonnull AddressType type) {
		return type.equals(AddressType.PUBLIC);
	}

    @Override
    public boolean isAssigned(@Nonnull IPVersion version) throws CloudException, InternalException {
        return version.equals(IPVersion.IPV4);
    }

    @Override
    public boolean isAssignablePostLaunch(@Nonnull IPVersion version) throws CloudException, InternalException {
        return version.equals(IPVersion.IPV4);
    }

    @Override
    public boolean isForwarding() {
		return false;
	}

    @Override
    public boolean isForwarding(IPVersion version) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isRequestable(@Nonnull AddressType type) {
        return type.equals(AddressType.PUBLIC);
    }

    @Override
    public boolean isRequestable(@Nonnull IPVersion version) throws CloudException, InternalException {
        return version.equals(IPVersion.IPV4);
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return true;
    }
    
    @Override
    public @Nonnull Iterable<IpAddress> listPrivateIpPool(boolean unassignedOnly) throws InternalException, CloudException {
	    return Collections.emptyList();
	}

    @Override
    public @Nonnull Iterable<IpAddress> listPublicIpPool(boolean unassignedOnly) throws InternalException, CloudException {
        return listIpPool(IPVersion.IPV4, unassignedOnly);
    }

    @Override
    public @Nonnull Iterable<IpAddress> listIpPool(@Nonnull IPVersion version, boolean unassignedOnly) throws InternalException, CloudException {
        APITrace.begin(provider, "IpAddress.listIpPool");
        try {
          Future<Iterable<IpAddress>> ipPoolFuture = listIpPoolConcurrently( IPVersion.IPV4, false );
          return ipPoolFuture.get();
        } catch (CloudException ce) {
          throw ce;
        } catch (Exception e) {
          throw new InternalException(e);
        }
        finally {
            APITrace.end();
        }
    }

    public Future<Iterable<IpAddress>> listIpPoolConcurrently(IPVersion version, boolean unassignedOnly) throws CloudException, InternalException {
      return threadPool.submit(
        new ListIpPoolCallable(
          version,
          unassignedOnly
        )
      );
    }

    public class ListIpPoolCallable implements Callable {
      IPVersion version;
      boolean unassignedOnly;

      public ListIpPoolCallable( IPVersion version, boolean unassignedOnly ) {
        this.version = version;
        this.unassignedOnly = unassignedOnly;
      }

      public Iterable<IpAddress> call() throws CloudException, InternalException {
        if( !version.equals(IPVersion.IPV4) ) {
          return Collections.emptyList();
        }
        ProviderContext ctx = provider.getContext();
        if( ctx == null ) {
          throw new CloudException("No context was set for this request");
        }
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_ADDRESSES);
        ArrayList<IpAddress> list = new ArrayList<IpAddress>();
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
        blocks = doc.getElementsByTagName("addressesSet");
        for( int i=0; i<blocks.getLength(); i++ ) {
          NodeList items = blocks.item(i).getChildNodes();

          for( int j=0; j<items.getLength(); j++ ) {
            Node item = items.item(j);

            if( item.getNodeName().equals("item") ) {
              IpAddress address = toAddress(ctx, item);

              if( address != null && (!unassignedOnly || (address.getServerId() == null && address.getProviderLoadBalancerId() == null)) ) {
                list.add(address);
              }
            }
          }
        }
        return list;
      }
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listIpPoolStatus(@Nonnull IPVersion version) throws InternalException, CloudException {
        APITrace.begin(provider, "IpAddress.listIpPoolStatus");
        try {
            if( !version.equals(IPVersion.IPV4) ) {
                return Collections.emptyList();
            }
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_ADDRESSES);
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
            blocks = doc.getElementsByTagName("addressesSet");
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
    public @Nonnull Collection<IpForwardingRule> listRules(@Nonnull String addressId)	throws InternalException, CloudException {
        return Collections.emptyList();
	}

    @Override
    public @Nonnull Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        return Collections.singletonList(IPVersion.IPV4);
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        if( action.equals(IpAddressSupport.ANY) ) {
            return new String[] { EC2Method.EC2_PREFIX + "*" };
        }
        else if( action.equals(IpAddressSupport.ASSIGN) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.ASSOCIATE_ADDRESS };
        }
        else if( action.equals(IpAddressSupport.CREATE_IP_ADDRESS) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.ALLOCATE_ADDRESS };
        }
        else if( action.equals(IpAddressSupport.FORWARD) ) {
            return new String[0];
        }
        else if( action.equals(IpAddressSupport.GET_IP_ADDRESS) ) {

            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_ADDRESSES };
        }
        else if( action.equals(IpAddressSupport.LIST_IP_ADDRESS) ) {

            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_ADDRESSES };            
        }
        else if( action.equals(IpAddressSupport.RELEASE) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DISASSOCIATE_ADDRESS };
        }
        else if( action.equals(IpAddressSupport.REMOVE_IP_ADDRESS) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.RELEASE_ADDRESS };
        }
        else if( action.equals(IpAddressSupport.STOP_FORWARD) ) {
            return new String[0];
        }
        return new String[0];
    }

    @Override
    public void releaseFromServer(@Nonnull String addressId) throws InternalException, CloudException {
        APITrace.begin(provider, "IpAddress.releaseFromServer");
        try {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DISASSOCIATE_ADDRESS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            setId("", parameters, getIpAddress(addressId), addressId, true);
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
                    throw new CloudException("Release of address denied.");
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    private void setId(@Nonnull String postfix, @Nonnull Map<String,String> parameters, @Nullable IpAddress address, @Nonnull String addressId, @Nullable Boolean disassociate) throws CloudException {
        if( address == null ) {
            throw new CloudException("Invalid IP address: " + addressId);
        }
        String associationId = address.getProviderAssociationId();
        if( address.isForVlan() ) {
          if( disassociate != null && disassociate ) {
            parameters.put("AssociationId" + postfix, associationId);
          } else {
            parameters.put("AllocationId" + postfix, addressId);
          }
        }
        else {
            parameters.put("PublicIp" + postfix, addressId);
        }
    }

    @Override
    public void releaseFromPool(@Nonnull String addressId) throws InternalException, CloudException {
       APITrace.begin(provider, "IpAddress.releaseFromPool");
       try {
           Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.RELEASE_ADDRESS);
           EC2Method method;
           NodeList blocks;
           Document doc;

           setId("", parameters, getIpAddress(addressId), addressId, false);
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
                   throw new CloudException("Deletion of address denied.");
               }
           }
       }
       finally {
           APITrace.end();
       }
   }

    @Override
    public @Nonnull String request(@Nonnull AddressType betterBePublic) throws InternalException, CloudException {
        if( !betterBePublic.equals(AddressType.PUBLIC) ) {
            throw new OperationNotSupportedException("AWS supports only public IP address requests.");
        }
       return request(IPVersion.IPV4);
    }

    @Override
    public @Nonnull String request(@Nonnull IPVersion version) throws InternalException, CloudException {
        APITrace.begin(provider, "IpAddress.request");
        try {
            if( !version.equals(IPVersion.IPV4) ) {
                throw new OperationNotSupportedException(provider.getCloudName() + " does not support " + version);
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.ALLOCATE_ADDRESS);
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
            blocks = doc.getElementsByTagName("publicIp");
            if( blocks.getLength() > 0 ) {
                return blocks.item(0).getFirstChild().getNodeValue().trim();
            }
            throw new CloudException("Unable to create an address.");
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String requestForVLAN(@Nonnull IPVersion forVersion) throws InternalException, CloudException {
        APITrace.begin(provider, "IpAddress.requestForVLAN");
        try {
            if( !forVersion.equals(IPVersion.IPV4) ) {
                throw new OperationNotSupportedException(provider.getCloudName() + " does not support " + forVersion + ".");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.ALLOCATE_ADDRESS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("Domain","vpc");
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("allocationId");
            if( blocks.getLength() > 0 ) {
                return blocks.item(0).getFirstChild().getNodeValue().trim();
            }
            throw new CloudException("Unable to create an address.");
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String requestForVLAN(@Nonnull IPVersion version, @Nonnull String vlanId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("AWS IP addresses may not be assigned to a specific VLAN");
    }

    @Override
    public void stopForward(@Nonnull String ruleId) throws InternalException, CloudException {
        throw new OperationNotSupportedException();
    }

    @Override
    public boolean supportsVLANAddresses(@Nonnull IPVersion version) throws InternalException, CloudException {
        return version.equals(IPVersion.IPV4);
    }

    private @Nullable IpAddress toAddress(@Nonnull ProviderContext ctx, @Nullable Node node) throws CloudException {
      if( node == null ) {
        return null;
      }
      String regionId = ctx.getRegionId();

      if( regionId == null ) {
        throw new CloudException("No regionID was set in context");
      }
      NodeList attrs = node.getChildNodes();
      IpAddress address = new IpAddress();
      String instanceId = null,ip = null;
      String ipAddressId = null, nicId = null,associationId = null;
      boolean forVlan = false;

      for( int i=0; i<attrs.getLength(); i++ ) {
        Node attr = attrs.item(i);
        String name;

        name = attr.getNodeName();
        if( name.equals("publicIp") ) {
          ip = attr.getFirstChild().getNodeValue().trim();
        }
        else if( name.equals("instanceId") ) {
          if( attr.getChildNodes().getLength() > 0 ) {
            Node id = attr.getFirstChild();

            if( id != null ) {
              String value = id.getNodeValue();

              if( value != null ) {
                value = value.trim();
                if( value.length() > 0 ) {
                  instanceId = value;
                }
              }
            }
          }
        }
        else if( name.equals("allocationId") && attr.hasChildNodes() ) {
          ipAddressId = attr.getFirstChild().getNodeValue().trim();
        }
        else if( name.equals("associationId") && attr.hasChildNodes() ) {
          associationId = attr.getFirstChild().getNodeValue().trim();
        }
        else if( name.equals("domain") && attr.hasChildNodes() ) {
          forVlan = attr.getFirstChild().getNodeValue().trim().equalsIgnoreCase("vpc");
          if( !forVlan ) {
            address.setAddressType(AddressType.PUBLIC);
          } else {
            address.setAddressType(AddressType.PRIVATE);
          }
        }
        else if( name.equals("networkInterfaceId") && attr.hasChildNodes() ) {
          nicId = attr.getFirstChild().getNodeValue().trim();
        }
      }
      if( ip == null ) {
        throw new CloudException("Invalid address data, no IP.");
      }
      if( ipAddressId == null ) {
        ipAddressId = ip;
      }
      address.setVersion(IPVersion.IPV4);
      address.setAddressType(AddressType.PUBLIC);
      address.setAddress(ip);
      address.setIpAddressId(ipAddressId);
      address.setRegionId(regionId);
      address.setForVlan(forVlan);
      address.setProviderAssociationId(associationId);
      address.setProviderNetworkInterfaceId(nicId);
      if( instanceId != null && provider.getEC2Provider().isEucalyptus() ) {
        if( instanceId.startsWith("available") ) {
          instanceId = null;
        }
        else {
          int idx = instanceId.indexOf(' ');
          if( idx > 0 ) {
            instanceId = instanceId.substring(0,idx);
          }
        }
      }
      address.setServerId(instanceId);
      return address;
	  }

    private @Nullable ResourceStatus toStatus(@Nullable Node node) throws CloudException {
        if( node == null ) {
          return null;
        }
        NodeList attrs = node.getChildNodes();
        String instanceId = null, ipAddressId = null, nicId = null, ip = null;
        for( int i=0; i<attrs.getLength(); i++ ) {
          Node attr = attrs.item(i);
          String name;
          name = attr.getNodeName();
          if( name.equals("publicIp") ) {
            ip = attr.getFirstChild().getNodeValue().trim();
          }
          else if( name.equals("instanceId") ) {
            if( attr.getChildNodes().getLength() > 0 ) {
              Node id = attr.getFirstChild();
              if( id != null ) {
                String value = id.getNodeValue();
                if( value != null ) {
                  value = value.trim();
                  if( value.length() > 0 ) {
                    instanceId = value;
                  }
                }
              }
            }
          }
          else if( name.equals("allocationId") && attr.hasChildNodes() ) {
            ipAddressId = attr.getFirstChild().getNodeValue().trim();
          }
          else if( name.equals("networkInterfaceId") && attr.hasChildNodes() ) {
            nicId = attr.getFirstChild().getNodeValue().trim();
          }
        }
        if( ipAddressId == null ) {
          ipAddressId = ip;
        }
        if( ipAddressId == null ) {
          return null;
        }
        return new ResourceStatus(ipAddressId, instanceId == null && nicId == null);
    }
}
