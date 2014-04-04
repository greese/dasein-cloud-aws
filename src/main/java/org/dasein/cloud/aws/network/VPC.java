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
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.*;
import org.dasein.cloud.util.APITrace;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorPopulator;
import org.dasein.util.PopulatorThread;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import java.util.*;

public class VPC extends AbstractVLANSupport {
    static private final Logger logger = Logger.getLogger(VPC.class);

    private AWSCloud provider;
    private transient volatile NetworkCapabilities capabilities;

    VPC(AWSCloud provider) {
        super(provider);
        this.provider = provider;
    }

    @Override
    public boolean allowsNewSubnetCreation() throws CloudException, InternalException {
        return getCapabilities().allowsNewSubnetCreation();
    }

    @Override
    public void assignRoutingTableToSubnet(@Nonnull String subnetId, @Nonnull String routingTableId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.assignRoutingTableToSubnet");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.ASSOCIATE_ROUTE_TABLE);
            EC2Method method;

            parameters.put("SubnetId", subnetId);
            parameters.put("RouteTableId", routingTableId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
        } finally {
            APITrace.end();
        }
    }

    @Override
    public void disassociateRoutingTableFromSubnet(@Nonnull String subnetId, @Nonnull String routingTableId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.disassociateRoutingTableFromSubnet");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DISASSOCIATE_ROUTE_TABLE);
            EC2Method method;

            String associationId = getRoutingTableAssociationIdForSubnet(subnetId, routingTableId);
            parameters.put("AssociationId", associationId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
        } finally {
            APITrace.end();
        }
    }

    private @Nonnull String getRoutingTableAssociationIdForSubnet(@Nonnull String subnetId, @Nonnull String routingTableId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.getRoutingTableAssociationIdForSubnet");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_ROUTE_TABLES);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("Filter.1.Name", "association.route-table-id");
            parameters.put("Filter.1.Value.1", routingTableId);
            parameters.put("Filter.2.Name", "association.subnet-id");
            parameters.put("Filter.2.Value.1", subnetId);

            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("associationSet");

            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node set = blocks.item(i);
                NodeList items = set.getChildNodes();

                for( int i1 = 0; i1 < items.getLength(); i1++ ) {
                    Node item = items.item(i1);

                    if( item.getNodeName().equalsIgnoreCase("item") && item.hasChildNodes() ) {
                        NodeList attrs = item.getChildNodes();

                        for( int j = 0; j < attrs.getLength(); j++ ) {
                            Node attr = attrs.item(j);

                            if( attr.getNodeName().equalsIgnoreCase("routeTableAssociationId") && attr.hasChildNodes() ) {
                                return attr.getFirstChild().getNodeValue().trim();
                            }
                        }
                    }
                }
            }
            throw new CloudException("Could not identify the association between subnet " + subnetId + " and routing table " + routingTableId);
        } finally {
            APITrace.end();
        }
    }

    private @Nonnull String getMainRoutingTableAssociationIdForVlan(@Nonnull String vlanId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.getMainRoutingTableAssociationIdForVlan");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_ROUTE_TABLES);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("Filter.1.Name", "association.main");
            parameters.put("Filter.1.Value.1", "true");
            parameters.put("Filter.2.Name", "vpc-id");
            parameters.put("Filter.2.Value.1", vlanId);

            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("associationSet");

            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node set = blocks.item(i);
                NodeList items = set.getChildNodes();

                for( int i1 = 0; i1 < items.getLength(); i1++ ) {
                    Node item = items.item(i1);

                    if( item.getNodeName().equalsIgnoreCase("item") && item.hasChildNodes() ) {
                        NodeList attrs = item.getChildNodes();

                        for( int j = 0; j < attrs.getLength(); j++ ) {
                            Node attr = attrs.item(j);

                            if( attr.getNodeName().equalsIgnoreCase("routeTableAssociationId") && attr.hasChildNodes() ) {
                                return attr.getFirstChild().getNodeValue().trim();
                            }
                        }
                    }
                }
            }
            throw new CloudException("Could not identify the main routing table for " + vlanId);
        } finally {
            APITrace.end();
        }
    }

    @Override
    public void assignRoutingTableToVlan(@Nonnull String vlanId, @Nonnull String routingTableId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.assignRoutingTableToVlan");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            String associationId = getMainRoutingTableAssociationIdForVlan(vlanId);

            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.REPLACE_ROUTE_TABLE_ASSOCIATION);
            EC2Method method;

            parameters.put("AssociationId", associationId);
            parameters.put("RouteTableId", routingTableId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
        } finally {
            APITrace.end();
        }
    }

    @Override
    public void attachNetworkInterface(@Nonnull String nicId, @Nonnull String vmId, int index) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.attachNetworkInterface");
        try {
            if( index < 1 ) {
                index = 1;
                for( NetworkInterface nic : listNetworkInterfacesForVM(vmId) ) {
                    if( nic != null ) {
                        index++;
                    }
                }
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.ATTACH_NIC);
            EC2Method method;

            parameters.put("NetworkInterfaceId", nicId);
            parameters.put("InstanceId", vmId);
            parameters.put("DeviceIndex", String.valueOf(index));
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
        } finally {
            APITrace.end();
        }
    }

    @Override
    public String createInternetGateway(@Nonnull String vlanId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.createInternetGateway");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.CREATE_INTERNET_GATEWAY);
            EC2Method method;
            NodeList blocks;
            Document doc;

            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            String gatewayId = null;

            blocks = doc.getElementsByTagName("internetGatewayId");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node item = blocks.item(i);

                if( item.hasChildNodes() ) {
                    gatewayId = item.getFirstChild().getNodeValue().trim();
                }
            }
            if( gatewayId == null ) {
                throw new CloudException("No internet gateway was created, but no error was reported");
            }
            parameters = provider.getStandardParameters(provider.getContext(), EC2Method.ATTACH_INTERNET_GATEWAY);
            parameters.put("VpcId", vlanId);
            parameters.put("InternetGatewayId", gatewayId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                method.invoke();
                return gatewayId;
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String createRoutingTable(@Nonnull String vlanId, @Nonnull String name, @Nonnull String description) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.createRoutingTable");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.CREATE_ROUTE_TABLE);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("VpcId", vlanId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("routeTable");
            String id = null;

            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                RoutingTable table = toRoutingTable(ctx, item);

                if( table != null ) {
                    id = table.getProviderRoutingTableId();
                    break;
                }
            }
            if( id == null ) {
                throw new CloudException("No table was created, but no error was reported");
            }
            Tag[] tags = new Tag[2];
            Tag t = new Tag();

            t.setKey("Name");
            t.setValue(name);
            tags[0] = t;
            t = new Tag();
            t.setKey("Description");
            t.setValue(description);
            tags[1] = t;
            provider.createTags(id, tags);
            return id;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull NetworkInterface createNetworkInterface(@Nonnull NICCreateOptions options) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.createNetworkInterface");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.CREATE_NIC);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("SubnetId", options.getSubnetId());
            if( options.getIpAddress() != null ) {
                parameters.put("PrivateIpAddress", options.getIpAddress());
            }
            parameters.put("Description", options.getDescription());
            if( options.getFirewallIds().length > 0 ) {
                int i = 1;

                for( String id : options.getFirewallIds() ) {
                    parameters.put("SecurityGroupId." + i, id);
                }
            }
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("networkInterface");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                NetworkInterface nic = toNIC(ctx, item);

                if( nic != null ) {
                    Tag[] tags = new Tag[2];
                    Tag t = new Tag();

                    t.setKey("Name");
                    t.setValue(options.getName());
                    tags[0] = t;
                    t = new Tag();
                    t.setKey("Description");
                    t.setValue(options.getDescription());
                    tags[1] = t;
                    provider.createTags(nic.getProviderNetworkInterfaceId(), tags);
                    nic.setName(options.getName());
                    nic.setDescription(options.getDescription());
                    return nic;
                }
            }
            throw new CloudException("No network interface was created, but no error was reported");
        } finally {
            APITrace.end();
        }
    }

    @Override
    public Route addRouteToAddress(@Nonnull String routingTableId, @Nonnull IPVersion version, @Nullable String destinationCidr, @Nonnull String address) throws CloudException, InternalException {
        throw new OperationNotSupportedException("You cannot route to a raw IP address in " + provider.getCloudName() + ".");
    }

    @Override
    public Route addRouteToGateway(@Nonnull String routingTableId, @Nonnull IPVersion version, @Nullable String destinationCidr, @Nonnull String gatewayId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.addRouteToGateway");
        try {
            if( !version.equals(IPVersion.IPV4) ) {
                throw new CloudException(provider.getCloudName() + " does not support " + version);
            }
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.CREATE_ROUTE);
            EC2Method method;

            parameters.put("GatewayId", gatewayId);
            parameters.put("RouteTableId", routingTableId);
            parameters.put("DestinationCidrBlock", destinationCidr);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            return Route.getRouteToGateway(version, destinationCidr, gatewayId);
        } finally {
            APITrace.end();
        }
    }

    @Override
    public Route addRouteToNetworkInterface(@Nonnull String routingTableId, @Nonnull IPVersion version, @Nullable String destinationCidr, @Nonnull String nicId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.addRouteToNetworkInterface");
        try {
            if( !version.equals(IPVersion.IPV4) ) {
                throw new CloudException(provider.getCloudName() + " does not support " + version);
            }
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.CREATE_ROUTE);
            EC2Method method;

            parameters.put("NetworkInterfaceId", nicId);
            parameters.put("RouteTableId", routingTableId);
            parameters.put("DestinationCidrBlock", destinationCidr);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            return Route.getRouteToNetworkInterface(version, destinationCidr, nicId);
        } finally {
            APITrace.end();
        }
    }

    @Override
    public Route addRouteToVirtualMachine(@Nonnull String routingTableId, @Nonnull IPVersion version, @Nullable String destinationCidr, @Nonnull String vmId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.addRouteToVirtualMachine");
        try {
            if( !version.equals(IPVersion.IPV4) ) {
                throw new CloudException(provider.getCloudName() + " does not support " + version);
            }
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.CREATE_ROUTE);
            EC2Method method;

            parameters.put("InstanceId", vmId);
            parameters.put("RouteTableId", routingTableId);
            parameters.put("DestinationCidrBlock", destinationCidr);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            // TODO: Find out what ownerId should be
            return Route.getRouteToVirtualMachine(version, destinationCidr, "", vmId);
        } finally {
            APITrace.end();
        }
    }

    @Override
    public boolean allowsNewNetworkInterfaceCreation() throws CloudException, InternalException {
        return getCapabilities().allowsNewNetworkInterfaceCreation();
    }

    @Override
    public boolean allowsNewVlanCreation() throws CloudException, InternalException {
        return getCapabilities().allowsNewVlanCreation();
    }

    @Override
    public boolean allowsNewRoutingTableCreation() throws CloudException, InternalException {
        return getCapabilities().allowsNewRoutingTableCreation();
    }

    private void assignDHCPOptions(VLAN vlan, String domainName, String[] dnsServers, String[] ntpServers) throws CloudException, InternalException {
        boolean differs = false;

        if( vlan.getDomainName() != null ) {
            if( domainName == null ) {
                differs = true;
            } else if( !domainName.equals(vlan.getDomainName()) ) {
                differs = true;
            }
        } else if( domainName != null ) {
            differs = true;
        }
        if( !differs ) {
            if( vlan.getDnsServers() != null ) {
                if( dnsServers == null || vlan.getDnsServers().length != dnsServers.length ) {
                    differs = true;
                } else {
                    for( int i = 0; i < dnsServers.length; i++ ) {
                        if( !dnsServers[i].equalsIgnoreCase(vlan.getDnsServers()[i]) ) {
                            differs = true;
                            break;
                        }
                    }
                }
            } else if( dnsServers != null ) {
                if( vlan.getDnsServers().length != dnsServers.length ) {
                    differs = true;
                } else {
                    for( int i = 0; i < dnsServers.length; i++ ) {
                        if( !dnsServers[i].equalsIgnoreCase(vlan.getDnsServers()[i]) ) {
                            differs = true;
                            break;
                        }
                    }
                }
            }
            if( !differs ) {
                if( vlan.getNtpServers() != null ) {
                    if( ntpServers == null || vlan.getNtpServers().length != ntpServers.length ) {
                        differs = true;
                    } else {
                        for( int i = 0; i < ntpServers.length; i++ ) {
                            if( !ntpServers[i].equalsIgnoreCase(vlan.getNtpServers()[i]) ) {
                                differs = true;
                                break;
                            }
                        }
                    }
                } else if( ntpServers != null ) {
                    if( vlan.getNtpServers().length != ntpServers.length ) {
                        differs = true;
                    } else {
                        for( int i = 0; i < ntpServers.length; i++ ) {
                            if( !ntpServers[i].equalsIgnoreCase(vlan.getNtpServers()[i]) ) {
                                differs = true;
                                break;
                            }
                        }
                    }
                }
            }
        }
        if( differs ) {
            String dhcp = createDhcp(domainName, dnsServers, ntpServers);

            assignDhcp(vlan.getProviderVlanId(), dhcp);
        }
    }

    private void assignDhcp(String vlanId, String dhcp) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.assignDhcp");
        try {
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.ASSOCIATE_DHCP_OPTIONS);
            EC2Method method;

            parameters.put("DhcpOptionsId", dhcp);
            parameters.put("VpcId", vlanId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
        } finally {
            APITrace.end();
        }
    }

    private String createDhcp(String domainName, String[] dnsServers, String[] ntpServers) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.createDhcp");
        try {
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.CREATE_DHCP_OPTIONS);
            EC2Method method;
            NodeList blocks;
            Document doc;
            int idx = 1;

            if( domainName != null ) {
                parameters.put("DhcpConfiguration." + idx + ".Key", "domain-name");
                parameters.put("DhcpConfiguration." + idx + ".Value.1", domainName);
                idx++;
            }
            if( dnsServers != null && dnsServers.length > 0 ) {
                int vdx = 1;

                parameters.put("DhcpConfiguration." + idx + ".Key", "domain-name-servers");
                for( String dns : dnsServers ) {
                    parameters.put("DhcpConfiguration." + idx + ".Value." + vdx, dns);
                    vdx++;
                }
                idx++;
            }
            if( ntpServers != null && ntpServers.length > 0 ) {
                int vdx = 1;

                parameters.put("DhcpConfiguration." + idx + ".Key", "ntp-servers");
                for( String ntp : ntpServers ) {
                    parameters.put("DhcpConfiguration." + idx + ".Value." + vdx, ntp);
                    vdx++;
                }
            }
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("dhcpOptionsId");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node id = blocks.item(i);

                if( id != null ) {
                    return id.getFirstChild().getNodeValue().trim();
                }
            }
            throw new CloudException("No DHCP options were created, but no error was reported");
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Subnet createSubnet(@Nonnull SubnetCreateOptions options) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.createSubnet");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.CREATE_SUBNET);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("CidrBlock", options.getCidr());
            parameters.put("VpcId", options.getProviderVlanId());

            String dc = options.getProviderDataCenterId();

            if( dc != null ) {
                parameters.put("AvailabilityZone", dc);
            }
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("subnet");

            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                Subnet subnet = toSubnet(ctx, item);

                if( subnet != null ) {
                    Map<String, Object> metaData = new HashMap<String, Object>();

                    metaData.put("Name", options.getName());
                    metaData.put("Description", options.getDescription());
                    Tag[] tags = new Tag[metaData.size()];
                    int j = 0;

                    for( Map.Entry<String, Object> entry : metaData.entrySet() ) {
                        Tag t = new Tag();

                        t.setKey(entry.getKey());
                        t.setValue(entry.getValue().toString());
                        tags[j++] = t;
                    }
                    provider.createTags(subnet.getProviderSubnetId(), tags);
                    subnet.setName(options.getDescription());
                    subnet.setDescription(options.getDescription());
                    return subnet;
                }
            }
            throw new CloudException("No subnet was created, but no error was reported");
        } finally {
            APITrace.end();
        }
    }

    @Override
    @Deprecated
    public @Nonnull VLAN createVlan(@Nonnull String cidr, @Nonnull String name, @Nonnull String description, @Nullable String domainName, @Nonnull String[] dnsServers, @Nonnull String[] ntpServers) throws CloudException, InternalException {
        VlanCreateOptions vco = VlanCreateOptions.getInstance(name, description, cidr, domainName, dnsServers, ntpServers);
        return createVlan(vco);
    }

    @Override
    public @Nonnull VLAN createVlan(final @Nonnull VlanCreateOptions options) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.createVLAN");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.CREATE_VPC);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("CidrBlock", options.getCidr());
            parameters.put("InstanceTenancy", "default");
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("vpc");

            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                VLAN vlan = toVLAN(ctx, item);

                if( vlan != null ) {
                    String domain = options.getDomain();
                    String[] dns = options.getDnsServers();
                    String[] ntp = options.getNtpServers();
                    if( domain != null || dns != null || ntp != null ) {
                        assignDHCPOptions(vlan, options.getDomain(), options.getDnsServers(), options.getNtpServers());
                    }
                    Tag[] tags = new Tag[2];
                    Tag t = new Tag();

                    t.setKey("Name");
                    t.setValue(options.getName());
                    tags[0] = t;
                    t = new Tag();
                    t.setKey("Description");
                    t.setValue(options.getDescription());
                    tags[1] = t;
                    provider.createTags(vlan.getProviderVlanId(), tags);
                    vlan.setName(options.getName());
                    vlan.setDescription(options.getDescription());
                    return vlan;
                }
            }
            throw new CloudException("No VLAN was created, but no error was reported");
        } finally {
            APITrace.end();
        }
    }

    static private class Attachment {
        public String attachmentId;
        public String virtualMachineId;
    }

    private @Nonnull Collection<Attachment> getAttachments(@Nonnull String nicId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.getAttachments");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_NICS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("NetworkInterfaceId.1", nicId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                if( e.getCode() != null && e.getCode().startsWith("InvalidNetworkInterfaceID") ) {
                    Collections.emptyList();
                }
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            ArrayList<Attachment> attachments = new ArrayList<Attachment>();

            blocks = doc.getElementsByTagName("item");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node item = blocks.item(i);

                NodeList attrs = item.getChildNodes();

                for( int j = 0; j < attrs.getLength(); j++ ) {
                    Node attr = attrs.item(j);

                    if( attr.getNodeName().equalsIgnoreCase("attachment") ) {
                        NodeList parts = attr.getChildNodes();
                        Attachment a = new Attachment();

                        for( int k = 0; k < parts.getLength(); k++ ) {
                            Node part = parts.item(k);

                            if( part.getNodeName().equalsIgnoreCase("instanceId") && part.hasChildNodes() ) {
                                a.virtualMachineId = part.getFirstChild().getNodeValue().trim();
                            } else if( part.getNodeName().equalsIgnoreCase("attachmentId") && part.hasChildNodes() ) {
                                a.attachmentId = part.getFirstChild().getNodeValue().trim();
                            }
                        }
                        if( a.virtualMachineId != null && a.attachmentId != null ) {
                            attachments.add(a);
                        }
                    }
                }
            }
            return attachments;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public void detachNetworkInterface(@Nonnull String nicId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.detachNetworkInterface");
        try {
            Collection<Attachment> attachments = getAttachments(nicId);

            for( Attachment a : attachments ) {
                Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DETACH_NIC);
                EC2Method method;

                parameters.put("AttachmentId", a.attachmentId);
                method = new EC2Method(provider, provider.getEc2Url(), parameters);
                try {
                    method.invoke();
                } catch( EC2Exception e ) {
                    logger.error(e.getSummary());
                    if( logger.isDebugEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);
                }
            }
        } finally {
            APITrace.end();
        }
    }

    @Override
    public VLANCapabilities getCapabilities() {
        if( capabilities == null ) {
            capabilities = new NetworkCapabilities(provider);
        }
        return capabilities;
    }

    @Override
    public int getMaxNetworkInterfaceCount() throws CloudException, InternalException {
        return getCapabilities().getMaxNetworkInterfaceCount();
    }

    @Override
    public int getMaxVlanCount() throws CloudException, InternalException {
        return getCapabilities().getMaxVlanCount();
    }

    @Override
    public @Nonnull String getProviderTermForNetworkInterface(@Nonnull Locale locale) {
        return getCapabilities().getProviderTermForNetworkInterface(locale);
    }

    @Override
    public @Nonnull String getProviderTermForSubnet(@Nonnull Locale locale) {
        return getCapabilities().getProviderTermForSubnet(locale);
    }

    @Override
    public @Nonnull String getProviderTermForVlan(@Nonnull Locale locale) {
        return getProviderTermForVlan(locale);
    }

    @Override
    public NetworkInterface getNetworkInterface(@Nonnull String nicId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.getNetworkInterface");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_NICS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("NetworkInterfaceId.1", nicId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                if( e.getCode() != null && e.getCode().toLowerCase().startsWith("invalidnetworkinterfaceid") ) {
                    return null;
                }
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("item");

            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                NetworkInterface nic = toNIC(ctx, item);

                if( nic != null ) {
                    return nic;
                }
            }
            return null;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public RoutingTable getRoutingTableForSubnet(@Nonnull String subnetId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.getRoutingTableForSubnet");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_ROUTE_TABLES);
            EC2Method method;
            NodeList blocks;
            Document doc;

            //parameters.put("Filter.1.Name", "association.main");
            //parameters.put("Filter.1.Value.1", "true");
            parameters.put("Filter.1.Name", "association.subnet-id");
            parameters.put("Filter.1.Value.1", subnetId);

            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("routeTableSet");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node set = blocks.item(i);
                NodeList items = set.getChildNodes();

                for( int j = 0; j < items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equalsIgnoreCase("item") && item.hasChildNodes() ) {
                        RoutingTable t = toRoutingTable(ctx, item);

                        if( t != null ) {
                            return t;
                        }
                    }
                }
            }
            throw new CloudException("Could not identify the subnet routing table for " + subnetId);
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Requirement getRoutingTableSupport() throws CloudException, InternalException {
        return getCapabilities().getRoutingTableSupport();
    }

    @Override
    public RoutingTable getRoutingTableForVlan(@Nonnull String vlanId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.getRoutingTableForVlan");
        try {
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_ROUTE_TABLES);
            parameters.put("Filter.1.Name", "association.main");
            parameters.put("Filter.1.Value.1", "true");
            parameters.put("Filter.2.Name", "vpc-id");
            parameters.put("Filter.2.Value.1", vlanId);
            RoutingTable rt = getRoutingTableAbstract(parameters);
            // here for backwards compatability - should just return null similar to other resources
            if( rt == null ) {
                throw new CloudException("Could not identify the main routing table for " + vlanId);
            } else {
                return rt;
            }
        } finally {
            APITrace.end();
        }
    }

    @Override
    public RoutingTable getRoutingTable(@Nonnull String id) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.getRoutingTable");
        try {
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_ROUTE_TABLES);
            parameters.put("Filter.1.Name", "route-table-id");
            parameters.put("Filter.1.Value.1", id);
            return getRoutingTableAbstract(parameters);
        } finally {
            APITrace.end();
        }
    }

    private RoutingTable getRoutingTableAbstract(@Nonnull Map<String, String> parameters) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was configured");
        }

        EC2Method method;
        NodeList blocks;
        Document doc;

        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            doc = method.invoke();
        } catch( EC2Exception e ) {
            logger.error(e.getSummary());
            if( logger.isDebugEnabled() ) {
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("routeTableSet");
        for( int i = 0; i < blocks.getLength(); i++ ) {
            Node set = blocks.item(i);
            NodeList items = set.getChildNodes();

            for( int j = 0; j < items.getLength(); j++ ) {
                Node item = items.item(j);

                if( item.getNodeName().equalsIgnoreCase("item") && item.hasChildNodes() ) {
                    RoutingTable t = toRoutingTable(ctx, item);

                    if( t != null ) {
                        return t;
                    }
                }
            }
        }
        return null;
    }

    @Override
    public @Nullable Subnet getSubnet(@Nonnull String providerSubnetId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.getSubnet");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_SUBNETS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("SubnetId.1", providerSubnetId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                if( e.getCode() != null && e.getCode().startsWith("InvalidSubnetID") ) {
                    return null;
                }
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("item");

            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                Subnet subnet = toSubnet(ctx, item);

                if( subnet != null ) {
                    return subnet;
                }
            }
            return null;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Requirement getSubnetSupport() throws CloudException, InternalException {
        return getCapabilities().getSubnetSupport();
    }

    @Override
    public @Nullable VLAN getVlan(@Nonnull String providerVlanId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.getVlan");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_VPCS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("VpcId.1", providerVlanId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                if( e.getCode() != null && e.getCode().startsWith("InvalidVpcID") ) {
                    return null;
                }
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("item");

            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                VLAN vlan = toVLAN(ctx, item);

                if( vlan != null ) {
                    return vlan;
                }
            }
            return null;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Requirement identifySubnetDCRequirement() {
        try {
            return getCapabilities().identifySubnetDCRequirement();
        } catch( CloudException e ) {
        } catch( InternalException e ) {
        }
        return null;
    }

    @Override
    public boolean isConnectedViaInternetGateway(@Nonnull String vlanId) throws CloudException, InternalException {
        return ( getAttachedInternetGatewayId(vlanId) != null );
    }

    @Override
    public boolean isNetworkInterfaceSupportEnabled() throws CloudException, InternalException {
        return getCapabilities().isNetworkInterfaceSupportEnabled();
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.isSubscribed");
        try {
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_VPCS);
            EC2Method method;

            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                method.invoke();
                return true;
            } catch( EC2Exception e ) {
                if( e.getStatus() == HttpServletResponse.SC_UNAUTHORIZED || e.getStatus() == HttpServletResponse.SC_FORBIDDEN ) {
                    return false;
                }
                String code = e.getCode();

                if( code != null && ( code.equals("SubscriptionCheckFailed") || code.equals("AuthFailure") || code.equals("SignatureDoesNotMatch") || code.equals("UnsupportedOperation") || code.equals("InvalidClientTokenId") || code.equals("OptInRequired") ) ) {
                    return false;
                }
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
        } finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isVlanDataCenterConstrained() throws CloudException, InternalException {
        return getCapabilities().isVlanDataCenterConstrained();
    }

    @Override
    public @Nonnull Collection<String> listFirewallIdsForNIC(@Nonnull String nicId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.listFirewallIdsForNIC");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_NICS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("NetworkInterfaceId.1", nicId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                if( e.getCode() != null && e.getCode().startsWith("InvalidNetworkInterfaceID") ) {
                    Collections.emptyList();
                }
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            TreeSet<String> firewallIds = new TreeSet<String>();

            blocks = doc.getElementsByTagName("item");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node item = blocks.item(i);

                NodeList attrs = item.getChildNodes();

                for( int j = 0; j < attrs.getLength(); j++ ) {
                    Node attr = attrs.item(j);

                    if( attr.getNodeName().equalsIgnoreCase("groupSet") ) {
                        NodeList parts = attr.getChildNodes();

                        for( int k = 0; k < parts.getLength(); k++ ) {
                            Node part = parts.item(k);

                            if( part.getNodeName().equalsIgnoreCase("item") && part.hasChildNodes() ) {
                                NodeList fws = part.getChildNodes();

                                for( int l = 0; l < fws.getLength(); l++ ) {
                                    Node fw = fws.item(l);

                                    if( fw.hasChildNodes() ) {
                                        NodeList faList = fw.getChildNodes();

                                        for( int m = 0; m < faList.getLength(); m++ ) {
                                            Node fa = faList.item(m);

                                            if( fa.getNodeName().equalsIgnoreCase("groupId") && fa.hasChildNodes() ) {
                                                firewallIds.add(fw.getFirstChild().getNodeValue().trim());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return firewallIds;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listNetworkInterfaceStatus() throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.listNetworkInterfaceStatus");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_NICS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            ArrayList<ResourceStatus> nics = new ArrayList<ResourceStatus>();
            blocks = doc.getElementsByTagName("item");

            for( int i = 0; i < blocks.getLength(); i++ ) {
                ResourceStatus status = toNICStatus(blocks.item(i));

                if( status != null ) {
                    nics.add(status);
                }
            }
            return nics;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<NetworkInterface> listNetworkInterfaces() throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.listNetworkInterfaces");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_NICS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            ArrayList<NetworkInterface> nics = new ArrayList<NetworkInterface>();
            blocks = doc.getElementsByTagName("item");

            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                NetworkInterface nic = toNIC(ctx, item);

                if( nic != null ) {
                    nics.add(nic);
                }
            }
            return nics;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<NetworkInterface> listNetworkInterfacesForVM(@Nonnull String forVmId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.listNetworkInterfacesForVM");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_NICS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("Filter.1.Name", "attachment.instance-id");
            parameters.put("Filter.1.Value.1", forVmId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            ArrayList<NetworkInterface> nics = new ArrayList<NetworkInterface>();
            blocks = doc.getElementsByTagName("item");

            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                NetworkInterface nic = toNIC(ctx, item);

                if( nic != null ) {
                    nics.add(nic);
                }
            }
            return nics;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<NetworkInterface> listNetworkInterfacesInSubnet(@Nonnull String subnetId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.listNetworkInterfacesInSubnet");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_NICS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("Filter.1.Name", "subnet-id");
            parameters.put("Filter.1.Value.1", subnetId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            ArrayList<NetworkInterface> nics = new ArrayList<NetworkInterface>();
            blocks = doc.getElementsByTagName("item");

            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                NetworkInterface nic = toNIC(ctx, item);

                if( nic != null ) {
                    nics.add(nic);
                }
            }
            return nics;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<NetworkInterface> listNetworkInterfacesInVLAN(@Nonnull String vlanId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.listNetworkInterfacesInVLAN");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_NICS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("Filter.1.Name", "vpc-id");
            parameters.put("Filter.1.Value.1", vlanId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            ArrayList<NetworkInterface> nics = new ArrayList<NetworkInterface>();
            blocks = doc.getElementsByTagName("item");

            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                NetworkInterface nic = toNIC(ctx, item);

                if( nic != null ) {
                    nics.add(nic);
                }
            }
            return nics;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public
    @Nonnull Iterable<Networkable> listResources(final @Nonnull String vlanId) throws CloudException, InternalException {
        getProvider().hold();
        PopulatorThread<Networkable> populator = new PopulatorThread<Networkable>(new JiteratorPopulator<Networkable>() {
            @Override
            public void populate(@Nonnull Jiterator<Networkable> iterator) throws Exception {
                try {
                    APITrace.begin(getProvider(), "VLAN.listResources");
                    try {
                        NetworkServices network = provider.getNetworkServices();

                        if( network != null ) {
                            FirewallSupport fwSupport = network.getFirewallSupport();

                            if( fwSupport != null ) {
                                for( Firewall fw : fwSupport.list() ) {
                                    if( vlanId.equals(fw.getProviderVlanId()) ) {
                                        iterator.push(fw);
                                    }
                                }
                            }

                            IpAddressSupport ipSupport = network.getIpAddressSupport();

                            if( ipSupport != null ) {
                                for( IPVersion version : ipSupport.listSupportedIPVersions() ) {
                                    for( IpAddress addr : ipSupport.listIpPool(version, false) ) {
                                        if( vlanId.equals(addr.getProviderVlanId()) ) {
                                            iterator.push(addr);
                                        }
                                    }

                                }
                            }
                            for( RoutingTable table : listRoutingTables(vlanId) ) {
                                iterator.push(table);
                            }
                            ComputeServices compute = provider.getComputeServices();
                            VirtualMachineSupport vmSupport = compute == null ? null : compute.getVirtualMachineSupport();
                            Iterable<VirtualMachine> vms;

                            if( vmSupport == null ) {
                                vms = Collections.emptyList();
                            } else {
                                vms = vmSupport.listVirtualMachines();
                            }
                            for( Subnet subnet : listSubnets(vlanId) ) {
                                iterator.push(subnet);
                                for( VirtualMachine vm : vms ) {
                                    if( subnet.getProviderSubnetId().equals(vm.getProviderVlanId()) ) {
                                        iterator.push(vm);
                                    }
                                }
                            }
                        }
                    } finally {
                        APITrace.end();
                    }
                } finally {
                    getProvider().release();
                }
            }
        });

        populator.populate();
        return populator.getResult();
    }

    @Override
    public @Nonnull Iterable<RoutingTable> listRoutingTablesForSubnet(@Nonnull String subnetId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.listRoutingTablesForSubnet");
        try {
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_ROUTE_TABLES);
            parameters.put("Filter.1.Name", "association.subnet-id");
            parameters.put("Filter.1.Value.1", subnetId);
            return listRoutingTablesForResource(parameters);
        } finally {
            APITrace.end();
        }
    }

    @Override
    @Deprecated
    public @Nonnull Iterable<RoutingTable> listRoutingTables(@Nonnull String vlanId) throws CloudException, InternalException {
        return listRoutingTablesForVlan(vlanId);
    }

    @Override
    public @Nonnull Iterable<RoutingTable> listRoutingTablesForVlan(@Nullable String vlanId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.listRoutingTablesForVlan");
        try {
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_ROUTE_TABLES);
            if( vlanId != null ) {
                parameters.put("Filter.1.Name", "vpc-id");
                parameters.put("Filter.1.Value.1", vlanId);
            }
            return listRoutingTablesForResource(parameters);
        } finally {
            APITrace.end();
        }
    }

    private @Nonnull Iterable<RoutingTable> listRoutingTablesForResource(@Nonnull Map<String, String> params) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was configured");
        }

        EC2Method method;
        Document doc;

        method = new EC2Method(provider, provider.getEc2Url(), params);
        try {
            doc = method.invoke();
        } catch( EC2Exception e ) {
            logger.error(e.getSummary());
            if( logger.isDebugEnabled() ) {
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
        ArrayList<RoutingTable> tables = new ArrayList<RoutingTable>();
        NodeList blocks = doc.getElementsByTagName("routeTableSet");

        for( int i = 0; i < blocks.getLength(); i++ ) {
            Node set = blocks.item(i);
            NodeList items = set.getChildNodes();

            for( int j = 0; j < items.getLength(); j++ ) {
                Node item = items.item(j);

                if( item.getNodeName().equalsIgnoreCase("item") && item.hasChildNodes() ) {
                    RoutingTable t = toRoutingTable(ctx, item);

                    if( t != null ) {
                        tables.add(t);
                    }
                }
            }
        }
        return tables;
    }

    @Override
    public boolean isSubnetDataCenterConstrained() throws CloudException, InternalException {
        return getCapabilities().isSubnetDataCenterConstrained();
    }

    @Override
    public @Nonnull Iterable<Subnet> listSubnets(@Nullable String providerVlanId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.listSubnets");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_SUBNETS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            if( providerVlanId != null && !providerVlanId.equals("") ) {
                parameters.put("Filter.1.Name", "vpc-id");
                parameters.put("Filter.1.Value.1", providerVlanId);
            }
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("item");

            ArrayList<Subnet> list = new ArrayList<Subnet>();

            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                Subnet subnet = toSubnet(ctx, item);

                if( subnet != null ) {
                    list.add(subnet);
                }
            }
            return list;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        return getCapabilities().listSupportedIPVersions();
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listVlanStatus() throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.listVlanStatus");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_VPCS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("item");

            ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();

            for( int i = 0; i < blocks.getLength(); i++ ) {
                ResourceStatus status = toVLANStatus(blocks.item(i));

                if( status != null ) {
                    list.add(status);
                }
            }
            return list;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<VLAN> listVlans() throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.listVlans");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_VPCS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("item");

            ArrayList<VLAN> list = new ArrayList<VLAN>();

            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                VLAN vlan = toVLAN(ctx, item);

                if( vlan != null ) {
                    list.add(vlan);
                }
            }
            return list;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nullable String getAttachedInternetGatewayId(@Nonnull String vlanId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.getAttachedInternetGatewayId");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_INTERNET_GATEWAYS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("Filter.1.Name", "attachment.vpc-id");
            parameters.put("Filter.1.Value.1", vlanId);

            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("internetGatewaySet");

            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node set = blocks.item(i);
                NodeList items = set.getChildNodes();

                for( int i1 = 0; i1 < items.getLength(); i1++ ) {
                    Node item = items.item(i1);

                    if( item.getNodeName().equalsIgnoreCase("item") && item.hasChildNodes() ) {
                        NodeList attrs = item.getChildNodes();

                        for( int j = 0; j < attrs.getLength(); j++ ) {
                            Node attr = attrs.item(j);

                            if( attr.getNodeName().equalsIgnoreCase("internetGatewayId") && attr.hasChildNodes() ) {
                                return attr.getFirstChild().getNodeValue().trim();
                            }
                        }
                    }
                }
            }
            return null;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nullable InternetGateway getInternetGatewayById(@Nonnull String gatewayId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.getInternetGatewayById");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_INTERNET_GATEWAYS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("InternetGatewayId.1", gatewayId);

            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                if( e.getCode() != null && e.getCode().startsWith("InvalidInternetGatewayID") ) {
                    return null;
                }
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("internetGatewaySet");
            Node set = blocks.item(0);
            NodeList items = set.getChildNodes();
            Node item = items.item(1);
            return toInternetGateway(ctx, item);
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Collection<InternetGateway> listInternetGateways(@Nullable String vlanId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.listInternetGateways");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured");
            }
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_INTERNET_GATEWAYS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            if( vlanId != null ) {
                parameters.put("Filter.1.Name", "attachment.vpc-id");
                parameters.put("Filter.1.Value.1", vlanId);
            }

            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("internetGatewaySet");
            Node set = blocks.item(0);
            NodeList items = set.getChildNodes();

            ArrayList<InternetGateway> list = new ArrayList<InternetGateway>();

            for( int i1 = 0; i1 < items.getLength(); i1++ ) {
                Node item = items.item(i1);
                InternetGateway ig = toInternetGateway(ctx, item);

                if( ig != null ) {
                    list.add(ig);
                }
            }
            return list;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public void removeInternetGateway(@Nonnull String vlanId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.removeInternetGateway");
        try {
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DETACH_INTERNET_GATEWAY);
            String gatewayId = getAttachedInternetGatewayId(vlanId);

            if( gatewayId == null ) {
                return; // NO-OP
            }
            EC2Method method;

            parameters.put("InternetGatewayId", gatewayId);
            parameters.put("VpcId", vlanId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            removeGateway(gatewayId);
        } finally {
            APITrace.end();
        }
    }

    @Override
    public void removeInternetGatewayById(@Nonnull String id) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.removeInternetGatewayById");
        try {
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DETACH_INTERNET_GATEWAY);
            EC2Method method;

            InternetGateway ig = getInternetGatewayById(id);

            if( ig == null ) {
                throw new CloudException("No such internet gateway with id " + id);
            }

            parameters.put("InternetGatewayId", id);
            parameters.put("VpcId", ig.getProviderVlanId());
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            removeGateway(id);
        } finally {
            APITrace.end();
        }
    }

    private void removeGateway(@Nonnull String gatewayId) throws CloudException, InternalException {
        Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DELETE_INTERNET_GATEWAY);

        parameters.put("InternetGatewayId", gatewayId);

        EC2Method method = new EC2Method(provider, provider.getEc2Url(), parameters);

        try {
            method.invoke();
        } catch( EC2Exception e ) {
            logger.error(e.getSummary());
            if( logger.isDebugEnabled() ) {
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
    }

    @Override
    public void removeNetworkInterface(@Nonnull String nicId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.removeNetworkInterface");
        try {
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DELETE_NIC);
            EC2Method method;

            parameters.put("NetworkInterfaceId", nicId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
        } finally {
            APITrace.end();
        }
    }

    @Override
    public void removeRoute(@Nonnull String inRoutingTableId, @Nonnull String destinationCidr) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.removeRoute");
        try {
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DELETE_ROUTE);
            EC2Method method;

            parameters.put("RouteTableId", inRoutingTableId);
            parameters.put("DestinationCidrBlock", destinationCidr);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
        } finally {
            APITrace.end();
        }
    }

    @Override
    public void removeRoutingTable(@Nonnull String routingTableId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.removeRoutingTable");
        try {
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DELETE_ROUTE_TABLE);
            EC2Method method;

            parameters.put("RouteTableId", routingTableId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
        } finally {
            APITrace.end();
        }
    }

    private void loadDHCPOptions(String dhcpOptionsId, VLAN vlan) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.loadDHCPOptions");
        try {
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_DHCP_OPTIONS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("DhcpOptionsId.1", dhcpOptionsId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("dhcpConfigurationSet");

            for( int i = 0; i < blocks.getLength(); i++ ) {
                Node config = blocks.item(i);

                if( config.hasChildNodes() ) {
                    NodeList items = config.getChildNodes();

                    for( int j = 0; j < items.getLength(); j++ ) {
                        Node item = items.item(j);

                        String nodeName = item.getNodeName();

                        if( nodeName.equals("item") ) {
                            ArrayList<String> list = new ArrayList<String>();
                            NodeList attributes = item.getChildNodes();
                            String key = null;

                            for( int k = 0; k < attributes.getLength(); k++ ) {
                                Node attribute = attributes.item(k);

                                nodeName = attribute.getNodeName();
                                if( nodeName.equalsIgnoreCase("key") ) {
                                    key = attribute.getFirstChild().getNodeValue().trim();
                                } else if( nodeName.equalsIgnoreCase("valueSet") ) {
                                    NodeList attrItems = attribute.getChildNodes();

                                    for( int l = 0; l < attrItems.getLength(); l++ ) {
                                        Node attrItem = attrItems.item(l);

                                        if( attrItem.getNodeName().equalsIgnoreCase("item") ) {
                                            NodeList values = attrItem.getChildNodes();

                                            for( int m = 0; m < values.getLength(); m++ ) {
                                                Node value = values.item(m);

                                                if( value.getNodeName().equalsIgnoreCase("value") ) {
                                                    list.add(value.getFirstChild().getNodeValue().trim());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            if( key != null && list.size() > 0 ) {
                                if( key.equals("domain-name") ) {
                                    vlan.setDomainName(list.iterator().next());
                                } else if( key.equals("domain-name-servers") ) {
                                    vlan.setDnsServers(list.toArray(new String[list.size()]));
                                } else if( key.equals("ntp-servers") ) {
                                    vlan.setNtpServers(list.toArray(new String[list.size()]));
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        if( action.equals(VLANSupport.ANY) ) {
            return new String[]{EC2Method.EC2_PREFIX + "*"};
        } else if( action.equals(VLANSupport.ASSIGN_ROUTE_TO_SUBNET) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.ASSOCIATE_ROUTE_TABLE};
        } else if( action.equals(VLANSupport.ASSIGN_ROUTE_TO_VLAN) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.REPLACE_ROUTE_TABLE_ASSOCIATION};
        } else if( action.equals(VLANSupport.ATTACH_INTERNET_GATEWAY) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.ATTACH_INTERNET_GATEWAY};
        } else if( action.equals(VLANSupport.CREATE_INTERNET_GATEWAY) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.CREATE_INTERNET_GATEWAY};
        } else if( action.equals(VLANSupport.CREATE_ROUTING_TABLE) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.CREATE_ROUTE_TABLE};
        } else if( action.equals(VLANSupport.ADD_ROUTE) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.CREATE_ROUTE};
        } else if( action.equals(VLANSupport.CREATE_SUBNET) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.CREATE_SUBNET};
        } else if( action.equals(VLANSupport.CREATE_VLAN) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.CREATE_VPC, EC2Method.EC2_PREFIX + EC2Method.CREATE_DHCP_OPTIONS, EC2Method.EC2_PREFIX + EC2Method.ASSOCIATE_DHCP_OPTIONS, EC2Method.EC2_PREFIX + EC2Method.CREATE_INTERNET_GATEWAY, EC2Method.EC2_PREFIX + EC2Method.ATTACH_INTERNET_GATEWAY};
        } else if( action.equals(VLANSupport.GET_SUBNET) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_SUBNETS};
        } else if( action.equals(VLANSupport.GET_VLAN) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_VPCS, EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_DHCP_OPTIONS, EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_INTERNET_GATEWAYS};
        } else if( action.equals(VLANSupport.LIST_SUBNET) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_SUBNETS};
        } else if( action.equals(VLANSupport.LIST_VLAN) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_VPCS, EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_DHCP_OPTIONS, EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_INTERNET_GATEWAYS};
        } else if( action.equals(VLANSupport.REMOVE_INTERNET_GATEWAY) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.DELETE_INTERNET_GATEWAY, EC2Method.EC2_PREFIX + EC2Method.DETACH_INTERNET_GATEWAY};
        } else if( action.equals(VLANSupport.REMOVE_SUBNET) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.DELETE_SUBNET};
        } else if( action.equals(VLANSupport.REMOVE_VLAN) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.DELETE_VPC};
        } else if( action.equals(VLANSupport.CREATE_NIC) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.CREATE_NIC};
        } else if( action.equals(VLANSupport.ATTACH_NIC) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.ATTACH_NIC};
        } else if( action.equals(VLANSupport.DETACH_NIC) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.DETACH_NIC};
        } else if( action.equals(VLANSupport.REMOVE_NIC) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.DELETE_NIC};
        } else if( action.equals(VLANSupport.GET_NIC) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_NICS};
        } else if( action.equals(VLANSupport.LIST_NIC) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_NICS};
        } else if( action.equals(VLANSupport.REMOVE_ROUTE) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.DELETE_ROUTE};
        } else if( action.equals(VLANSupport.REMOVE_ROUTING_TABLE) ) {
            return new String[]{EC2Method.EC2_PREFIX + EC2Method.DELETE_ROUTE_TABLE};
        }
        return new String[0];
    }

    @Override
    public void removeSubnet(String providerSubnetId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.removeSubnet");
        try {
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DELETE_SUBNET);
            EC2Method method;

            parameters.put("SubnetId", providerSubnetId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
        } finally {
            APITrace.end();
        }
    }

    @Override
    public void removeVlan(String providerVpcId) throws CloudException, InternalException {
        APITrace.begin(provider, "VLAN.removeVlan");
        try {
            Map<String, String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DELETE_VPC);
            EC2Method method;

            parameters.put("VpcId", providerVpcId);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
        } finally {
            APITrace.end();
        }
    }

    @Override
    public boolean supportsInternetGatewayCreation() throws CloudException, InternalException {
        return getCapabilities().supportsInternetGatewayCreation();
    }

    @Override
    public boolean supportsRawAddressRouting() throws CloudException, InternalException {
        return getCapabilities().supportsRawAddressRouting();
    }

    public String getDefaultVPCIDForRegion(String regionId) throws CloudException, InternalException{
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_ACCOUNT_ATTRIBUTES);
        parameters.put("AttributeName.1", "supported-platforms");
        EC2Method method = new EC2Method(provider, provider.getEc2Url(provider.getContext().getRegionId()), parameters);
        try{
            Document doc = method.invoke();

            NodeList attributes = doc.getElementsByTagName("attributeValueSet").item(0).getChildNodes();
            for(int i=0;i<attributes.getLength();i++){
                Node attribute = attributes.item(i);
                if(attribute.getNodeType() == Node.TEXT_NODE)continue;

                if(attribute.getNodeName().equals("item")){
                    NodeList data = attribute.getChildNodes();

                    for(int j=0;j<data.getLength();j++){
                        Node value = data.item(j);
                        if(value.getNodeType() == Node.TEXT_NODE)continue;

                        return value.getFirstChild().getNodeValue().trim();
                    }
                }
            }
        }
        catch( EC2Exception e ) {
            logger.error(e.getSummary());
            throw new CloudException(e);
        }

        throw new InternalException("An error occurred finding the default VPC for the region");
    }

    private @Nullable NetworkInterface toNIC(@Nonnull ProviderContext ctx, @Nullable Node item) throws CloudException, InternalException {
        if( item == null ) {
            return null;
        }
        NodeList children = item.getChildNodes();
        NetworkInterface nic = new NetworkInterface();
        String name = null, description = null;

        nic.setProviderOwnerId(ctx.getAccountNumber());
        nic.setProviderRegionId(ctx.getRegionId());
        nic.setCurrentState(NICState.PENDING);

        for( int i = 0; i < children.getLength(); i++ ) {
            Node child = children.item(i);
            String nodeName = child.getNodeName();

            if( nodeName.equalsIgnoreCase("networkInterfaceId") && child.hasChildNodes() ) {
                nic.setProviderNetworkInterfaceId(child.getFirstChild().getNodeValue().trim());
            } else if( nodeName.equalsIgnoreCase("subnetId") && child.hasChildNodes() ) {
                nic.setProviderSubnetId(child.getFirstChild().getNodeValue().trim());
            } else if( nodeName.equalsIgnoreCase("vpcId") && child.hasChildNodes() ) {
                nic.setProviderVlanId(child.getFirstChild().getNodeValue().trim());
            } else if( nodeName.equalsIgnoreCase("availabilityZone") && child.hasChildNodes() ) {
                nic.setProviderDataCenterId(child.getFirstChild().getNodeValue().trim());
            } else if( nodeName.equalsIgnoreCase("description") && child.hasChildNodes() ) {
                nic.setDescription(child.getFirstChild().getNodeValue().trim());
            } else if( nodeName.equalsIgnoreCase("privateIpAddress") && child.hasChildNodes() ) {
                nic.setIpAddresses(new RawAddress(child.getFirstChild().getNodeValue().trim()));
            } else if( nodeName.equalsIgnoreCase("status") && child.hasChildNodes() ) {
                nic.setCurrentState(toNICState(child.getFirstChild().getNodeValue().trim()));
            } else if( nodeName.equalsIgnoreCase("macAddress") && child.hasChildNodes() ) {
                nic.setMacAddress(child.getFirstChild().getNodeValue().trim());
            } else if( nodeName.equalsIgnoreCase("privateDnsName") && child.hasChildNodes() ) {
                nic.setDnsName(child.getFirstChild().getNodeValue().trim());
            } else if( nodeName.equalsIgnoreCase("attachment") && child.hasChildNodes() ) {
                NodeList sublist = child.getChildNodes();

                for( int j = 0; j < sublist.getLength(); j++ ) {
                    Node sub = sublist.item(j);

                    if( sub.getNodeName().equalsIgnoreCase("instanceID") && sub.hasChildNodes() ) {
                        nic.setProviderVirtualMachineId(sub.getFirstChild().getNodeValue().trim());
                    }
                }
            } else if( nodeName.equalsIgnoreCase("tagSet") && child.hasChildNodes() ) {
                provider.setTags(child, nic);
                if( nic.getTags().get("name") != null ) {
                    name = nic.getTags().get("name");
                }
                if( nic.getTags().get("description") != null ) {
                    description = nic.getTags().get("description");
                }
            }
        }
        if( nic.getProviderNetworkInterfaceId() == null ) {
            return null;
        }
        if( nic.getName() == null ) {
            nic.setName(name == null ? nic.getProviderNetworkInterfaceId() : name);
        }
        if( nic.getDescription() == null ) {
            nic.setDescription(description == null ? nic.getName() : description);
        }
        return nic;
    }

    private @Nonnull NICState toNICState(@Nonnull String status) {
        if( status.equalsIgnoreCase("pending") ) {
            return NICState.PENDING;
        } else if( status.equalsIgnoreCase("available") ) {
            return NICState.AVAILABLE;
        } else if( status.equalsIgnoreCase("in-use") ) {
            return NICState.IN_USE;
        } else {
            System.out.println("DEBUG: New AWS network interface status: " + status);
            return NICState.PENDING;
        }
    }

    private @Nullable ResourceStatus toNICStatus(@Nullable Node item) throws CloudException, InternalException {
        if( item == null ) {
            return null;
        }
        NodeList children = item.getChildNodes();
        NICState state = NICState.PENDING;
        String nicId = null;

        for( int i = 0; i < children.getLength(); i++ ) {
            Node child = children.item(i);
            String nodeName = child.getNodeName();

            if( nodeName.equalsIgnoreCase("networkInterfaceId") && child.hasChildNodes() ) {
                nicId = child.getFirstChild().getNodeValue().trim();
            } else if( nodeName.equalsIgnoreCase("status") && child.hasChildNodes() ) {
                state = toNICState(child.getFirstChild().getNodeValue().trim());
            }
        }
        if( nicId == null ) {
            return null;
        }
        return new ResourceStatus(nicId, state);
    }

    private @Nullable RoutingTable toRoutingTable(@Nonnull ProviderContext ctx, @Nullable Node node) throws CloudException, InternalException {
        if( node == null ) {
            return null;
        }
        NodeList children = node.getChildNodes();
        RoutingTable table = new RoutingTable();
        String name = null, description = null;

        table.setProviderOwnerId(ctx.getAccountNumber());
        table.setProviderRegionId(ctx.getRegionId());
        for( int i = 0; i < children.getLength(); i++ ) {
            Node child = children.item(i);
            String nodeName = child.getNodeName();

            if( nodeName.equalsIgnoreCase("routeTableId") && child.hasChildNodes() ) {
                table.setProviderRoutingTableId(child.getFirstChild().getNodeValue().trim());
            } else if( nodeName.equalsIgnoreCase("vpcId") && child.hasChildNodes() ) {
                table.setProviderVlanId(child.getFirstChild().getNodeValue().trim());
            } else if( nodeName.equalsIgnoreCase("routeSet") && child.hasChildNodes() ) {
                ArrayList<Route> routes = new ArrayList<Route>();
                NodeList set = child.getChildNodes();

                for( int j = 0; j < set.getLength(); j++ ) {
                    Node item = set.item(j);

                    if( item.getNodeName().equalsIgnoreCase("item") && item.hasChildNodes() ) {
                        String destination = null, gateway = null, instanceId = null, ownerId = null, nicId = null;
                        NodeList attrs = item.getChildNodes();
                        boolean active = false;

                        for( int k = 0; k < attrs.getLength(); k++ ) {
                            Node attr = attrs.item(k);

                            if( attr.getNodeName().equalsIgnoreCase("destinationCidrBlock") && attr.hasChildNodes() ) {
                                destination = attr.getFirstChild().getNodeValue().trim();
                            } else if( attr.getNodeName().equalsIgnoreCase("gatewayId") && attr.hasChildNodes() ) {
                                gateway = attr.getFirstChild().getNodeValue().trim();
                            } else if( attr.getNodeName().equalsIgnoreCase("instanceId") && attr.hasChildNodes() ) {
                                instanceId = attr.getFirstChild().getNodeValue().trim();
                            } else if( attr.getNodeName().equalsIgnoreCase("instanceOwnerId") && attr.hasChildNodes() ) {
                                ownerId = attr.getFirstChild().getNodeValue().trim();
                            } else if( attr.getNodeName().equalsIgnoreCase("networkInterfaceId") && attr.hasChildNodes() ) {
                                nicId = attr.getFirstChild().getNodeValue().trim();
                            } else if( attr.getNodeName().equalsIgnoreCase("state") && attr.hasChildNodes() ) {
                                active = attr.getFirstChild().getNodeValue().trim().equalsIgnoreCase("active");
                            }
                        }
                        if( active && destination != null ) {
                            if( gateway != null ) {
                                routes.add(Route.getRouteToGateway(IPVersion.IPV4, destination, gateway));
                            }
                            if( instanceId != null && nicId != null ) {
                                routes.add(Route.getRouteToVirtualMachineAndNetworkInterface(IPVersion.IPV4, destination, ownerId, instanceId, nicId));
                            } else {
                                if( nicId != null ) {
                                    routes.add(Route.getRouteToNetworkInterface(IPVersion.IPV4, destination, nicId));
                                } else if( instanceId != null ) {
                                    routes.add(Route.getRouteToVirtualMachine(IPVersion.IPV4, destination, ownerId, instanceId));
                                }
                            }
                        }
                    }
                }
                table.setRoutes(routes.toArray(new Route[routes.size()]));
            } else if( nodeName.equalsIgnoreCase("associationSet") && child.hasChildNodes() ) {
                ArrayList<String> associations = new ArrayList<String>();
                NodeList set = child.getChildNodes();
                for( int j = 0; j < set.getLength(); j++ ) {
                    Node item = set.item(j);
                    if( item.getNodeName().equalsIgnoreCase("item") && item.hasChildNodes() ) {
                        String subnet = null;
                        NodeList attrs = item.getChildNodes();
                        for( int k = 0; k < attrs.getLength(); k++ ) {
                            Node attr = attrs.item(k);
                            if( attr.getNodeName().equalsIgnoreCase("subnetId") && attr.hasChildNodes() ) {
                                subnet = attr.getFirstChild().getNodeValue().trim();
                            }
                        }
                        if( subnet != null ) {
                            associations.add(subnet);
                        }
                    }
                }
                table.setProviderSubnetIds(associations.toArray(new String[associations.size()]));
            } else if( nodeName.equalsIgnoreCase("tagSet") && child.hasChildNodes() ) {
                provider.setTags(child, table);
                if( table.getTags().get("name") != null ) {
                    name = table.getTags().get("name");
                } else if( table.getTags().get("Name") != null ) {
                    name = table.getTags().get("Name");
                }
                if( table.getTags().get("description") != null ) {
                    description = table.getTags().get("description");
                } else if( table.getTags().get("Description") != null ) {
                    description = table.getTags().get("Description");
                }
            }
        }
        if( table.getProviderRoutingTableId() == null ) {
            return null;
        }
        if( table.getName() == null ) {
            table.setName(name == null ? table.getProviderRoutingTableId() : name);
        }
        if( table.getDescription() == null ) {
            table.setDescription(description == null ? table.getName() : description);
        }
        return table;
    }

    private @Nullable Subnet toSubnet(@Nonnull ProviderContext ctx, @Nullable Node item) throws CloudException, InternalException {
        if( item == null ) {
            return null;
        }
        NodeList children = item.getChildNodes();
        Subnet subnet = new Subnet();

        subnet.setProviderOwnerId(ctx.getAccountNumber());
        subnet.setProviderRegionId(ctx.getRegionId());
        subnet.setTags(new HashMap<String, String>());
        for( int i = 0; i < children.getLength(); i++ ) {
            Node child = children.item(i);
            String nodeName = child.getNodeName();

            if( nodeName.equalsIgnoreCase("subnetId") ) {
                subnet.setProviderSubnetId(child.getFirstChild().getNodeValue().trim());
            } else if( nodeName.equalsIgnoreCase("state") ) {
                String value = child.getFirstChild().getNodeValue().trim();
                SubnetState state;

                if( value.equalsIgnoreCase("available") ) {
                    state = SubnetState.AVAILABLE;
                } else if( value.equalsIgnoreCase("pending") ) {
                    state = SubnetState.PENDING;
                } else {
                    logger.warn("Unknown subnet state: " + value);
                    state = null;
                }
                subnet.setCurrentState(state);
            } else if( nodeName.equalsIgnoreCase("vpcId") ) {
                subnet.setProviderVlanId(child.getFirstChild().getNodeValue().trim());
            } else if( nodeName.equalsIgnoreCase("cidrBlock") ) {
                subnet.setCidr(child.getFirstChild().getNodeValue().trim());
            } else if( nodeName.equalsIgnoreCase("availableIpAddressCount") ) {
                subnet.setAvailableIpAddresses(Integer.parseInt(child.getFirstChild().getNodeValue().trim()));
            } else if( nodeName.equalsIgnoreCase("availabilityZone") ) {
                subnet.setProviderDataCenterId(child.getFirstChild().getNodeValue().trim());
            } else if( nodeName.equalsIgnoreCase("tagSet") ) {
                provider.setTags(child, subnet);
            }
        }
        if( subnet.getProviderSubnetId() == null ) {
            return null;
        }
        if( subnet.getName() == null ) {
            String name = subnet.getTags().get("Name");

            subnet.setName(( name == null || name.length() < 1 ) ? subnet.getProviderSubnetId() : name);
        }
        if( subnet.getDescription() == null ) {
            String desc = subnet.getTags().get("Description");

            subnet.setDescription(( desc == null || desc.length() < 1 ) ? subnet.getName() : desc);
        }
        return subnet;
    }

    private @Nullable VLAN toVLAN(@Nonnull ProviderContext ctx, @Nullable Node item) throws CloudException, InternalException {
        if( item == null ) {
            return null;
        }
        NodeList children = item.getChildNodes();
        VLAN vlan = new VLAN();
        String dhcp = null;

        vlan.setProviderOwnerId(ctx.getAccountNumber());
        vlan.setProviderRegionId(ctx.getRegionId());
        vlan.setTags(new HashMap<String, String>());
        vlan.setSupportedTraffic(new IPVersion[]{IPVersion.IPV4});
        for( int i = 0; i < children.getLength(); i++ ) {
            Node child = children.item(i);
            String nodeName = child.getNodeName();

            if( nodeName.equalsIgnoreCase("vpcId") ) {
                vlan.setProviderVlanId(child.getFirstChild().getNodeValue().trim());
            } else if( nodeName.equalsIgnoreCase("state") ) {
                String value = child.getFirstChild().getNodeValue().trim();
                VLANState state;

                if( value.equalsIgnoreCase("available") ) {
                    state = VLANState.AVAILABLE;
                } else if( value.equalsIgnoreCase("pending") ) {
                    state = VLANState.PENDING;
                } else {
                    logger.warn("Unknown VLAN state: " + value);
                    state = null;
                }
                vlan.setCurrentState(state);
            } else if( nodeName.equalsIgnoreCase("cidrBlock") ) {
                vlan.setCidr(child.getFirstChild().getNodeValue().trim());
            } else if( nodeName.equalsIgnoreCase("dhcpOptionsId") ) {
                dhcp = child.getFirstChild().getNodeValue().trim();
            } else if( nodeName.equalsIgnoreCase("tagSet") && child.hasChildNodes() ) {
                provider.setTags(child, vlan);
                if( vlan.getTags().get("name") != null ) {
                    vlan.setName(vlan.getTags().get("name"));
                } else {
                    if( vlan.getTags().get("Name") != null ) {
                        vlan.setName(vlan.getTags().get("Name"));
                    }
                }
                if( vlan.getTags().get("description") != null ) {
                    vlan.setDescription(vlan.getTags().get("description"));
                } else {
                    if( vlan.getTags().get("Description") != null ) {
                        vlan.setDescription(vlan.getTags().get("Description"));
                    }
                }
            }
        }
        if( vlan.getProviderVlanId() == null ) {
            return null;
        }
        if( vlan.getName() == null ) {
            vlan.setName(vlan.getProviderVlanId());
        }
        if( vlan.getDescription() == null ) {
            vlan.setDescription(vlan.getName());
        }
        if( dhcp != null ) {
            loadDHCPOptions(dhcp, vlan);
        }
        return vlan;
    }

    private @Nonnull VLANState toVLANState(@Nonnull String status) {
        if( status.equalsIgnoreCase("available") ) {
            return VLANState.AVAILABLE;
        } else if( status.equalsIgnoreCase("pending") ) {
            return VLANState.PENDING;
        } else {
            logger.warn("DEBUG: Unknown AWS VLAN state: " + status);
            return VLANState.PENDING;
        }
    }

    private @Nullable ResourceStatus toVLANStatus(@Nullable Node item) throws CloudException, InternalException {
        if( item == null ) {
            return null;
        }
        NodeList children = item.getChildNodes();
        VLANState state = VLANState.PENDING;
        String vlanId = null;

        for( int i = 0; i < children.getLength(); i++ ) {
            Node child = children.item(i);
            String nodeName = child.getNodeName();

            if( nodeName.equalsIgnoreCase("vpcId") ) {
                vlanId = child.getFirstChild().getNodeValue().trim();
            } else if( nodeName.equalsIgnoreCase("state") ) {
                state = toVLANState(child.getFirstChild().getNodeValue().trim());

            }
        }
        if( vlanId == null ) {
            return null;
        }
        return new ResourceStatus(vlanId, state);
    }

    private @Nullable InternetGateway toInternetGateway(@Nonnull ProviderContext ctx, @Nullable Node item) throws CloudException, InternalException {
        if( item == null ) {
            return null;
        }
        InternetGateway ig = new InternetGateway();

        ig.setProviderOwnerId(ctx.getAccountNumber());
        ig.setProviderRegionId(ctx.getRegionId());

        if( item.getNodeName().equalsIgnoreCase("item") && item.hasChildNodes() ) {
            NodeList childNodes = item.getChildNodes();

            for( int j = 0; j < childNodes.getLength(); j++ ) {
                Node child = childNodes.item(j);

                if( child.getNodeName().equalsIgnoreCase("internetGatewayId") && child.hasChildNodes() ) {

                    ig.setProviderInternetGatewayId(child.getFirstChild().getNodeValue().trim());

                } else if( child.getNodeName().equalsIgnoreCase("attachmentSet") && child.hasChildNodes() ) {
                    NodeList attachmentChildren = child.getChildNodes();

                    for( int x1 = 0; x1 < attachmentChildren.getLength(); x1++ ) {
                        Node attachmentItem = attachmentChildren.item(x1);

                        if( attachmentItem.getNodeName().equalsIgnoreCase("item") && item.hasChildNodes() ) {
                            NodeList attachmentChildNodes = attachmentItem.getChildNodes();

                            for( int y1 = 0; y1 < attachmentChildNodes.getLength(); y1++ ) {
                                Node attachmentChild = attachmentChildNodes.item(y1);

                                if( attachmentChild.getNodeName().equalsIgnoreCase("vpcId") && child.hasChildNodes() ) {

                                    ig.setProviderVlanId(attachmentChild.getFirstChild().getNodeValue().trim());

                                } else if( attachmentChild.getNodeName().equalsIgnoreCase("state") && attachmentChild.hasChildNodes() ) {
                                    String value = attachmentChild.getFirstChild().getNodeValue().trim();
                                    InternetGatewayAttachmentState state;

                                    if( value.equalsIgnoreCase("available") ) {
                                        state = InternetGatewayAttachmentState.AVAILABLE;
                                    } else if( value.equalsIgnoreCase("attaching") ) {
                                        state = InternetGatewayAttachmentState.ATTACHED;
                                    } else if( value.equalsIgnoreCase("attached") ) {
                                        state = InternetGatewayAttachmentState.ATTACHED;
                                    } else if( value.equalsIgnoreCase("detaching") ) {
                                        state = InternetGatewayAttachmentState.DETACHING;
                                    } else if( value.equalsIgnoreCase("detached") ) {
                                        state = InternetGatewayAttachmentState.DETACHED;
                                    } else {
                                        logger.warn("Unknown Internet Gateway state: " + value);
                                        state = null;
                                    }
                                    ig.setAttachmentState(state);
                                }
                            }
                        }
                    }
                }
            }
        }
        if( ig.getProviderInternetGatewayId() == null ) {
            return null;
        }
        return ig;
    }

    @Override
    public void updateSubnetTags(@Nonnull String subnetId, @Nonnull Tag... tags) throws CloudException, InternalException {
        ( (AWSCloud) getProvider() ).createTags(subnetId, tags);
    }

    @Override
    public void updateSubnetTags(@Nonnull String[] subnetIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        ( (AWSCloud) getProvider() ).createTags(subnetIds, tags);
    }

    @Override
    public void updateVLANTags(@Nonnull String vlanId, @Nonnull Tag... tags) throws CloudException, InternalException {
        ( (AWSCloud) getProvider() ).createTags(vlanId, tags);
    }

    @Override
    public void updateVLANTags(@Nonnull String[] vlanIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        ( (AWSCloud) getProvider() ).createTags(vlanIds, tags);
    }

    @Override
    public void removeSubnetTags(@Nonnull String subnetId, @Nonnull Tag... tags) throws CloudException, InternalException {
        ( (AWSCloud) getProvider() ).removeTags(subnetId, tags);
    }

    @Override
    public void removeSubnetTags(@Nonnull String[] subnetIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        ( (AWSCloud) getProvider() ).removeTags(subnetIds, tags);
    }

    @Override
    public void removeVLANTags(@Nonnull String vlanId, @Nonnull Tag... tags) throws CloudException, InternalException {
        ( (AWSCloud) getProvider() ).removeTags(vlanId, tags);
    }

    @Override
    public void removeVLANTags(@Nonnull String[] vlanIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        ( (AWSCloud) getProvider() ).removeTags(vlanIds, tags);
    }
}
