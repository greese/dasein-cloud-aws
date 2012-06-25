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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.aws.compute.EC2Method;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.NICCreateOptions;
import org.dasein.cloud.network.NICState;
import org.dasein.cloud.network.NetworkInterface;
import org.dasein.cloud.network.Subnet;
import org.dasein.cloud.network.SubnetState;
import org.dasein.cloud.network.VLAN;
import org.dasein.cloud.network.VLANState;
import org.dasein.cloud.network.VLANSupport;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class VPC implements VLANSupport {
    static private final Logger logger = Logger.getLogger(VPC.class);
    
    private AWSCloud provider;
    
    VPC(AWSCloud provider) { this.provider = provider; }
    
    @Override
    public boolean allowsNewSubnetCreation() throws CloudException, InternalException {
        return true;
    }

    @Override
    public void attachNetworkInterface(@Nonnull String nicId, @Nonnull String vmId, int index) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull NetworkInterface createNetworkInterface(@Nonnull NICCreateOptions options) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was configured");
        }
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.CREATE_NIC);
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
        }
        catch( EC2Exception e ) {
            logger.error(e.getSummary());
            e.printStackTrace();
            throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("networkInterface");

        for( int i=0; i<blocks.getLength(); i++ ) {
            Node item = blocks.item(i);
            NetworkInterface nic = toNIC(ctx, item);

            if( nic != null ) {
                return nic;
            }
        }
        throw new CloudException("No network interface was created, but no error was reported");
    }

    @Override
    public boolean allowsNewNetworkInterfaceCreation() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean allowsNewVlanCreation() throws CloudException, InternalException {
        return true;
    }
    
    private void assignDHCPOptions(VLAN vlan, String domainName, String[] dnsServers, String[] ntpServers) throws CloudException, InternalException {
        boolean differs = false;
        
        if( vlan.getDomainName() != null ) {
            if( domainName == null ) {
                differs = true;
            }
            else if( !domainName.equals(vlan.getDomainName()) ) {
                differs = true;
            }
        }
        else if( domainName != null ) {
            differs = true;
        }
        if( !differs ) {
            if( vlan.getDnsServers() != null ) {
                if( dnsServers == null || vlan.getDnsServers().length != dnsServers.length ) {
                    differs = true;
                }
                else {
                    for( int i=0; i<dnsServers.length; i++ ) {
                        if( !dnsServers[i].equalsIgnoreCase(vlan.getDnsServers()[i]) ) {
                            differs = true;
                            break;
                        }
                    }
                }
            }
            else if( dnsServers != null ) {
                if( vlan.getDnsServers().length != dnsServers.length ) {
                    differs = true;
                }
                else {
                    for( int i=0; i<dnsServers.length; i++ ) {
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
                    }
                    else {
                        for( int i=0; i<ntpServers.length; i++ ) {
                            if( !ntpServers[i].equalsIgnoreCase(vlan.getNtpServers()[i]) ) {
                                differs = true;
                                break;
                            }
                        }
                    }
                }
                else if( ntpServers != null ) {
                    if( vlan.getNtpServers().length != ntpServers.length ) {
                        differs = true;
                    }
                    else {
                        for( int i=0; i<ntpServers.length; i++ ) {
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
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.ASSOCIATE_DHCP_OPTIONS);
        EC2Method method;
        
        parameters.put("DhcpOptionsId", dhcp);
        parameters.put("VpcId", vlanId);
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            method.invoke();
        }
        catch( EC2Exception e ) {
            logger.error(e.getSummary());
            if( logger.isDebugEnabled() ) {
                e.printStackTrace();
            }
            throw new CloudException(e);
        }                  
    }

    private String createDhcp(String domainName, String[] dnsServers, String[] ntpServers) throws CloudException, InternalException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.CREATE_DHCP_OPTIONS);
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
        }
        catch( EC2Exception e ) {
            logger.error(e.getSummary());
            if( logger.isDebugEnabled() ) {
                e.printStackTrace();
            }
            throw new CloudException(e);
        }           
        blocks = doc.getElementsByTagName("dhcpOptionsId");        
        for( int i=0; i<blocks.getLength(); i++ ) {
            Node id = blocks.item(i);
            
            if( id != null ) {
                return id.getFirstChild().getNodeValue().trim();
            }
        }
        throw new CloudException("No DHCP options were created, but no error was reported");        
    }

    @Override
    public @Nonnull Subnet createSubnet(@Nonnull String cidr, @Nonnull String inProviderVlanId, @Nonnull String name, @Nonnull String description) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();
        
        if( ctx == null ) {
            throw new CloudException("No context was configured");
        }
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.CREATE_SUBNET);
        EC2Method method;
        NodeList blocks;
        Document doc;
        
        parameters.put("CidrBlock", cidr);
        parameters.put("VpcId", inProviderVlanId);
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            doc = method.invoke();
        }
        catch( EC2Exception e ) {
            logger.error(e.getSummary());
            if( logger.isDebugEnabled() ) {
                e.printStackTrace();
            }
            throw new CloudException(e);
        }           
        blocks = doc.getElementsByTagName("subnet");

        for( int i=0; i<blocks.getLength(); i++ ) {
            Node item = blocks.item(i);
            Subnet subnet = toSubnet(ctx, item);
            
            if( subnet != null ) {
                return subnet;
            }
        }
        throw new CloudException("No subnet was created, but no error was reported");
    }

    @Override
    public VLAN createVlan(String cidr, String name, String description, String domainName, String[] dnsServers, String[] ntpServers) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was configured");
        }
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.CREATE_VPC);
        EC2Method method;
        NodeList blocks;
        Document doc;
        
        parameters.put("CidrBlock", cidr);
        parameters.put("InstanceTenancy", "default");
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            doc = method.invoke();
        }
        catch( EC2Exception e ) {
            logger.error(e.getSummary());
            if( logger.isDebugEnabled() ) {
                e.printStackTrace();
            }
            throw new CloudException(e);
        }           
        blocks = doc.getElementsByTagName("vpc");
        
        for( int i=0; i<blocks.getLength(); i++ ) {
            Node item = blocks.item(i);
            VLAN vlan = toVLAN(ctx, item);
            
            if( vlan != null ) {
                if( domainName != null || (dnsServers != null && dnsServers.length > 0) || (ntpServers != null && ntpServers.length > 0) ) {
                    assignDHCPOptions(vlan, domainName, dnsServers, ntpServers);
                }
                return vlan;
            }
        }
        throw new CloudException("No VLAN was created, but no error was reported");
    }

    @Override
    public void detachNetworkInterface(@Nonnull String nicId) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getMaxNetworkInterfaceCount() throws CloudException, InternalException {
        return -2;
    }

    @Override
    public int getMaxVlanCount() throws CloudException, InternalException {
        return 1;
    }

    @Override
    public String getProviderTermForNetworkInterface(Locale locale) {
        return "network interface";
    }

    @Override
    public String getProviderTermForSubnet(Locale locale) {
        return "subnet";
    }

    @Override
    public String getProviderTermForVlan(Locale locale) {
        return "VPC";
    }

    @Override
    public NetworkInterface getNetworkInterface(@Nonnull String nicId) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override 
    public @Nullable Subnet getSubnet(@Nonnull String providerSubnetId) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was configured");
        }
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_SUBNETS);
        EC2Method method;
        NodeList blocks;
        Document doc;
        
        parameters.put("SubnetId.1", providerSubnetId);
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            doc = method.invoke();
        }
        catch( EC2Exception e ) {
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
        
        for( int i=0; i<blocks.getLength(); i++ ) {
            Node item = blocks.item(i);
            Subnet subnet = toSubnet(ctx, item);
            
            if( subnet != null ) {
                return subnet;
            }
        }
        return null;
    }
    
    @Override
    public @Nullable VLAN getVlan(@Nonnull String providerVlanId) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was configured");
        }
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_VPCS);
        EC2Method method;
        NodeList blocks;
        Document doc;
        
        parameters.put("VpcId.1", providerVlanId);
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            doc = method.invoke();
        }
        catch( EC2Exception e ) {
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
        
        for( int i=0; i<blocks.getLength(); i++ ) {
            Node item = blocks.item(i);
            VLAN vlan = toVLAN(ctx, item);
            
            if( vlan != null ) {
                return vlan;
            }
        }
        return null;
    }

    @Override
    public boolean isNetworkInterfaceSupportEnabled() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_VPCS);
        EC2Method method;
        
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            method.invoke();
            return true;
        }
        catch( EC2Exception e ) {
            if( e.getStatus() == HttpServletResponse.SC_UNAUTHORIZED || e.getStatus() == HttpServletResponse.SC_FORBIDDEN ) {
                return false;
            }
            String code = e.getCode();
            
            if( code != null && (code.equals("SubscriptionCheckFailed") || code.equals("AuthFailure") || code.equals("SignatureDoesNotMatch") || code.equals("UnsupportedOperation") || code.equals("InvalidClientTokenId") || code.equals("OptInRequired")) ) {
                return false;
            }
            logger.error(e.getSummary());
            if( logger.isDebugEnabled() ) {
                e.printStackTrace();
            }
            throw new CloudException(e);
        }  
    }

    @Override
    public boolean isVlanDataCenterConstrained() throws CloudException, InternalException {
        return false;
    }

    @Nonnull
    @Override
    public Iterable<NetworkInterface> listNetworkInterfaces() throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Nonnull
    @Override
    public Iterable<NetworkInterface> listNetworkInterfacesForVM(@Nonnull String forVmId) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }


    @Override
    public boolean isSubnetDataCenterConstrained() throws CloudException, InternalException {
        return true;
    }

    @Override
    public @Nonnull Iterable<Subnet> listSubnets(@Nonnull String providerVlanId) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was configured");
        }
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_SUBNETS);
        EC2Method method;
        NodeList blocks;
        Document doc;
        
        parameters.put("Filter.1.Name", "vpc-id");
        parameters.put("Filter.1.Value.1", providerVlanId);
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            doc = method.invoke();
        }
        catch( EC2Exception e ) {
            logger.error(e.getSummary());
            if( logger.isDebugEnabled() ) {
                e.printStackTrace();
            }
            throw new CloudException(e);
        }           
        blocks = doc.getElementsByTagName("item");
        
        ArrayList<Subnet> list = new ArrayList<Subnet>();
        
        for( int i=0; i<blocks.getLength(); i++ ) {
            Node item = blocks.item(i);
            Subnet subnet = toSubnet(ctx, item);
            
            if( subnet != null ) {
                list.add(subnet);
            }
        }
        return list;
    }

    @Override
    public @Nonnull Iterable<VLAN> listVlans() throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was configured");
        }
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_VPCS);
        EC2Method method;
        NodeList blocks;
        Document doc;
        
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            doc = method.invoke();
        }
        catch( EC2Exception e ) {
            logger.error(e.getSummary());
            if( logger.isDebugEnabled() ) {
                e.printStackTrace();
            }
            throw new CloudException(e);
        }           
        blocks = doc.getElementsByTagName("item");
        
        ArrayList<VLAN> list = new ArrayList<VLAN>();
        
        for( int i=0; i<blocks.getLength(); i++ ) {
            Node item = blocks.item(i);
            VLAN vlan = toVLAN(ctx, item);
            
            if( vlan != null ) {
                list.add(vlan);
            }
        }
        return list;
    }

    @Override
    public void removeNetworkInterface(@Nonnull String nicId) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    private void loadDHCPOptions(String dhcpOptionsId, VLAN vlan) throws CloudException, InternalException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DESCRIBE_DHCP_OPTIONS);
        EC2Method method;
        NodeList blocks;
        Document doc;
        
        parameters.put("DhcpOptionsId.1", dhcpOptionsId);
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            doc = method.invoke();
        }
        catch( EC2Exception e ) {
            logger.error(e.getSummary());
            if( logger.isDebugEnabled() ) {
                e.printStackTrace();
            }
            throw new CloudException(e);
        }           
        blocks = doc.getElementsByTagName("dhcpConfigurationSet");
        
        for( int i=0; i<blocks.getLength(); i++ ) {
            Node config = blocks.item(i);

            if( config.hasChildNodes() ) {
                NodeList items = config.getChildNodes();
                
                for( int j=0; j < items.getLength(); j++ ) {
                    Node item = items.item(j);
                    
                    String nodeName = item.getNodeName();
                    
                    if( nodeName.equals("item") ) {
                        ArrayList<String> list = new ArrayList<String>();
                        NodeList attributes = item.getChildNodes();
                        String key = null;
                        
                        for( int k=0; k<attributes.getLength(); k++ ) {
                            Node attribute = attributes.item(k);
                            
                            nodeName = attribute.getNodeName();
                            if( nodeName.equalsIgnoreCase("key") ) {
                                key = attribute.getFirstChild().getNodeValue().trim();
                            }
                            else if( nodeName.equalsIgnoreCase("valueSet") ) {
                                NodeList attrItems = attribute.getChildNodes();
                                
                                for( int l=0; l<attrItems.getLength(); l++ ) {
                                    Node attrItem = attrItems.item(l);
                                    
                                    if( attrItem.getNodeName().equalsIgnoreCase("item") ) {
                                        NodeList values = attrItem.getChildNodes();
                                        
                                        for( int m=0; m<values.getLength(); m++ ) {
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
                            }
                            else if( key.equals("domain-name-servers") ) {
                                vlan.setDnsServers(list.toArray(new String[list.size()]));
                            }
                            else if( key.equals("ntp-servers") ) {
                                vlan.setNtpServers(list.toArray(new String[list.size()]));
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        if( action.equals(VLANSupport.ANY) ) {
            return new String[] { EC2Method.EC2_PREFIX + "*" };
        }
        else if( action.equals(VLANSupport.CREATE_SUBNET) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.CREATE_SUBNET };
        }
        else if( action.equals(VLANSupport.CREATE_VLAN) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.CREATE_VPC, EC2Method.EC2_PREFIX + EC2Method.CREATE_DHCP_OPTIONS, EC2Method.EC2_PREFIX + EC2Method.ASSOCIATE_DHCP_OPTIONS };
        }
        else if( action.equals(VLANSupport.GET_SUBNET) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_SUBNETS };
        }
        else if( action.equals(VLANSupport.GET_VLAN) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_VPCS, EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_DHCP_OPTIONS };
        }
        else if( action.equals(VLANSupport.LIST_SUBNET) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_SUBNETS };
        }
        else if( action.equals(VLANSupport.LIST_VLAN) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_VPCS, EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_DHCP_OPTIONS };
        }
        else if( action.equals(VLANSupport.REMOVE_SUBNET) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DELETE_SUBNET };
        }
        else if( action.equals(VLANSupport.REMOVE_VLAN) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DELETE_VPC };
        }
        else if( action.equals(VLANSupport.CREATE_NIC) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.CREATE_NIC };
        }
        return new String[0];    
    }

    @Override
    public void removeSubnet(String providerSubnetId) throws CloudException, InternalException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DELETE_SUBNET);
        EC2Method method;
        
        parameters.put("SubnetId", providerSubnetId);
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            method.invoke();
        }
        catch( EC2Exception e ) {
            logger.error(e.getSummary());
            if( logger.isDebugEnabled() ) {
                e.printStackTrace();
            }
            throw new CloudException(e);
        }  
    }

    @Override
    public void removeVlan(String providerVpcId) throws CloudException, InternalException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), ELBMethod.DELETE_VPC);
        EC2Method method;
        
        parameters.put("VpcId", providerVpcId);
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            method.invoke();
        }
        catch( EC2Exception e ) {
            logger.error(e.getSummary());
            if( logger.isDebugEnabled() ) {
                e.printStackTrace();
            }
            throw new CloudException(e);
        }  
    }
    
    private @Nullable NetworkInterface toNIC(@Nonnull ProviderContext ctx, @Nullable Node item) throws CloudException, InternalException {
        if( item == null ) {
            return null;
        }
        NodeList children = item.getChildNodes();
        NetworkInterface nic = new NetworkInterface();

        nic.setProviderOwnerId(ctx.getAccountNumber());
        nic.setProviderRegionId(ctx.getRegionId());
        nic.setCurrentState(NICState.PENDING);

        for( int i=0; i<children.getLength(); i++ ) {
            Node child = children.item(i);
            String nodeName = child.getNodeName();

            if( nodeName.equalsIgnoreCase("networkInterfaceId") && child.hasChildNodes() ) {
                nic.setProviderNetworkInterfaceId(child.getFirstChild().getNodeValue().trim());
            }
            else if( nodeName.equalsIgnoreCase("subnetId") && child.hasChildNodes() ) {
                nic.setProviderSubnetId(child.getFirstChild().getNodeValue().trim());
            }
            else if( nodeName.equalsIgnoreCase("vpcId") && child.hasChildNodes() ) {
                nic.setProviderVlanId(child.getFirstChild().getNodeValue().trim());
            }
            else if( nodeName.equalsIgnoreCase("availabilityZone") && child.hasChildNodes() ) {
                nic.setProviderDataCenterId(child.getFirstChild().getNodeValue().trim());
            }
            else if( nodeName.equalsIgnoreCase("description") && child.hasChildNodes() ) {
                nic.setDescription(child.getFirstChild().getNodeValue().trim());
            }
            else if( nodeName.equalsIgnoreCase("privateIpAddress") && child.hasChildNodes() ) {
                nic.setIpAddress(child.getFirstChild().getNodeValue().trim());
            }
            else if( nodeName.equalsIgnoreCase("status") && child.hasChildNodes() ) {
                String status = child.getFirstChild().getNodeValue().trim();

                if( status.equalsIgnoreCase("pending") ) {
                    nic.setCurrentState(NICState.PENDING);
                }
                else if( status.equalsIgnoreCase("available") ) {
                    nic.setCurrentState(NICState.AVAILABLE);
                }
                else if( status.equalsIgnoreCase("in-use") ) {
                    nic.setCurrentState(NICState.IN_USE);
                }
                else {
                    System.out.println("DEBUG: New AWS network interface status: " + status);
                }
            }
            else if( nodeName.equalsIgnoreCase("macAddress") && child.hasChildNodes() ) {
                nic.setMacAddress(child.getFirstChild().getNodeValue().trim());
            }
            else if( nodeName.equalsIgnoreCase("privateDnsName") && child.hasChildNodes() ) {
                nic.setDnsName(child.getFirstChild().getNodeValue().trim());
            }
            else if( nodeName.equalsIgnoreCase("attachment") && child.hasChildNodes() ) {
                NodeList sublist = child.getChildNodes();
                
                for( int j=0; j<sublist.getLength(); j++ ) {
                    Node sub = sublist.item(j);
                    
                    if( sub.getNodeName().equalsIgnoreCase("instanceID") && sub.hasChildNodes() ) {
                        nic.setProviderVirtualMachineId(sub.getFirstChild().getNodeValue().trim());
                    }
                }
            }
            else if( nodeName.equalsIgnoreCase("tagSet") && child.hasChildNodes() ) {
                // TODO: tags
            }
            else if( nodeName.equalsIgnoreCase("groupSet") && child.hasChildNodes() ) {
                // TODO: firewalls
            }
        }
        if( nic.getName() == null ) {
            nic.setName(nic.getProviderNetworkInterfaceId());
        }
        if( nic.getDescription() == null ) {
            nic.setDescription(nic.getName());
        }
        return nic;
    }

    private @Nullable Subnet toSubnet(@Nonnull ProviderContext ctx, @Nullable Node item) throws CloudException, InternalException {
        if( item == null ) {
            return null;
        }
        NodeList children = item.getChildNodes();
        Subnet subnet = new Subnet();
        
        subnet.setProviderOwnerId(ctx.getAccountNumber());
        subnet.setProviderRegionId(ctx.getRegionId());
        subnet.setTags(new HashMap<String,String>());
        for( int i=0; i<children.getLength(); i++ ) {
            Node child = children.item(i);
            String nodeName = child.getNodeName();
            
            if( nodeName.equalsIgnoreCase("subnetId") ) {
                subnet.setProviderSubnetId(child.getFirstChild().getNodeValue().trim());
            }
            else if( nodeName.equalsIgnoreCase("state") ) {
                String value = child.getFirstChild().getNodeValue().trim();
                SubnetState state;
                
                if( value.equalsIgnoreCase("available") ) {
                    state = SubnetState.AVAILABLE;
                }
                else if( value.equalsIgnoreCase("pending") ) {
                    state = SubnetState.PENDING;                    
                }
                else {
                    logger.warn("Unknown subnet state: " + value);
                    state = null;
                }
                subnet.setCurrentState(state);
            }
            else if( nodeName.equalsIgnoreCase("vpcId") ) {
                subnet.setProviderVlanId(child.getFirstChild().getNodeValue().trim());
            }
            else if( nodeName.equalsIgnoreCase("cidrBlock") ) {
                subnet.setCidr(child.getFirstChild().getNodeValue().trim());
            }
            else if( nodeName.equalsIgnoreCase("availableIpAddressCount") ) {
                subnet.setAvailableIpAddresses(Integer.parseInt(child.getFirstChild().getNodeValue().trim()));
            }
            else if( nodeName.equalsIgnoreCase("availabilityZone") ) {
                subnet.setProviderDataCenterId(child.getFirstChild().getNodeValue().trim());
            }
        }
        if( subnet.getProviderSubnetId() == null ) {
            return null;
        }
        if( subnet.getName() == null ) {
            subnet.setName(subnet.getProviderSubnetId());
        }
        if( subnet.getDescription() == null ) {
            subnet.setDescription(subnet.getName());
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
        vlan.setTags(new HashMap<String,String>());
        for( int i=0; i<children.getLength(); i++ ) {
            Node child = children.item(i);
            String nodeName = child.getNodeName();
            
            if( nodeName.equalsIgnoreCase("vpcId") ) {
                vlan.setProviderVlanId(child.getFirstChild().getNodeValue().trim());
            }
            else if( nodeName.equalsIgnoreCase("state") ) {
                String value = child.getFirstChild().getNodeValue().trim();
                VLANState state;
                
                if( value.equalsIgnoreCase("available") ) {
                    state = VLANState.AVAILABLE;
                }
                else if( value.equalsIgnoreCase("pending") ) {
                    state = VLANState.PENDING;                    
                }
                else {
                    logger.warn("Unknown VLAN state: " + value);
                    state = null;
                }
                vlan.setCurrentState(state);
            }
            else if( nodeName.equalsIgnoreCase("cidrBlock") ) {
                vlan.setCidr(child.getFirstChild().getNodeValue().trim());
            }
            else if( nodeName.equalsIgnoreCase("dhcpOptionsId") ) {
                dhcp = child.getFirstChild().getNodeValue().trim();
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
        vlan.setGateway(""); // TODO: fix me
        if( dhcp != null ) {
            loadDHCPOptions(dhcp, vlan);
        }
        return vlan;
    }

    @Override
    public boolean supportsVlansWithSubnets() {
        return true;
    }
}
