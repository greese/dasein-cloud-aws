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

package org.dasein.cloud.aws.network;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.Tag;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.aws.compute.EC2Method;
import org.dasein.cloud.compute.ComputeServices;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.AbstractFirewallSupport;
import org.dasein.cloud.network.FirewallCreateOptions;
import org.dasein.cloud.network.RuleTargetType;
import org.dasein.cloud.network.Direction;
import org.dasein.cloud.network.Firewall;
import org.dasein.cloud.network.FirewallRule;
import org.dasein.cloud.network.FirewallSupport;
import org.dasein.cloud.network.Permission;
import org.dasein.cloud.network.Protocol;
import org.dasein.cloud.network.RuleTarget;
import org.dasein.cloud.util.APITrace;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SecurityGroup extends AbstractFirewallSupport {
	static private final Logger logger = AWSCloud.getLogger(SecurityGroup.class);

	private AWSCloud provider = null;
	
	SecurityGroup(AWSCloud provider) {
        super(provider);
		this.provider = provider;
	}

    @Override
    public @Nonnull String authorize(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull Permission permission, @Nonnull RuleTarget sourceEndpoint, @Nonnull Protocol protocol, @Nonnull RuleTarget destinationEndpoint, int beginPort, int endPort, @Nonnegative int precedence) throws CloudException, InternalException {
        APITrace.begin(provider, "Firewall.authorize");
        try {
            if( Permission.DENY.equals(permission) ) {
                throw new OperationNotSupportedException("AWS does not support DENY rules");
            }
            //if( !destinationEndpoint.getRuleTargetType().equals(RuleTargetType.GLOBAL) ) {
            //    throw new OperationNotSupportedException("AWS does not support discreet routing of rules");
            //}
            Firewall fw = getFirewall(firewallId);

            if( fw == null ) {
                throw new CloudException("No such firewall: " + firewallId);
            }
            if( direction.equals(Direction.EGRESS) && fw.getProviderVlanId() == null ) {
                throw new OperationNotSupportedException("AWS does not support EGRESS rules for non-VPC security groups");
            }
            String action = (direction.equals(Direction.INGRESS) ? EC2Method.AUTHORIZE_SECURITY_GROUP_INGRESS : EC2Method.AUTHORIZE_SECURITY_GROUP_EGRESS);
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), action);
            String targetGroupId = null;
            boolean group;
            EC2Method method;
            NodeList blocks;
            Document doc;

            if( direction.equals(Direction.INGRESS) ) {
                group = sourceEndpoint.getRuleTargetType().equals(RuleTargetType.GLOBAL);
                if( group ) {
                    targetGroupId = sourceEndpoint.getProviderFirewallId();
                }
            }
            else {
                group = destinationEndpoint.getRuleTargetType().equals(RuleTargetType.GLOBAL);
                if( group ) {
                    targetGroupId = destinationEndpoint.getProviderFirewallId();
                }
            }
            if( provider.getEC2Provider().isEucalyptus() ) {
                parameters.put("GroupName", firewallId);
                parameters.put("IpProtocol", protocol.name().toLowerCase());
                parameters.put("FromPort", String.valueOf(beginPort));
                parameters.put("ToPort", endPort == -1 ? String.valueOf(beginPort) : String.valueOf(endPort));
                if( group ) {
                    parameters.put("GroupName", targetGroupId);
                }
                else {
                    parameters.put("CidrIp", sourceEndpoint.getCidr());
                }
            }
            else {
                parameters.put("GroupId", firewallId);
                parameters.put("IpPermissions.1.IpProtocol", protocol.name().toLowerCase());
                parameters.put("IpPermissions.1.FromPort", String.valueOf(beginPort));
                parameters.put("IpPermissions.1.ToPort", endPort == -1 ? String.valueOf(beginPort) : String.valueOf(endPort));
                if( group ) {
                    if( targetGroupId.startsWith("sg-") ) {
                        parameters.put("IpPermissions.1.Groups.1.GroupId", targetGroupId);
                    }
                    else {
                        parameters.put("IpPermissions.1.Groups.1.GroupName", targetGroupId);
                    }
                }
                else if( direction.equals(Direction.INGRESS) ) {
                    parameters.put("IpPermissions.1.IpRanges.1.CidrIp", sourceEndpoint.getCidr());
                }
                else {
                    parameters.put("IpPermissions.1.IpRanges.1.CidrIp", destinationEndpoint.getCidr());
                }
            }
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && code.equals("InvalidPermission.Duplicate") ) {
                    if( direction.equals(Direction.INGRESS) ) {
                        //return FirewallRule.getInstance(null, firewallId, source, direction, protocol, Permission.ALLOW, RuleTarget.getGlobal(firewallId), beginPort, endPort).getProviderRuleId();
                    }
                    else {

                    }
                    // TODO: fix me
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("return");
            if( blocks.getLength() > 0 ) {
                if( !blocks.item(0).getFirstChild().getNodeValue().equalsIgnoreCase("true") ) {
                    throw new CloudException("Failed to authorize security group rule without explanation.");
                }
            }
            return FirewallRule.getInstance(null, firewallId, sourceEndpoint, direction, protocol, permission, destinationEndpoint, beginPort, endPort).getProviderRuleId();
        }
        finally {
            APITrace.end();
        }
    }

    @Override
	public @Nonnull String create(@Nonnull FirewallCreateOptions options) throws InternalException, CloudException {
        APITrace.begin(provider, "Firewall.create");
        try {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.CREATE_SECURITY_GROUP);
            EC2Method method;
            NodeList blocks;
            Document doc;

            String name = getUniqueName(options.getName());
            parameters.put("GroupName", name);
            parameters.put("GroupDescription", options.getDescription());
            String vlanId = options.getProviderVlanId();

            if( vlanId != null ) {
                parameters.put("VpcId", vlanId);
            }
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            if( provider.getEC2Provider().isEucalyptus() ) {
                return name;
            }
            else {
                blocks = doc.getElementsByTagName("groupId");
                if( blocks.getLength() > 0 ) {
                    Map<String,String> metaData = options.getMetaData();

                    String id = blocks.item(0).getFirstChild().getNodeValue().trim();

                    if( !metaData.isEmpty() ) {
                        ArrayList<Tag> tags = new ArrayList<Tag>();

                        for( Map.Entry<String,String> entry : metaData.entrySet() ) {
                            String key = entry.getKey();
                            String value = entry.getValue();

                            if( value != null ) {
                                tags.add(new Tag(key, value));
                            }
                        }
                        provider.createTags(id, tags.toArray(new Tag[tags.size()]));
                    }
                    return id;
                }
                throw new CloudException("Failed to create security group without explanation.");
            }
        }
        finally {
            APITrace.end();
        }
	}

	@Override
	public void delete(@Nonnull String securityGroupId) throws InternalException, CloudException {
        APITrace.begin(provider, "Firewall.delete");
        try {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DELETE_SECURITY_GROUP);
            EC2Method method;
            NodeList blocks;
            Document doc;

            if( provider.getEC2Provider().isEucalyptus() ) {
                parameters.put("GroupName", securityGroupId);
            }
            else {
                parameters.put("GroupId", securityGroupId);
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
                    throw new CloudException("Failed to delete security group without explanation.");
                }
            }
        }
        finally {
            APITrace.end();
        }
	}

	@Override
	public @Nullable Firewall getFirewall(@Nonnull String securityGroupId) throws InternalException, CloudException {
        APITrace.begin(provider, "Firewall.getFirewall");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context has been established for this request");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_SECURITY_GROUPS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            if( provider.getEC2Provider().isEucalyptus() ) {
                parameters.put("GroupName.1", securityGroupId);
            }
            else {
                parameters.put("GroupId.1", securityGroupId);
            }
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && code.startsWith("InvalidGroup") ) {
                    return null;
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("securityGroupInfo");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("item") ) {
                        Firewall firewall = toFirewall(ctx, item);

                        if( firewall != null && securityGroupId.equals(firewall.getProviderFirewallId()) ) {
                            return firewall;
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
	public @Nonnull String getProviderTermForFirewall(@Nonnull Locale locale) {
		return "security group";
	}

	@Override
	public @Nonnull Collection<FirewallRule> getRules(@Nonnull String securityGroupId) throws InternalException, CloudException {
        APITrace.begin(provider, "Firewall.getRules");
        try {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_SECURITY_GROUPS);
            ArrayList<FirewallRule> list = new ArrayList<FirewallRule>();
            EC2Method method;
            NodeList blocks;
            Document doc;

            if( provider.getEC2Provider().isEucalyptus() ) {
                parameters.put("GroupName.1", securityGroupId);
            }
            else {
                parameters.put("GroupId.1", securityGroupId);
            }
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && code.startsWith("InvalidGroup") ) {
                    return Collections.emptyList();
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("securityGroupInfo");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("item") ) {
                        NodeList attrs = item.getChildNodes();

                        for( int k=0; k<attrs.getLength(); k++ ) {
                            Node attr = attrs.item(k);

                            if( attr.getNodeName().equals("ipPermissions") ) {
                                NodeList subList = attr.getChildNodes();

                                for( int l=0; l<subList.getLength(); l++ ) {
                                    Node sub = subList.item(l);

                                    if( sub.getNodeName().equals("item") ) {
                                        list.addAll(toFirewallRules(securityGroupId, sub, Direction.INGRESS));
                                    }
                                }
                            }
                            else if( attr.getNodeName().equals("ipPermissionsEgress") ) {
                                NodeList subList = attr.getChildNodes();

                                for( int l=0; l<subList.getLength(); l++ ) {
                                    Node sub = subList.item(l);

                                    if( sub.getNodeName().equals("item") ) {
                                        list.addAll(toFirewallRules(securityGroupId, sub, Direction.EGRESS));
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
    public @Nonnull Requirement identifyPrecedenceRequirement(boolean inVlan) throws InternalException, CloudException {
        return Requirement.NONE;
    }

    private @Nonnull String getUniqueName(@Nonnull String name) throws InternalException, CloudException {
        StringBuilder str = new StringBuilder();
        
        for( int i=0; i<name.length(); i++ ) {
            char c = name.charAt(i);

            if( c == '_' || c == '-' ) {
                str.append(c);
            }
            else if( i == 0 && Character.isDigit(c)  ) {
                str.append("e-");
                str.append(c);
            }
            else if( i == 0 && Character.isLetter(c) ) {
                str.append(c);
            }
            else if( i > 0 && (Character.isLetterOrDigit(c) ) ) {
                str.append(c);
            }
        }
        if( str.length() < 1 ) {
            return "new-group";
        }
        String baseName = str.toString();
        String withName = baseName;
        int count = 1;
        boolean found;
        char c = 'a';
        
        do {
            found = false;
            for( Firewall fw : list() ) {
                String id = fw.getProviderFirewallId();
                
                if( id == null ) {
                    continue;
                }
                if( id.equals(withName) ) {
                    found = true;
                    if( count == 1 ) {
                        withName = baseName + "-" + String.valueOf(c);
                    }
                    else {
                        withName = baseName + String.valueOf(c);
                    }
                    if( c == 'z' ) {
                        if( count == 1 ) {
                            baseName = baseName + "-a";
                        }
                        else {
                            baseName = baseName + "a";
                        }
                        c = 'a';
                        count++;
                        if( count > 10 ) {
                            throw new CloudException("Could not generate a unique firewall name from " + baseName);
                        }
                    }
                    else {
                        c++;
                    }
                    break;
                }                        
            }                        
        } while( found ); 		
        return withName;
	}

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(provider, "Firewall.isSubscribed");
        try {
            ComputeServices svc = provider.getComputeServices();
        
            if( svc == null ) {
                return false;
            }
            VirtualMachineSupport support = svc.getVirtualMachineSupport();

            return (support != null && support.isSubscribed());
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isZeroPrecedenceHighest() throws InternalException, CloudException {
        return true; // nonsense
    }

    @Override
	public @Nonnull Collection<Firewall> list() throws InternalException, CloudException {
        APITrace.begin(provider, "Firewall.list");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context has been established for this request");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_SECURITY_GROUPS);
            ArrayList<Firewall> list = new ArrayList<Firewall>();
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
            blocks = doc.getElementsByTagName("securityGroupInfo");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("item") ) {
                        Firewall firewall = toFirewall(ctx, item);

                        if( firewall != null ) {
                            list.add(firewall);
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
    public @Nonnull Iterable<ResourceStatus> listFirewallStatus() throws InternalException, CloudException {
        APITrace.begin(provider, "Firewall.listFirewallStatus");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context has been established for this request");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_SECURITY_GROUPS);
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
            blocks = doc.getElementsByTagName("securityGroupInfo");
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
    public @Nonnull Iterable<RuleTargetType> listSupportedDestinationTypes(boolean inVlan) throws InternalException, CloudException {
        return Collections.singletonList(RuleTargetType.GLOBAL);
    }

    @Override
    public @Nonnull Iterable<Direction> listSupportedDirections(boolean inVlan) throws InternalException, CloudException {
        if( inVlan ) {
            ArrayList<Direction>  list = new ArrayList<Direction>();

            list.add(Direction.EGRESS);
            list.add(Direction.INGRESS);
            return list;
        }
        else {
            return Collections.singletonList(Direction.INGRESS);
        }
    }

    @Override
    public @Nonnull Iterable<Permission> listSupportedPermissions(boolean inVlan) throws InternalException, CloudException {
        return Collections.singletonList(Permission.ALLOW);
    }

    @Override
    public @Nonnull Iterable<RuleTargetType> listSupportedSourceTypes(boolean inVlan) throws InternalException, CloudException {
        ArrayList<RuleTargetType>  types = new ArrayList<RuleTargetType>();

        types.add(RuleTargetType.CIDR);
        types.add(RuleTargetType.GLOBAL);
        return types;
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        if( action.equals(FirewallSupport.ANY) ) {
            return new String[] { EC2Method.EC2_PREFIX + "*" };
        }
        else if( action.equals(FirewallSupport.AUTHORIZE) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.AUTHORIZE_SECURITY_GROUP_INGRESS, EC2Method.EC2_PREFIX + EC2Method.AUTHORIZE_SECURITY_GROUP_EGRESS };
        }
        else if( action.equals(FirewallSupport.CREATE_FIREWALL) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.CREATE_SECURITY_GROUP };
        }
        else if( action.equals(FirewallSupport.GET_FIREWALL) || action.equals(FirewallSupport.LIST_FIREWALL) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_SECURITY_GROUPS };
        }
        else if( action.equals(FirewallSupport.REMOVE_FIREWALL) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DELETE_SECURITY_GROUP };
        }
        else if( action.equals(FirewallSupport.REVOKE) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.REVOKE_SECURITY_GROUP_INGRESS, EC2Method.EC2_PREFIX + EC2Method.REVOKE_SECURITY_GROUP_EGRESS };
        }
        return new String[0];
    }


    @Override
    public void removeTags(@Nonnull String firewallId, @Nonnull Tag... tags) throws CloudException, InternalException {
        removeTags(new String[] { firewallId }, tags);
    }

    @Override
    public void removeTags(@Nonnull String[] firewallIds, @Nonnull Tag ... tags) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Firewall.removeTags");
        try {
            provider.removeTags(firewallIds, tags);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void updateTags(@Nonnull String firewallId, @Nonnull Tag... tags) throws CloudException, InternalException {
        removeTags(new String[] { firewallId }, tags);
    }

    @Override
    public void updateTags(@Nonnull String[] firewallIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Firewall.updateTags");
        try {
            provider.createTags(firewallIds, tags);
        }
        finally {
            APITrace.end();
        }
    }
    @Override
    public void revoke(@Nonnull String providerFirewallRuleId) throws InternalException, CloudException {
        FirewallRule rule = null;

        for( Firewall f : list() ) {
            String fwId = f.getProviderFirewallId();

            if( fwId != null ) {
                for( FirewallRule r : getRules(fwId) ) {
                    if( providerFirewallRuleId.equals(r.getProviderRuleId()) ) {
                        rule = r;
                        break;
                    }
                }
            }
        }
        if( rule == null ) {
            throw new CloudException("Unable to parse rule ID: " + providerFirewallRuleId);
        }
        revoke(rule.getFirewallId(), rule.getDirection(), rule.getPermission(), rule.getSourceEndpoint(), rule.getProtocol(), rule.getDestinationEndpoint(), rule.getStartPort(), rule.getEndPort());
    }

    private void revoke(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull Permission permission, @Nonnull RuleTarget sourceEndpoint, @Nonnull Protocol protocol, @Nonnull RuleTarget destinationEndpoint, int beginPort, int endPort) throws CloudException, InternalException {
        APITrace.begin(provider, "Firewall.revoke");
        try {
            if( Permission.DENY.equals(permission) ) {
                throw new OperationNotSupportedException("AWS does not support DENY rules");
            }
            //if( !destinationEndpoint.getRuleTargetType().equals(RuleTargetType.GLOBAL) ) {
            //    throw new OperationNotSupportedException("AWS does not support discreet routing of rules");
            //}
            Firewall fw = getFirewall(firewallId);

            if( fw == null ) {
                throw new CloudException("No such firewall: " + firewallId);
            }
            if( direction.equals(Direction.EGRESS) && fw.getProviderVlanId() == null ) {
                throw new OperationNotSupportedException("AWS does not support EGRESS rules for non-VPC security groups");
            }
            String action = (direction.equals(Direction.INGRESS) ? EC2Method.REVOKE_SECURITY_GROUP_INGRESS : EC2Method.REVOKE_SECURITY_GROUP_EGRESS);
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), action);
            String targetGroupId = null;
            boolean group;
            EC2Method method;
            Document doc;

            if( direction.equals(Direction.INGRESS) ) {
                group = sourceEndpoint.getRuleTargetType().equals(RuleTargetType.GLOBAL);
                if( group ) {
                    targetGroupId = sourceEndpoint.getProviderFirewallId();
                }
            }
            else {
                group = destinationEndpoint.getRuleTargetType().equals(RuleTargetType.GLOBAL);
                if( group ) {
                    targetGroupId = destinationEndpoint.getProviderFirewallId();
                }
            }
            if( provider.getEC2Provider().isEucalyptus() ) {
                parameters.put("GroupName", firewallId);
                parameters.put("IpProtocol", protocol.name().toLowerCase());
                parameters.put("FromPort", String.valueOf(beginPort));
                parameters.put("ToPort", endPort == -1 ? String.valueOf(beginPort) : String.valueOf(endPort));
                if( group ) {
                    parameters.put("GroupName", targetGroupId);
                }
                else {
                    parameters.put("CidrIp", sourceEndpoint.getCidr());
                }
            }
            else {
                parameters.put("GroupId", firewallId);
                parameters.put("IpPermissions.1.IpProtocol", protocol.name().toLowerCase());
                parameters.put("IpPermissions.1.FromPort", String.valueOf(beginPort));
                parameters.put("IpPermissions.1.ToPort", endPort == -1 ? String.valueOf(beginPort) : String.valueOf(endPort));
                if( group ) {
                    if( targetGroupId.startsWith("sg-") ) {
                        parameters.put("IpPermissions.1.Groups.1.GroupId", targetGroupId);
                    }
                    else {
                        parameters.put("IpPermissions.1.Groups.1.GroupName", targetGroupId);
                    }
                }
                else if( direction.equals(Direction.INGRESS) ) {
                    parameters.put("IpPermissions.1.IpRanges.1.CidrIp", sourceEndpoint.getCidr());
                }
                else {
                    parameters.put("IpPermissions.1.IpRanges.1.CidrIp", destinationEndpoint.getCidr());
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
            method.checkSuccess(doc.getElementsByTagName("return"));
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void revoke(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull Permission permission, @Nonnull String cidr, @Nonnull Protocol protocol, @Nonnull RuleTarget destination, int beginPort, int endPort) throws CloudException, InternalException {
        RuleTarget source;

        if( cidr.startsWith("sg-") ) {
            source = RuleTarget.getGlobal(cidr);
        }
        else {
            source = RuleTarget.getCIDR(cidr);
        }
        if( direction.equals(Direction.INGRESS) ) {
            revoke(firewallId, direction, permission, source, protocol, destination, beginPort, endPort);
        }
        else {
            revoke(firewallId, direction, permission, destination, protocol, source, beginPort, endPort);
        }
    }


    @Override
    public boolean supportsFirewallCreation(boolean inVlan) throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean supportsFirewallSources() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean supportsRules(@Nonnull Direction direction, @Nonnull Permission permission, boolean inVlan) throws CloudException, InternalException {
        return (permission.equals(Permission.ALLOW) && (!(inVlan && provider.getEC2Provider().isEucalyptus()) && (inVlan || direction.equals(Direction.INGRESS))));
    }

    private @Nullable Firewall toFirewall(@Nonnull ProviderContext ctx, @Nullable Node node) {
        if( node == null ) {
            return null;
        }
        String fwName = null, fwId = null, fwDesc = null;
		NodeList attrs = node.getChildNodes();
		Firewall firewall = new Firewall();
        String regionId = ctx.getRegionId();
		String vpcId = null;

        if( regionId == null ) {
            return null;
        }
        firewall.setRegionId(regionId);
        firewall.setAvailable(true);
        firewall.setActive(true);
		for( int i=0; i<attrs.getLength(); i++ ) {
			Node attr = attrs.item(i);
			String name;
			
			name = attr.getNodeName();
			if( name.equals("groupName") ) {
                fwName = attr.getFirstChild().getNodeValue().trim();
			}
			else if( name.equals("groupDescription") ) {
				fwDesc = attr.getFirstChild().getNodeValue().trim();
			}
			else if( name.equals("groupId") ) {
                fwId = attr.getFirstChild().getNodeValue().trim();			    
			}
			else if( name.equals("vpcId") ) {
			    if( attr.hasChildNodes() ) {
			        vpcId = attr.getFirstChild().getNodeValue();
			        if( vpcId != null ) {
			            vpcId = vpcId.trim();
			        }
			    }
			}
            else if ( name.equals("tagSet")) {
                provider.setTags( attr, firewall );
            }
		}
        if( fwId == null ) {
            if( fwName == null ) {
                return null;
            }
            fwId = fwName;
        }
		if( fwName == null ) {
		    fwName = fwId;
		}
        firewall.setProviderFirewallId(fwId);
        firewall.setName(fwName);
        if( fwDesc == null ) {
            fwDesc = fwName;
        }
        firewall.setDescription(fwDesc);
		if( vpcId != null ) {
		    firewall.setName(firewall.getName() + " (VPC " + vpcId + ")");
		    firewall.setProviderVlanId(vpcId);
		}
		return firewall;
	}
	
	private @Nonnull Collection<FirewallRule> toFirewallRules(@Nonnull String securityGroupId, @Nullable Node node, @Nonnull Direction direction) {
	    ArrayList<FirewallRule> rules = new ArrayList<FirewallRule>();
        
        if( node == null ) {
            return rules;
        }
        ArrayList<String> cidrs = new ArrayList<String>();
        ArrayList<String> groups = new ArrayList<String>();
        NodeList attrs = node.getChildNodes();
        int startPort = -2, endPort = -2;
		Protocol protocol = Protocol.TCP;
		
		for( int i=0; i<attrs.getLength(); i++ ) {
			Node attr = attrs.item(i);
			String name;
			
			name = attr.getNodeName();
			if( name.equals("ipProtocol") ) {
			    String val = attr.getFirstChild().getNodeValue().trim();
			    
			    if( !val.equals("") && !val.equals("-1") ) {
			        protocol = Protocol.valueOf(attr.getFirstChild().getNodeValue().trim().toUpperCase());
			    }
			    else {
			        protocol = Protocol.ICMP;
			    }
			}
			else if( name.equals("fromPort") ) {
				startPort = Integer.parseInt(attr.getFirstChild().getNodeValue().trim());
			}
			else if( name.equals("toPort") ) {
				endPort = Integer.parseInt(attr.getFirstChild().getNodeValue().trim());
			}
            else if( name.equals("groups") && attr.hasChildNodes() ) {
                NodeList children = attr.getChildNodes();

                for( int j=0; j<children.getLength(); j++ ) {
                    Node child = children.item(j);

                    if( child.getNodeName().equals("item") ) {
                        if( child.hasChildNodes() ) {
                            NodeList targets = child.getChildNodes();
                            String groupId = null, groupName = null;

                            for( int k=0; k<targets.getLength(); k++ ) {
                                Node group = targets.item(k);

                                if( group.getNodeName().equals("groupId") ) {
                                    groupId = group.getFirstChild().getNodeValue().trim();
                                }
                                if( group.getNodeName().equals("groupName") ) {
                                    groupName = group.getFirstChild().getNodeValue().trim();
                                }
                            }
                            if( groupId != null ) {
                                groups.add(groupId);
                            }
                            else if( groupName != null ) {
                                groups.add(groupName);
                            }
                        }
                    }
                }
            }
			else if( name.equals("ipRanges") ) {
				if( attr.hasChildNodes() ) {
					NodeList children = attr.getChildNodes();
				
					for( int j=0; j<children.getLength(); j++ ) {
						Node child = children.item(j);
					
						if( child.getNodeName().equals("item") ) {
						    if( child.hasChildNodes() ) { 
						        NodeList targets = child.getChildNodes();
						        
						        for( int k=0; k<targets.getLength(); k++ ) {
						            Node cidr = targets.item(k);

						            if( cidr.getNodeName().equals("cidrIp") ) {
						                cidrs.add(cidr.getFirstChild().getNodeValue());
						            }
						        }
						    }
						}
					}
				}
			}
		}
        for( String gid : groups ) {
            if( direction.equals(Direction.INGRESS) ) {
                rules.add(FirewallRule.getInstance(null, securityGroupId, RuleTarget.getGlobal(gid), direction, protocol, Permission.ALLOW, RuleTarget.getGlobal(securityGroupId), startPort, endPort));
            }
            else {
                rules.add(FirewallRule.getInstance(null, securityGroupId, RuleTarget.getGlobal(securityGroupId), direction, protocol, Permission.ALLOW, RuleTarget.getGlobal(gid), startPort, endPort));
            }
        }
		for( String cidr : cidrs ) {
            if( direction.equals(Direction.INGRESS) ) {
                rules.add(FirewallRule.getInstance(null, securityGroupId, RuleTarget.getCIDR(cidr), direction, protocol, Permission.ALLOW, RuleTarget.getGlobal(securityGroupId), startPort, endPort));
            }
            else {
                rules.add(FirewallRule.getInstance(null, securityGroupId, RuleTarget.getGlobal(securityGroupId), direction, protocol, Permission.ALLOW, RuleTarget.getCIDR(cidr), startPort, endPort));
            }
        }
		return rules;		
	}

    private @Nullable ResourceStatus toStatus(@Nullable Node node) {
        if( node == null ) {
            return null;
        }
        NodeList attrs = node.getChildNodes();
        String fwId = null, fwName = null;

        for( int i=0; i<attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;

            name = attr.getNodeName();
            if( name.equals("groupName") ) {
                fwName = attr.getFirstChild().getNodeValue().trim();
            }
            else if( name.equals("groupId") ) {
                fwId = attr.getFirstChild().getNodeValue().trim();
            }
        }
        if( fwId == null && fwName == null ) {
            return null;
        }
        if( fwId == null ) {
            fwId = fwName;
        }
        return new ResourceStatus(fwId, true);
    }
}
