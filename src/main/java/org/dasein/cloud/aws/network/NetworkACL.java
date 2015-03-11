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

package org.dasein.cloud.aws.network;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.Tag;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.aws.compute.EC2Method;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.*;
import org.dasein.cloud.util.APITrace;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;

/**
 * Implements support for AWS VPC network ACLs as Dasein Cloud network firewall support.
 * <p>Created by George Reese: 2/4/13 9:52 AM</p>
 * @author George Reese
 * @since 2013.04
 * @version 2013.04 initial version (issue #8)
 */
public class NetworkACL extends AbstractNetworkFirewallSupport<AWSCloud> {
    static private final Logger logger = AWSCloud.getLogger(NetworkACL.class);

    private NetworkACLCapabilities capabilities;

    NetworkACL(AWSCloud cloud) {
        super(cloud);
    }

    @Override
    public void associateWithSubnet(@Nonnull String firewallId, @Nonnull String withSubnetId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "NetworkFirewall.associateWithSubnet");
        try {
            String currentAssociation = getCurrentAssociation(withSubnetId);

            if( currentAssociation == null ) {
                throw new CloudException("Unable to identify subnet association for " + withSubnetId);
            }
            Map<String,String> parameters = getProvider().getStandardParameters(getContext(), EC2Method.REPLACE_NETWORK_ACL_ASSOC);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("AssociationId", currentAssociation);
            parameters.put("NetworkAclId", firewallId);
            method = new EC2Method(getProvider(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            String id = null;

            blocks = doc.getElementsByTagName("newAssociationId");
            if( blocks.getLength() > 0 ) {
                id = blocks.item(0).getFirstChild().getNodeValue().trim();
            }
            if( id == null ) {
                throw new CloudException("Association failed without explanation");
            }
        }
        finally {
            APITrace.end();
        }

    }

    @Override
    public @Nonnull String authorize(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull Permission permission, @Nonnull RuleTarget sourceEndpoint, @Nonnull Protocol protocol, @Nonnull RuleTarget destinationEndpoint, int beginPort, int endPort, int precedence) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "NetworkFirewall.authorize");
        try {
            FirewallRule currentRule = null;

            for( FirewallRule rule : listRules(firewallId) ) {
                if( rule.getDirection().equals(direction) && rule.getPrecedence() == precedence ) {
                    currentRule = rule;
                }
            }

            Map<String,String> parameters = getProvider().getStandardParameters(getContext(), currentRule == null ? EC2Method.CREATE_NETWORK_ACL_ENTRY : EC2Method.REPLACE_NETWORK_ACL_ENTRY);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("NetworkAclId", firewallId);
            parameters.put("Egress", String.valueOf(direction.equals(Direction.EGRESS)));
            parameters.put("RuleNumber", String.valueOf(precedence));
            parameters.put("Protocol", toProtocolNumber(protocol));
            parameters.put("RuleAction", permission.name().toLowerCase());
            String cidr;

            if( direction.equals(Direction.INGRESS) ) {
                cidr = sourceEndpoint.getCidr();
            }
            else {
                cidr = destinationEndpoint.getCidr();
            }
            if( cidr == null ) {
                throw new CloudException("No CIDR was specified for " + (direction.equals(Direction.INGRESS) ? "the source endpoint" : "the destination endpoint"));
            }
            parameters.put("CidrBlock", cidr);
            if( !protocol.equals(Protocol.ICMP) ) {
                parameters.put("PortRange.From", String.valueOf(beginPort));
                parameters.put("PortRange.To", String.valueOf(endPort));
            }
            else {
                parameters.put("Icmp.Code", "-1");
                parameters.put("Icmp.Type", "-1");
            }
            method = new EC2Method(getProvider(), parameters);
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
            return (firewallId + ":" + direction.name() + ":" + String.valueOf(precedence));
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String createFirewall(@Nonnull FirewallCreateOptions options) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "NetworkFirewall.createFirewall");
        try {
            Map<String,String> parameters = getProvider().getStandardParameters(getContext(), EC2Method.CREATE_NETWORK_ACL);
            EC2Method method;
            NodeList blocks;
            Document doc;

            String vlanId = options.getProviderVlanId();

            if( vlanId != null ) {
                parameters.put("VpcId", vlanId);
            }
            else {
                throw new CloudException("You must specify a VLAN with which to associate your network ACL");
            }
            method = new EC2Method(getProvider(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            String firewallId;

            blocks = doc.getElementsByTagName("networkAclId");
            if( blocks.getLength() > 0 ) {
                String id = blocks.item(0).getFirstChild().getNodeValue().trim();
                Map<String,String> metaData = options.getMetaData();

                metaData.put("Name", options.getName());
                metaData.put("Description", options.getDescription());

                ArrayList<Tag> tags = new ArrayList<Tag>();

                for( Map.Entry<String,String> entry : metaData.entrySet() ) {
                    String key = entry.getKey();
                    String value = entry.getValue();

                    if( value != null ) {
                        tags.add(new Tag(key, value));
                    }
                }
                getProvider().createTags(EC2Method.SERVICE_ID, id, tags.toArray(new Tag[tags.size()]));
                firewallId = id;
            }
            else {
                throw new CloudException("Failed to create network ACL without explanation.");
            }
            FirewallRuleCreateOptions[] ruleOptions = options.getInitialRules();

            if( ruleOptions != null && ruleOptions.length > 0 ) {
                for( FirewallRuleCreateOptions option : ruleOptions ) {
                    authorize(firewallId, option);
                }
            }
            return firewallId;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Map<FirewallConstraints.Constraint, Object> getActiveConstraintsForFirewall(@Nonnull String firewallId) throws CloudException, InternalException {
        return new HashMap<FirewallConstraints.Constraint, Object>();
    }

    @Nonnull
    @Override
    public NetworkFirewallCapabilities getCapabilities() {
        if( capabilities == null ) {
            capabilities = new NetworkACLCapabilities( getProvider());
        }
        return capabilities;
    }

    private @Nullable String getCurrentAssociation(@Nonnull String subnetId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "NetworkFirewall.getCurrentACLAssociation");
        try {
            Map<String,String> parameters = getProvider().getStandardParameters(getContext(), EC2Method.DESCRIBE_NETWORK_ACLS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("Filter.1.Name", "association.subnet-id");
            parameters.put("Filter.1.Value.1", subnetId);
            method = new EC2Method(getProvider(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("associationSet");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("item") ) {
                        NodeList attributes = item.getChildNodes();
                        String aid = null;
                        String sid = null;

                        for( int k=0; k<attributes.getLength(); k++ ) {
                            Node attribute = attributes.item(k);

                            if( attribute.getNodeName().equalsIgnoreCase("subnetId") && attribute.hasChildNodes() ) {
                                sid = attribute.getFirstChild().getNodeValue().trim();
                            }
                            else if( attribute.getNodeName().equalsIgnoreCase("networkAclAssociationId") && attribute.hasChildNodes() ) {
                                aid = attribute.getFirstChild().getNodeValue().trim();
                            }
                        }
                        if( sid != null && sid.equals(subnetId) ) {
                            if( aid != null ) {
                                return aid;
                            }
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
    public @Nullable Firewall getFirewall(@Nonnull String firewallId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "NetworkFirewall.getFirewall");
        try {
            Map<String,String> parameters = getProvider().getStandardParameters(getContext(), EC2Method.DESCRIBE_NETWORK_ACLS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("NetworkAclId.1", firewallId);
            method = new EC2Method(getProvider(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                if( "InvalidNetworkAclID.NotFound".equalsIgnoreCase(e.getCode()) ) {
                    return null;
                }
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("networkAclSet");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("item") ) {
                        Firewall firewall = toFirewall(item);

                        if( firewall != null ) {
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
    public @Nonnull FirewallConstraints getFirewallConstraintsForCloud() throws CloudException, InternalException {
        return getCapabilities().getFirewallConstraintsForCloud();
    }

    @Override
    public @Nonnull String getProviderTermForNetworkFirewall(@Nonnull Locale locale) {
        return getCapabilities().getProviderTermForNetworkFirewall(locale);
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "NetworkFirewall.isSubscribed");
        try {
            NetworkServices services = getProvider().getNetworkServices();

            if( services != null ) {
                VLANSupport support = services.getVlanSupport();

                return (support != null && support.isSubscribed());
            }
            return false;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Collection<Firewall> listFirewalls() throws InternalException, CloudException {
        APITrace.begin(getProvider(), "NetworkFirewall.listFirewalls");
        try {
            Map<String,String> parameters = getProvider().getStandardParameters(getContext(), EC2Method.DESCRIBE_NETWORK_ACLS);
            ArrayList<Firewall> list = new ArrayList<Firewall>();
            EC2Method method;
            NodeList blocks;
            Document doc;

            method = new EC2Method(getProvider(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("networkAclSet");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("item") ) {
                        Firewall firewall = toFirewall(item);

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
    public @Nonnull Iterable<FirewallRule> listRules(@Nonnull String firewallId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "NetworkFirewall.listRules");
        try {
            Map<String,String> parameters = getProvider().getStandardParameters(getContext(), EC2Method.DESCRIBE_NETWORK_ACLS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("NetworkAclId.1", firewallId);
            method = new EC2Method(getProvider(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            ArrayList<FirewallRule> rules = new ArrayList<FirewallRule>();

            blocks = doc.getElementsByTagName("networkAclSet");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("item") ) {
                        NodeList attributes = item.getChildNodes();

                        for( int k=0; k<attributes.getLength(); k++ ) {
                            Node attribute = attributes.item(k);

                            if( attribute.getNodeName().equalsIgnoreCase("entrySet") && attribute.hasChildNodes() ) {
                                NodeList entryItems = attribute.getChildNodes();

                                for( int l=0; l<entryItems.getLength(); l++ ) {
                                    FirewallRule rule = toRule(firewallId, entryItems.item(l));

                                    if( rule != null ) {
                                        rules.add(rule);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return rules;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<RuleTargetType> listSupportedDestinationTypes() throws InternalException, CloudException {
        return getCapabilities().listSupportedDestinationTypes();
    }

    @Override
    public @Nonnull Iterable<Direction> listSupportedDirections() throws InternalException, CloudException {
        return getCapabilities().listSupportedDirections();
    }

    @Override
    public @Nonnull Iterable<Permission> listSupportedPermissions() throws InternalException, CloudException {
        return getCapabilities().listSupportedPermissions();
    }

    @Override
    public @Nonnull Iterable<RuleTargetType> listSupportedSourceTypes() throws InternalException, CloudException {
        return getCapabilities().listSupportedSourceTypes();
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        if( action.equals(FirewallSupport.ANY) ) {
            return new String[] { EC2Method.EC2_PREFIX + "*" };
        }
        else if( action.equals(NetworkFirewallSupport.ASSOCIATE) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.REPLACE_NETWORK_ACL_ASSOC };
        }
        else if( action.equals(NetworkFirewallSupport.AUTHORIZE) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.CREATE_NETWORK_ACL_ENTRY, EC2Method.EC2_PREFIX + EC2Method.REPLACE_NETWORK_ACL_ENTRY };
        }
        else if( action.equals(NetworkFirewallSupport.CREATE_FIREWALL) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.CREATE_NETWORK_ACL };
        }
        else if( action.equals(NetworkFirewallSupport.GET_FIREWALL) || action.equals(FirewallSupport.LIST_FIREWALL) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_NETWORK_ACLS };
        }
        else if( action.equals(NetworkFirewallSupport.REMOVE_FIREWALL) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DELETE_NETWORK_ACL };
        }
        else if( action.equals(NetworkFirewallSupport.REVOKE) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DELETE_NETWORK_ACL_ENTRY };
        }
        return new String[0];
    }

    @Override
    public void removeFirewall(@Nonnull String... firewallIds) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "NetworkFirewall.removeFirewall");
        try {
            for( String id : firewallIds ) {
                Firewall fw = getFirewall(id);

                if( fw != null ) {
                    String vlanId = fw.getProviderVlanId();
                    String newFw = null;

                    for( Firewall current : listFirewalls() ) {
                        if( current.getProviderVlanId().equals(vlanId) ) {
                            boolean pass = false;

                            for( String testId : firewallIds ) {
                                if( id.equals(testId) ) {
                                    pass = true;
                                }
                            }
                            if( !pass ) {
                                newFw = current.getProviderFirewallId();
                                if( current.getName().contains("default") ) {
                                    break;
                                }
                            }
                        }
                    }
                    if( newFw != null ) {
                        for( String subnetId : fw.getSubnetAssociations() ) {
                            associateWithSubnet(newFw, subnetId);
                        }
                    }

                    Map<String,String> parameters = getProvider().getStandardParameters(getContext(), EC2Method.DELETE_NETWORK_ACL);
                    EC2Method method;
                    NodeList blocks;
                    Document doc;

                    parameters.put("NetworkAclId", id);
                    method = new EC2Method(getProvider(), parameters);
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
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeTags(@Nonnull String firewallId, @Nonnull Tag... tags) throws CloudException, InternalException {
        removeTags(new String[] { firewallId }, tags);
    }

    @Override
    public void removeTags(@Nonnull String[] firewallIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "NetworkFirewall.removeTags");
        try {
            getProvider().removeTags(EC2Method.SERVICE_ID, firewallIds, tags);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void revoke(@Nonnull String providerFirewallRuleId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "NetworkFirewall.revoke");
        try {
            Map<String,String> parameters = getProvider().getStandardParameters(getContext(), EC2Method.DELETE_NETWORK_ACL_ENTRY);
            EC2Method method;
            NodeList blocks;
            Document doc;

            String[] parts = providerFirewallRuleId.split(":");
            if( parts.length != 3 ) {
                throw new CloudException("Invalid network ACL entry: " + providerFirewallRuleId);
            }
            parameters.put("NetworkAclId", parts[0]);
            parameters.put("Egress", String.valueOf(parts[1].equalsIgnoreCase(Direction.EGRESS.name())));
            parameters.put("RuleNumber", parts[2]);
            method = new EC2Method(getProvider(), parameters);
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
    public void updateTags(@Nonnull String firewallId, @Nonnull Tag... tags) throws CloudException, InternalException {
        updateTags(new String[] { firewallId }, tags);
    }

    @Override
    public void updateTags(@Nonnull String[] firewallIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "NetworkFirewall.updateTags");
        try {
            getProvider().createTags(EC2Method.SERVICE_ID, firewallIds, tags);
        }
        finally {
            APITrace.end();
        }
    }

    private @Nullable Firewall toFirewall(@Nullable Node networkAcl) throws CloudException, InternalException {
        if( networkAcl == null ) {
            return null;
        }
        Firewall firewall = new Firewall();

        firewall.setActive(true);
        firewall.setAvailable(true);

        String regionId = getContext().getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was specified for this context");
        }
        firewall.setRegionId(regionId);

        NodeList attributes = networkAcl.getChildNodes();

        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);

            if( attribute.getNodeName().equalsIgnoreCase("networkAclId") && attribute.hasChildNodes() ) {
                firewall.setProviderFirewallId(attribute.getFirstChild().getNodeValue().trim());
            }
            else if( attribute.getNodeName().equalsIgnoreCase("vpcId") && attribute.hasChildNodes() ) {
                firewall.setProviderVlanId(attribute.getFirstChild().getNodeValue().trim());
            }
            else if( attribute.getNodeName().equalsIgnoreCase("associationSet") && attribute.hasChildNodes() ) {
                TreeSet<String> subnets = new TreeSet<String>();
                NodeList items = attribute.getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.hasChildNodes() ) {
                        NodeList iaList = item.getChildNodes();

                        for( int k=0; k<iaList.getLength(); k++ ) {
                            Node ia = iaList.item(k);

                            if( ia.getNodeName().equalsIgnoreCase("subnetId") && ia.hasChildNodes() ) {
                                subnets.add(ia.getFirstChild().getNodeValue().trim());
                            }
                        }
                    }
                }
                firewall.setSubnetAssociations(subnets.toArray(new String[subnets.size()]));
            }
            else if ( attribute.getNodeName().equalsIgnoreCase("tagSet")) {
                getProvider().setTags( attribute, firewall );
            }
        }

        String id = firewall.getProviderFirewallId();

        if( id == null ) {
            return null;
        }

        String name = firewall.getName();

        if( name == null ) {
            name = firewall.getTags().get("Name");
            if( name == null ) {
                name = id;
            }
            firewall.setName(name);
        }
        String description = firewall.getDescription();

        if( description == null ) {
            description = firewall.getTags().get("Description");
            if( description == null ) {
                description = name;
            }
            firewall.setDescription(description);
        }
        return firewall;
    }

    private @Nonnull String toProtocolNumber(@Nonnull Protocol protocol) {
        switch( protocol ) {
            case TCP: return "6";
            case UDP: return "17";
            case ANY: return "-1";
            case ICMP: return "1";
            default: return "-1";
        }
    }

    private @Nullable FirewallRule toRule(@Nonnull String firewallId, @Nullable Node aclEntry) throws CloudException, InternalException {
        if( aclEntry == null ) {
            return null;
        }
        NodeList attributes = aclEntry.getChildNodes();
        Permission permission = Permission.ALLOW;
        Direction direction = null;
        Protocol protocol = null;
        int precedence = -1;
        int startPort = -1;
        int endPort = -1;
        String cidr = null;

        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);

            if( attribute.getNodeName().equalsIgnoreCase("ruleNumber") && attribute.hasChildNodes() ) {
                precedence = Integer.parseInt(attribute.getFirstChild().getNodeValue().trim());
            }
            if( attribute.getNodeName().equalsIgnoreCase("protocol") && attribute.hasChildNodes() ) {
                String proto = attribute.getFirstChild().getNodeValue().trim();

                if( proto.equalsIgnoreCase("tcp") || proto.equals("6") ) {
                    protocol = Protocol.TCP;
                }
                else if( proto.equalsIgnoreCase("udp") || proto.equals("17") ) {
                    protocol = Protocol.UDP;
                }
                else if( proto.equalsIgnoreCase("icmp") || proto.equals("1") || proto.equals("58") ) {
                    protocol = Protocol.ICMP;
                }
//                else if( proto.equals("-1") || proto.equals("4") || proto.equals("41") ) {
//                    protocol = Protocol.ANY;
//                }
                else {
                    protocol = Protocol.ANY;
                }
            }
            if( attribute.getNodeName().equalsIgnoreCase("ruleAction") && attribute.hasChildNodes() ) {
                String action = attribute.getFirstChild().getNodeValue().trim();

                if( action.equalsIgnoreCase("allow") ) {
                    permission = Permission.ALLOW;
                }
                else {
                    permission = Permission.DENY;
                }
            }
            if( attribute.getNodeName().equalsIgnoreCase("egress") && attribute.hasChildNodes() ) {
                boolean egress = attribute.getFirstChild().getNodeValue().trim().equalsIgnoreCase("true");

                if( egress ) {
                    direction = Direction.EGRESS;
                }
                else {
                    direction = Direction.INGRESS;
                }
            }
            if( attribute.getNodeName().equalsIgnoreCase("cidrBlock") && attribute.hasChildNodes() ) {
                cidr = attribute.getFirstChild().getNodeValue().trim();
            }
            if( attribute.getNodeName().equalsIgnoreCase("portRange") && attribute.hasChildNodes() ) {
                NodeList parts = attribute.getChildNodes();

                for( int j=0; j<parts.getLength(); j++ ) {
                    Node part = parts.item(j);

                    if( part.getNodeName().equalsIgnoreCase("from") && part.hasChildNodes() ) {
                        startPort = Integer.parseInt(part.getFirstChild().getNodeValue().trim());
                    }
                    else if( part.getNodeName().equalsIgnoreCase("to") && part.hasChildNodes() ) {
                        endPort = Integer.parseInt(part.getFirstChild().getNodeValue().trim());
                    }
                }
            }
        }
        if( direction == null || cidr == null ) {
            return null;
        }
        RuleTarget sourceEndpoint, destinationEndpoint;

        if( direction.equals(Direction.INGRESS) ) {
            sourceEndpoint = RuleTarget.getCIDR(cidr);
            destinationEndpoint = RuleTarget.getGlobal(firewallId);
        }
        else {
            destinationEndpoint = RuleTarget.getCIDR(cidr);
            sourceEndpoint = RuleTarget.getGlobal(firewallId);
        }
        if( startPort == -1 && endPort != -1 ) {
            startPort = endPort;
        }
        else if( startPort != -1 && endPort == -1 ) {
            endPort = startPort;
        }
        else if( startPort > endPort ) {
            int x = startPort;

            startPort = endPort;
            endPort = x;
        }
        FirewallRule rule = FirewallRule.getInstance(firewallId + ":" + direction.name() + ":" + String.valueOf(precedence), firewallId, sourceEndpoint, direction, protocol, permission, destinationEndpoint, startPort, endPort);

        rule.withPrecedence(precedence);
        return rule;
    }
}
