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

import org.dasein.cloud.*;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.RegionsAndZones;
import org.dasein.cloud.network.*;
import org.dasein.cloud.util.NamingConstraints;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

/**
 * Describes the capabilities of AWS with respect to Dasein firewall operations.
 * <p>Created by Stas Maksimov: 3/03/14 10:00 PM</p>
 * @author Stas Maksimov
 * @version 2014.03 initial version
 * @since 2014.03
 */
public class SecurityGroupCapabilities extends AbstractCapabilities<AWSCloud> implements FirewallCapabilities {

    public SecurityGroupCapabilities(@Nonnull AWSCloud cloud) { super(cloud); }

    @Nonnull
    @Override
    public FirewallConstraints getFirewallConstraintsForCloud() throws InternalException, CloudException {
        return FirewallConstraints.getInstance();
    }

    @Nonnull
    @Override
    public String getProviderTermForFirewall(@Nonnull Locale locale) {
        return "security group";
    }

    @Nullable
    @Override
    public VisibleScope getFirewallVisibleScope() {
        return null;
    }

    @Nonnull
    @Override
    public Requirement identifyPrecedenceRequirement(boolean inVlan) throws InternalException, CloudException {
        return Requirement.NONE;
    }

    @Override
    public boolean isZeroPrecedenceHighest() throws InternalException, CloudException {
        return true; // Doesn't matter as AWS doesn't support rule precedence, previously we also returned true.
    }

    @Nonnull
    @Override
    @Deprecated
    public Iterable<RuleTargetType> listSupportedDestinationTypes(boolean inVlan) throws InternalException, CloudException {
        return listSupportedDestinationTypes(inVlan, Direction.INGRESS);
    }

    @Override
    public @Nonnull Iterable<RuleTargetType> listSupportedDestinationTypes(boolean inVlan, Direction direction) throws InternalException, CloudException {
        List<RuleTargetType> supportedDestinationTypes = new ArrayList<RuleTargetType>();
        if (direction.equals(Direction.INGRESS)) {
            supportedDestinationTypes = Collections.singletonList(RuleTargetType.GLOBAL);
        }
        else if (direction.equals(Direction.EGRESS)){
            supportedDestinationTypes = Collections.unmodifiableList(Arrays.asList(RuleTargetType.CIDR, RuleTargetType.GLOBAL));
        }
        return supportedDestinationTypes;
    }

    static private volatile List<Direction> supportedDirectionsVlan =
            Collections.unmodifiableList(Arrays.asList(Direction.EGRESS, Direction.INGRESS));

    @Nonnull
    @Override
    public Iterable<Direction> listSupportedDirections(boolean inVlan) throws InternalException, CloudException {
        if( inVlan ) {
            return supportedDirectionsVlan;
        }
        else {
            return Collections.singletonList(Direction.INGRESS);
        }
    }

    @Nonnull
    @Override
    public Iterable<Permission> listSupportedPermissions(boolean inVlan) throws InternalException, CloudException {
        return Collections.singletonList(Permission.ALLOW);
    }

    @Override
    public @Nonnull Iterable<Protocol> listSupportedProtocols( boolean inVlan ) throws InternalException, CloudException {
        List<Protocol> protocols = Arrays.asList(Protocol.TCP, Protocol.UDP, Protocol.ICMP);
        // TODO: The ALL in VLAN limitation only seems valid for ingress; egress doesn't specify this limitation.
        if( inVlan ) {
            protocols.add(Protocol.ANY);
        }
        return Collections.unmodifiableList(protocols);
    }

    @Nonnull
    @Deprecated
    @Override
    public Iterable<RuleTargetType> listSupportedSourceTypes(boolean inVlan) throws InternalException, CloudException {
        return listSupportedSourceTypes(inVlan, Direction.INGRESS);
    }

    @Nonnull @Override public Iterable<RuleTargetType> listSupportedSourceTypes(boolean inVlan, Direction direction) throws InternalException, CloudException {
        List<RuleTargetType> supportedSourceTypes = new ArrayList<RuleTargetType>();
        if (direction.equals(Direction.INGRESS)) {
            supportedSourceTypes = Collections.unmodifiableList(Arrays.asList(RuleTargetType.CIDR, RuleTargetType.GLOBAL));
        }
        else if (direction.equals(Direction.EGRESS)){
            supportedSourceTypes = Collections.singletonList(RuleTargetType.GLOBAL);
        }
        return supportedSourceTypes;
    }

    @Override
    public boolean requiresRulesOnCreation() throws CloudException, InternalException {
        // Rules will be implied if not set upon create() call
        return false;
    }

    @Override
    @Nonnull
    public Requirement requiresVLAN() throws CloudException, InternalException {
        // no VLAN support in EC2-Classic, it's optional in EC2-VPC - the default VPC will be used if not specified
        final RegionsAndZones services = getProvider().getDataCenterServices();
        return AWSCloud.PLATFORM_EC2.equals(services.isRegionEC2VPC(getContext().getRegionId()))
                ? Requirement.NONE : Requirement.OPTIONAL;
    }

    @Override
    public boolean supportsRules(@Nonnull Direction direction, @Nonnull Permission permission, boolean inVlan) throws CloudException, InternalException {
        return (permission.equals(Permission.ALLOW) && (!(inVlan && getProvider().getEC2Provider().isEucalyptus()) && (inVlan || direction.equals(Direction.INGRESS))));
    }

    @Override
    public boolean supportsFirewallCreation(boolean inVlan) throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean supportsFirewallDeletion() throws CloudException, InternalException {
        return true;
    }

    @Override
    public @Nonnull NamingConstraints getFirewallNamingConstraints() throws CloudException, InternalException {
        return NamingConstraints.getAlphaNumeric(0, 255);
    }
}
