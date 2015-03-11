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

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.network.*;

import javax.annotation.Nonnull;
import java.util.*;

/**
 * Describes the capabilities of AWS with respect to Dasein network firewall operations.
 * <p>Created by Stas Maksimov: 09/03/2014 20:32</p>
 *
 * @author Stas Maksimov
 * @version 2014.03 initial version
 * @since 2014.03
 */
public class NetworkACLCapabilities extends AbstractCapabilities<AWSCloud> implements NetworkFirewallCapabilities {
    public NetworkACLCapabilities(AWSCloud provider) {
        super(provider);
    }

    static private final FirewallConstraints firewallConstraints = FirewallConstraints.getInstance();

    @Nonnull
    @Override
    public FirewallConstraints getFirewallConstraintsForCloud() throws InternalException, CloudException {
        return firewallConstraints;
    }

    @Nonnull
    @Override
    public String getProviderTermForNetworkFirewall(@Nonnull Locale locale) {
        return "network ACL";
    }

    @Nonnull
    @Override
    public Requirement identifyPrecedenceRequirement() throws InternalException, CloudException {
        return Requirement.REQUIRED;
    }

    @Override
    public boolean isZeroPrecedenceHighest() throws InternalException, CloudException {
        return true;
    }

    @Nonnull
    @Override
    public Iterable<RuleTargetType> listSupportedDestinationTypes() throws InternalException, CloudException {
        return Collections.singletonList(RuleTargetType.GLOBAL);
    }

    static private Collection<Direction> supportedDirections;

    @Nonnull
    @Override
    public Iterable<Direction> listSupportedDirections() throws InternalException, CloudException {
        if( supportedDirections == null ) {
            supportedDirections = Arrays.asList(Direction.INGRESS, Direction.EGRESS);
        }
        return supportedDirections;
    }

    static private Collection<Permission> supportedPermissions;

    @Nonnull
    @Override
    public Iterable<Permission> listSupportedPermissions() throws InternalException, CloudException {
        if( supportedPermissions == null ) {
            supportedPermissions = Arrays.asList(Permission.ALLOW, Permission.DENY);
        }
        return supportedPermissions;
    }

    @Nonnull
    @Override
    public Iterable<RuleTargetType> listSupportedSourceTypes() throws InternalException, CloudException {
        return Collections.singletonList(RuleTargetType.CIDR);
    }

    @Override
    public boolean supportsNetworkFirewallCreation() throws CloudException, InternalException {
        return true;
    }
}
