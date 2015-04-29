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

import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.network.AbstractNetworkServices;
import org.dasein.cloud.network.DNSSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class EC2NetworkServices extends AbstractNetworkServices<AWSCloud> {
    public EC2NetworkServices(AWSCloud provider) {
        super(provider);
    }

    @Override
    public @Nullable DNSSupport getDnsSupport() {
        if( getProvider().getEC2Provider().isAWS() || getProvider().getEC2Provider().isEnStratus() ) {
            return new Route53(getProvider());
        }
        return null;
    }

    @Override
    public @Nonnull SecurityGroup getFirewallSupport() {
        return new SecurityGroup(getProvider());
    }

    @Override
    public @Nonnull ElasticIP getIpAddressSupport() {
        return new ElasticIP(getProvider());
    }

    @Override
    public @Nullable ElasticLoadBalancer getLoadBalancerSupport() {
        if( getProvider().getEC2Provider().isAWS() || getProvider().getEC2Provider().isEnStratus() ) {
            return new ElasticLoadBalancer(getProvider());
        }
        return null;
    }

    @Override
    public @Nullable NetworkACL getNetworkFirewallSupport() {
        if( getProvider().getEC2Provider().isAWS() ) {
            return new NetworkACL(getProvider());
        }
        return null;
    }

    @Override
    public @Nullable VPC getVlanSupport() {
        if( getProvider().getEC2Provider().isAWS() || getProvider().getEC2Provider().isEnStratus() ) {
            return new VPC(getProvider());
        }
        return null;
    }
}
