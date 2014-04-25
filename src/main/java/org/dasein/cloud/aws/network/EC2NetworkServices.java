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

import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.network.AbstractNetworkServices;
import org.dasein.cloud.network.DNSSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class EC2NetworkServices extends AbstractNetworkServices {
    private AWSCloud cloud;
    
    public EC2NetworkServices(AWSCloud cloud) { this.cloud = cloud; }
    
    @Override
    public @Nullable DNSSupport getDnsSupport() {
        if( cloud.getEC2Provider().isAWS() || cloud.getEC2Provider().isEnStratus() ) {
            return new Route53(cloud);
        }
        return null;
    }
    
    @Override
    public @Nonnull SecurityGroup getFirewallSupport() {
        return new SecurityGroup(cloud);
    }
    
    @Override
    public @Nonnull ElasticIP getIpAddressSupport() {
        return new ElasticIP(cloud);
    }

    @Override
    public @Nullable ElasticLoadBalancer getLoadBalancerSupport() {
        if( cloud.getEC2Provider().isAWS() || cloud.getEC2Provider().isEnStratus() ) {
            return new ElasticLoadBalancer(cloud);
        }
        return null;
    }

    @Override
    public @Nullable NetworkACL getNetworkFirewallSupport() {
        if( cloud.getEC2Provider().isAWS() ) {
            return new NetworkACL(cloud);
        }
        return null;
    }

    @Override
    public @Nullable VPC getVlanSupport() {
        if( cloud.getEC2Provider().isAWS() || cloud.getEC2Provider().isEnStratus() ) {
            return new VPC(cloud);
        }
        return null;
    }

}
