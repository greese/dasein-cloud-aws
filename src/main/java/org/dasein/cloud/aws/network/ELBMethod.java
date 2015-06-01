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

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.compute.EC2Method;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.LoadBalancerSupport;

import javax.annotation.Nonnull;
import java.util.Map;

public class ELBMethod extends EC2Method {
    static public final String ELB_PREFIX = "elasticloadbalancing:";

    static public final String SERVICE_ID = "elasticloadbalancing";

    static public final String CREATE_LOAD_BALANCER       = "CreateLoadBalancer";
    static public final String DELETE_LOAD_BALANCER       = "DeleteLoadBalancer";
    static public final String DEREGISTER_INSTANCES       = "DeregisterInstancesFromLoadBalancer";
    static public final String DESCRIBE_LOAD_BALANCERS    = "DescribeLoadBalancers";
    static public final String DESCRIBE_INSTANCE_HEALTH   = "DescribeInstanceHealth";
    static public final String DISABLE_AVAILABILITY_ZONES = "DisableAvailabilityZonesForLoadBalancer";
    static public final String ENABLE_AVAILABILITY_ZONES  = "EnableAvailabilityZonesForLoadBalancer";
    static public final String REGISTER_INSTANCES         = "RegisterInstancesWithLoadBalancer";
    static public final String CONFIGURE_HEALTH_CHECK     = "ConfigureHealthCheck";
    static public final String SET_LB_SSL_CERTIFICATE     = "SetLoadBalancerListenerSSLCertificate";
    static public final String CREATE_LOAD_BALANCER_LISTENERS = "CreateLoadBalancerListeners";
    static public final String DELETE_LOAD_BALANCER_LISTENERS     = "DeleteLoadBalancerListeners";
    static public final String ATTACH_LB_TO_SUBNETS       = "AttachLoadBalancerToSubnets";
    static public final String DETACH_LB_FROM_SUBNETS     = "DetachLoadBalancerFromSubnets";
    static public final String APPLY_SECURITY_GROUPS_TO_LOAD_BALANCER = "ApplySecurityGroupsToLoadBalancer";
    static public final String MODIFY_LOADBALANCER_ATTRIBUTES         = "ModifyLoadBalancerAttributes";
    static public final String DESCRIBE_LOADBALANCER_ATTRIBUTES         = "DescribeLoadBalancerAttributes";

    static public @Nonnull ServiceAction[] asELBServiceAction(@Nonnull String action) {
        if( action.equals(CREATE_LOAD_BALANCER) ) {
            return new ServiceAction[] { LoadBalancerSupport.CREATE_LOAD_BALANCER };
        }
        else if( action.equals(DELETE_LOAD_BALANCER) ) {
            return new ServiceAction[] { LoadBalancerSupport.REMOVE_LOAD_BALANCER };
        }
        else if( action.equals(DEREGISTER_INSTANCES) ) {
            return new ServiceAction[] { LoadBalancerSupport.REMOVE_VMS };
        }
        else if( action.equals(DESCRIBE_LOAD_BALANCERS) ) {
            return new ServiceAction[] { LoadBalancerSupport.GET_LOAD_BALANCER, LoadBalancerSupport.LIST_LOAD_BALANCER };
        }
        else if( action.equals(DESCRIBE_INSTANCE_HEALTH) ) {
            return new ServiceAction[] { LoadBalancerSupport.GET_LOAD_BALANCER_SERVER_HEALTH };
        }
        else if( action.equals(DISABLE_AVAILABILITY_ZONES) ) {
            return new ServiceAction[] { LoadBalancerSupport.REMOVE_DATA_CENTERS };
        }
        else if( action.equals(ENABLE_AVAILABILITY_ZONES) ) {
            return new ServiceAction[] { LoadBalancerSupport.ADD_DATA_CENTERS };
        }
        else if( action.equals(REGISTER_INSTANCES) ) {
            return new ServiceAction[] { LoadBalancerSupport.ADD_VMS };
        }
        else if( action.equals(CONFIGURE_HEALTH_CHECK) ) {
            return new ServiceAction[] { LoadBalancerSupport.CONFIGURE_HEALTH_CHECK };
        }
        else if ( action.equals(SET_LB_SSL_CERTIFICATE) ) {
            return new ServiceAction[] { LoadBalancerSupport.SET_LB_SSL_CERTIFICATE };
        }
        else if ( action.equals( CREATE_LOAD_BALANCER_LISTENERS ) ) {
            return new ServiceAction[] { LoadBalancerSupport.CREATE_LOAD_BALANCER_LISTENERS };
        }
        else if ( action.equals(DELETE_LOAD_BALANCER_LISTENERS) ) {
            return new ServiceAction[] { LoadBalancerSupport.DELETE_LOAD_BALANCER_LISTENERS };
        }
        else if ( action.equals(APPLY_SECURITY_GROUPS_TO_LOAD_BALANCER) ) {
            return new ServiceAction[]{ LoadBalancerSupport.SET_FIREWALLS};
        }
        else if( action.equals(MODIFY_LOADBALANCER_ATTRIBUTES)) {
            return new ServiceAction[]{ LoadBalancerSupport.MODIFY_LB_ATTRIBUTES};
        }
        else if(action.equals(DESCRIBE_LOADBALANCER_ATTRIBUTES)) {
            return new ServiceAction[] {LoadBalancerSupport.DESCRIBE_LOADBALANCER_ATTRIBUTES};
        }
        return new ServiceAction[0];
    }

    public ELBMethod(@Nonnull AWSCloud provider, @Nonnull ProviderContext ctx, @Nonnull Map<String, String> parameters) throws CloudException, InternalException {
        super(SERVICE_ID, provider, parameters);
    }

}
