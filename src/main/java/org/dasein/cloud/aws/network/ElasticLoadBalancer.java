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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.*;
import org.dasein.cloud.util.APITrace;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ElasticLoadBalancer extends AbstractLoadBalancerSupport<AWSCloud> {
    static private final Logger logger = Logger.getLogger(ElasticLoadBalancer.class);
    
    private AWSCloud provider = null;
    private volatile transient ElasticLoadBalancerCapabilities capabilities;

    ElasticLoadBalancer(AWSCloud provider) {
        super(provider);
        this.provider = provider;
    }
    
    @Override
    public void addDataCenters(@Nonnull String toLoadBalancerId, @Nonnull String ... availabilityZoneIds) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.addDataCenters");
        try {
            //noinspection ConstantConditions
            if( availabilityZoneIds != null && availabilityZoneIds.length > 0 ) {
                ProviderContext ctx = provider.getContext();

                if( ctx == null ) {
                    throw new CloudException("No valid context is established for this request");
                }
                Map<String,String> parameters = getELBParameters(getContext(), ELBMethod.ENABLE_AVAILABILITY_ZONES);
                ELBMethod method;

                parameters.put("LoadBalancerName", toLoadBalancerId);
                int i = 1;
                for( String zoneId : availabilityZoneIds ) {
                    parameters.put("AvailabilityZones.member." + (i++), zoneId);
                }
                method = new ELBMethod(provider, ctx, parameters);
                try {
                    method.invoke();
                }
                catch( EC2Exception e ) {
                    logger.error(e.getSummary());
                    throw new CloudException(e);
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void addServers(@Nonnull String toLoadBalancerId, @Nonnull String ... instanceIds) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.addServers");
        try {
            //noinspection ConstantConditions
            if( instanceIds != null && instanceIds.length > 0 ) {
                ProviderContext ctx = provider.getContext();

                if( ctx == null ) {
                    throw new CloudException("No valid context is established for this request");
                }
                Map<String,String> parameters = getELBParameters(getContext(), ELBMethod.REGISTER_INSTANCES);
                ELBMethod method;

                LoadBalancer lb = getLoadBalancer(toLoadBalancerId);

                if( lb == null ) {
                    throw new CloudException("No such load balancer: " + toLoadBalancerId);
                }
                LbListener[] listeners = lb.getListeners();

                //noinspection ConstantConditions
                if( listeners == null ) {
                    throw new CloudException("The load balancer " + toLoadBalancerId + " is improperly configered.");
                }
                parameters.put("LoadBalancerName", toLoadBalancerId);
                int i = 1;
                for( String instanceId : instanceIds ) {
                    parameters.put("Instances.member." + (i++) + ".InstanceId", instanceId);
                }
                method = new ELBMethod(provider, ctx, parameters);
                try {
                    method.invoke();
                }
                catch( EC2Exception e ) {
                    throw new CloudException(e);
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String createLoadBalancer(@Nonnull LoadBalancerCreateOptions options) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.createLoadBalancer");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No valid context is established for this request");
            }
            Map<String,String> parameters = getELBParameters(provider.getContext(), ELBMethod.CREATE_LOAD_BALANCER);
            ELBMethod method;
            NodeList blocks;
            Document doc;

            if( options.getProviderIpAddressId() != null ) {
                throw new OperationNotSupportedException(getProvider().getCloudName() + " does not support assignment of IP addresses to load balancers.");
            }
            String name = verifyName(options.getName());
            parameters.put("LoadBalancerName", name);
            int i = 1;
            for( LbListener listener : options.getListeners() ) {
                switch( listener.getNetworkProtocol() ) {
                    case HTTP: parameters.put("Listeners.member." + i + ".Protocol", "HTTP"); break;
                    case RAW_TCP: parameters.put("Listeners.member." + i + ".Protocol", "TCP"); break;
                    default: throw new CloudException("Invalid protocol: " + listener.getNetworkProtocol());
                }
                parameters.put("Listeners.member." + i + ".LoadBalancerPort", String.valueOf(listener.getPublicPort()));
                parameters.put("Listeners.member." + i + ".InstancePort", String.valueOf(listener.getPrivatePort()));
                i++;
            }
            i = 1;
            for( String zoneId : options.getProviderDataCenterIds() ) {
                parameters.put("AvailabilityZones.member." + (i++), zoneId);
            }
            i = 1;
            for( String subnetId : options.getProviderSubnetIds() ) {
              parameters.put("Subnets.member." + (i++), subnetId);
            }

            if ( options.getType() != null && options.getType() == LbType.INTERNAL ) {
              parameters.put("Scheme", "internal");
            }
            method = new ELBMethod(provider, ctx, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("DNSName");
            if( blocks.getLength() > 0 ) {
                for( LoadBalancerEndpoint endpoint : options.getEndpoints() ) {
                    if( endpoint.getEndpointType().equals(LbEndpointType.VM) ) {
                        addServers(name, endpoint.getEndpointValue());
                    }
                    else {
                        addIPEndpoints(name, endpoint.getEndpointValue());
                    }
                }
                return name;
            }
            throw new CloudException("Unable to create a load balancer and no error message from the cloud.");
        }
        finally {
            APITrace.end();
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    @Deprecated
    public @Nonnull String create(@Nonnull String name, @Nonnull String description, @Nullable String addressId, @Nullable String[] zoneIds, @Nullable LbListener[] listeners, @Nullable String[] serverIds, @Nullable String[] subnetIds, @Nullable LbType type) throws CloudException, InternalException {
        LoadBalancerCreateOptions options = LoadBalancerCreateOptions.getInstance(name, description);

        if( zoneIds != null && zoneIds.length > 0 ) {
            options.limitedTo(zoneIds);
        }
        if( listeners != null && listeners.length > 0 ) {
            options.havingListeners(listeners);
        }
        if( serverIds != null && serverIds.length > 0 ) {
            options.withVirtualMachines(serverIds);
        }
        if( subnetIds != null && subnetIds.length > 0 ) {
            options.withProviderSubnetIds(subnetIds);
        }
        if(type != null){
          options.asType( type );
        }
        return createLoadBalancer(options);
    }

    @Override
    public @Nonnull LoadBalancerAddressType getAddressType() {
        return LoadBalancerAddressType.DNS;
    }

    @Nonnull
    @Override
    public LoadBalancerCapabilities getCapabilities() throws CloudException, InternalException {
        if( capabilities == null ) {
            capabilities = new ElasticLoadBalancerCapabilities(provider);
        }
        return capabilities;
    }

    private @Nonnull Map<String,String> getELBParameters(@Nonnull ProviderContext ctx, @Nonnull String action) throws InternalException {
        APITrace.begin(provider, "LB.getELBParameters");
        try {
            HashMap<String,String> parameters = new HashMap<String,String>();

            parameters.put(AWSCloud.P_ACTION, action);
            parameters.put(AWSCloud.P_SIGNATURE_VERSION, AWSCloud.SIGNATURE);
            try {
                parameters.put(AWSCloud.P_ACCESS, new String(ctx.getAccessPublic(), "utf-8"));
            }
            catch( UnsupportedEncodingException e ) {
                logger.error(e);
                e.printStackTrace();
                throw new InternalException(e);
            }
            parameters.put(AWSCloud.P_SIGNATURE_METHOD, AWSCloud.EC2_ALGORITHM);
            parameters.put(AWSCloud.P_TIMESTAMP, provider.getTimestamp(System.currentTimeMillis(), true));
            parameters.put(AWSCloud.P_VERSION, provider.getElbVersion());
            return parameters;
        }
        finally {
            APITrace.end();
        }
    }
    
    public int getMaxPublicPorts() {
        return 0;
    }
    
    @Override
    public @Nullable LoadBalancer getLoadBalancer(@Nonnull String loadBalancerId) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.getLoadBalancer");
        try {
            Map<String,String> parameters = getELBParameters(getContext(), ELBMethod.DESCRIBE_LOAD_BALANCERS);
            ELBMethod method;
            NodeList blocks;
            Document doc;

            if( loadBalancerId.length() > 32 ) {
                return null;
            }
            parameters.put("LoadBalancerNames.member.1", loadBalancerId);
            method = new ELBMethod(provider, getContext(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && code.equals("LoadBalancerNotFound") ) {
                    return null;
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("LoadBalancerDescriptions");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("member") ) {
                        LoadBalancer loadBalancer = toLoadBalancer(item);

                        if( loadBalancer != null ) {
                            return loadBalancer;
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
    
    static private volatile List<LbAlgorithm> algorithms;
    
    @Override
    public @Nonnull Iterable<LbAlgorithm> listSupportedAlgorithms() {
        if( algorithms == null ) {
            List<LbAlgorithm> list = new ArrayList<LbAlgorithm>();

            list.add(LbAlgorithm.ROUND_ROBIN);
            algorithms = Collections.unmodifiableList(list);
        }
        return algorithms;
    }

    @Override
    public @Nonnull Iterable<LbEndpointType> listSupportedEndpointTypes() throws CloudException, InternalException {
        return Collections.singletonList(LbEndpointType.VM);
    }

    static private volatile List<IPVersion> versions;
    
    @Override
    public @Nonnull Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        if( versions == null ) {
            ArrayList<IPVersion> tmp = new ArrayList<IPVersion>();
            
            tmp.add(IPVersion.IPV4);
            tmp.add(IPVersion.IPV6);
            versions = Collections.unmodifiableList(tmp);
        }
        return versions;
    }

    @Override
    public @Nonnull Iterable<LbPersistence> listSupportedPersistenceOptions() throws CloudException, InternalException {
        return Collections.singletonList(LbPersistence.NONE);
    }

    static private volatile List<LbProtocol> protocols;
    
    @Override
    public @Nonnull Iterable<LbProtocol> listSupportedProtocols() {
        if( protocols == null ) {
            List<LbProtocol> list = new ArrayList<LbProtocol>();

            list.add(LbProtocol.HTTP);
            list.add(LbProtocol.RAW_TCP);
            protocols = Collections.unmodifiableList(list);
        }
        return protocols;
    }
    
    public @Nonnull String getProviderTermForLoadBalancer(@Nonnull Locale locale) {
        return "load balancer";
    }

    @Override
    public @Nonnull Requirement identifyEndpointsOnCreateRequirement() throws CloudException, InternalException {
        return Requirement.OPTIONAL;
    }

    @Override
    public @Nonnull Requirement identifyListenersOnCreateRequirement() throws CloudException, InternalException {
        return Requirement.REQUIRED;
    }

    @Override
    public boolean isAddressAssignedByProvider() {
        return true;
    }

    @Override
    public boolean isDataCenterLimited() {
        return true;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(provider, "LB.isSubscribed");
        try {
            try {
                ProviderContext ctx = provider.getContext();

                if( ctx == null ) {
                    throw new CloudException("No valid context is established for this request");
                }
                if( !provider.getEC2Provider().isAWS() ) {
                    return false;
                }
                Map<String,String> parameters = getELBParameters(provider.getContext(), ELBMethod.DESCRIBE_LOAD_BALANCERS);
                ELBMethod method;

                method = new ELBMethod(provider, ctx, parameters);
                try {
                    method.invoke();
                    return true;
                }
                catch( EC2Exception e ) {
                    if( e.getStatus() == HttpServletResponse.SC_UNAUTHORIZED || e.getStatus() == HttpServletResponse.SC_FORBIDDEN ) {
                        return false;
                    }
                    String code = e.getCode();

                    if( code != null && (code.equals("SubscriptionCheckFailed") || code.equals("AuthFailure") || code.equals("SignatureDoesNotMatch") || code.equals("InvalidClientTokenId") || code.equals("OptInRequired")) ) {
                        return false;
                    }
                    throw new CloudException(e);
                }
            }
            catch( RuntimeException e ) {
                logger.error("Could not check subscription status: " + e.getMessage());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new InternalException(e);
            }
            catch( Error e ) {
                logger.error("Could not check subscription status: " + e.getMessage());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new InternalException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<LoadBalancerEndpoint> listEndpoints(@Nonnull String loadBalancerId) throws CloudException, InternalException {
        //noinspection RedundantArrayCreation
        return listEndpoints(loadBalancerId, LbEndpointType.VM, new String[0]);
    }

    @Override
    public @Nonnull Iterable<LoadBalancerEndpoint> listEndpoints(@Nonnull String loadBalancerId, @Nonnull LbEndpointType type, @Nonnull String ... endpoints) throws CloudException, InternalException {
        if( !type.equals(LbEndpointType.VM) ) {
            return Collections.emptyList();
        }
        APITrace.begin(provider, "LB.listEndpoints");
        try {
            ArrayList<LoadBalancerEndpoint> list = new ArrayList<LoadBalancerEndpoint>();
            Map<String,String> parameters = getELBParameters(getContext(), ELBMethod.DESCRIBE_INSTANCE_HEALTH);
            ELBMethod method;
            NodeList blocks;
            Document doc;

            parameters.put("LoadBalancerName", loadBalancerId );
            if( endpoints.length > 0 ) {
                for ( int i = 0; i < endpoints.length; i++ ) {
                    parameters.put( "Instances.member." + (i + 1) + ".InstanceId", endpoints[i] );
                }
            }
            method = new ELBMethod( provider, getContext(), parameters );
            try {
                doc = method.invoke();
            }
            catch ( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("InstanceStates");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("member") ) {
                        LoadBalancerEndpoint ep = toEndpoint(item);
                        if( ep != null ) {
                            list.add(ep);
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
    public boolean supportsMonitoring() {
        return true;
    }

    @Override
    public boolean supportsMultipleTrafficTypes() throws CloudException, InternalException {
        return true;
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listLoadBalancerStatus() throws CloudException, InternalException {
        APITrace.begin(provider, "LB.listLoadBalancerStatus");
        try {
            if(!provider.getEC2Provider().isAWS() ) {
                return Collections.emptyList();
            }

            ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();
            Map<String,String> parameters = getELBParameters(getContext(), ELBMethod.DESCRIBE_LOAD_BALANCERS);
            ELBMethod method;
            NodeList blocks;
            Document doc;

            method = new ELBMethod(provider, getContext(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("LoadBalancerDescriptions");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("member") ) {
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
    public @Nonnull Iterable<LoadBalancer> listLoadBalancers() throws CloudException, InternalException {
        APITrace.begin(provider, "LB.listLoadBalancers");
        try {
            if(!provider.getEC2Provider().isAWS() ) {
                return Collections.emptyList();
            }

            ArrayList<LoadBalancer> list = new ArrayList<LoadBalancer>();
            Map<String,String> parameters = getELBParameters(getContext(), ELBMethod.DESCRIBE_LOAD_BALANCERS);
            ELBMethod method;
            NodeList blocks;
            Document doc;

            method = new ELBMethod(provider, getContext(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("LoadBalancerDescriptions");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("member") ) {
                        LoadBalancer loadBalancer = toLoadBalancer(item);

                        if( loadBalancer != null ) {
                            list.add(loadBalancer);
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

    @SuppressWarnings("deprecation")
    @Override
    @Deprecated
    public @Nonnull Iterable<LoadBalancerServer> getLoadBalancerServerHealth(@Nonnull String loadBalancerId) throws CloudException, InternalException {
        return getLoadBalancerServerHealth(loadBalancerId, new String[0]);
    }

    @SuppressWarnings("deprecation")
    @Override
    @Deprecated
    public @Nonnull Iterable<LoadBalancerServer> getLoadBalancerServerHealth(@Nonnull String loadBalancerId, @Nonnull String ... serverIdsToCheck ) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.getLoadBalancerServerHealth");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No valid context is established for this request");
            }

            ArrayList<LoadBalancerServer> list = new ArrayList<LoadBalancerServer>();
            Map<String,String> parameters = getELBParameters(provider.getContext(), ELBMethod.DESCRIBE_INSTANCE_HEALTH);
            ELBMethod method;
            NodeList blocks;
            Document doc;

            parameters.put( "LoadBalancerName", loadBalancerId );
            if ( serverIdsToCheck != null && serverIdsToCheck.length > 0 ) {
                for ( int i = 0; i < serverIdsToCheck.length; i++ ) {
                    parameters.put( "Instances.member." + (i + 1) + ".InstanceId", serverIdsToCheck[i] );
                }
            }
            method = new ELBMethod( provider, ctx, parameters );
            try {
                doc = method.invoke();
            }
            catch ( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("InstanceStates");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("member") ) {
                        LoadBalancerServer loadBalancerServer = toLoadBalancerServer( ctx, loadBalancerId, item );
                        if( loadBalancerServer != null ) {
                            list.add(loadBalancerServer);
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
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        if( action.equals(LoadBalancerSupport.ANY) ) {
             return new String[] { ELBMethod.ELB_PREFIX + "*" };
        }
        else if( action.equals(LoadBalancerSupport.ADD_DATA_CENTERS) ) {
            return new String[] { ELBMethod.ELB_PREFIX + ELBMethod.ENABLE_AVAILABILITY_ZONES };
        }
        else if( action.equals(LoadBalancerSupport.ADD_VMS) ) {
            return new String[] { ELBMethod.ELB_PREFIX + ELBMethod.REGISTER_INSTANCES };
        }
        else if( action.equals(LoadBalancerSupport.CREATE_LOAD_BALANCER) ) {
            return new String[] { ELBMethod.ELB_PREFIX + ELBMethod.CREATE_LOAD_BALANCER };
        }
        else if( action.equals(LoadBalancerSupport.GET_LOAD_BALANCER) || action.equals(LoadBalancerSupport.LIST_LOAD_BALANCER) ) {
            return new String[] { ELBMethod.ELB_PREFIX + ELBMethod.DESCRIBE_LOAD_BALANCERS };
        }
        else if( action.equals(LoadBalancerSupport.GET_LOAD_BALANCER_SERVER_HEALTH) ) {
            return new String[] { ELBMethod.ELB_PREFIX + ELBMethod.DESCRIBE_INSTANCE_HEALTH };
        }
        else if( action.equals(LoadBalancerSupport.REMOVE_DATA_CENTERS) ) {
            return new String[] { ELBMethod.ELB_PREFIX + ELBMethod.DISABLE_AVAILABILITY_ZONES };
        }
        else if( action.equals(LoadBalancerSupport.REMOVE_LOAD_BALANCER) ) {
            return new String[] { ELBMethod.ELB_PREFIX + ELBMethod.DELETE_LOAD_BALANCER };
        }
        else if( action.equals(LoadBalancerSupport.REMOVE_VMS) ) {
            return new String[] { ELBMethod.ELB_PREFIX + ELBMethod.DEREGISTER_INSTANCES };            
        }
        return new String[0]; 
    }

    @SuppressWarnings("deprecation")
    @Override
    @Deprecated
    public void remove(@Nonnull String loadBalancerId) throws CloudException, InternalException {
        removeLoadBalancer(loadBalancerId);
    }

    @Override
    public void removeLoadBalancer(@Nonnull String loadBalancerId) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.remove");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No valid context is established for this request");
            }
            Map<String,String> parameters = getELBParameters(getContext(), ELBMethod.DELETE_LOAD_BALANCER);
            ELBMethod method;

            parameters.put("LoadBalancerName", loadBalancerId);
            method = new ELBMethod(provider, ctx, parameters);
            try {
                method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeDataCenters(@Nonnull String toLoadBalancerId, @Nonnull String ... availabilityZoneIds) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.removeDataCenters");
        try {
            //noinspection ConstantConditions
            if( availabilityZoneIds != null && availabilityZoneIds.length > 0 ) {
                ProviderContext ctx = provider.getContext();

                if( ctx == null ) {
                    throw new CloudException("No valid context is established for this request");
                }
                Map<String,String> parameters = getELBParameters(getContext(), ELBMethod.DISABLE_AVAILABILITY_ZONES);
                ELBMethod method;

                parameters.put("LoadBalancerName", toLoadBalancerId);
                int i = 1;
                for( String zoneId : availabilityZoneIds ) {
                    parameters.put("AvailabilityZones.member." + (i++), zoneId);
                }
                method = new ELBMethod(provider, ctx, parameters);
                try {
                    method.invoke();
                }
                catch( EC2Exception e ) {
                    logger.error(e.getSummary());
                    throw new CloudException(e);
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeServers(@Nonnull String toLoadBalancerId, @Nonnull String ... instanceIds) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.removeServers");
        try {
            //noinspection ConstantConditions
            if( instanceIds != null && instanceIds.length > 0 ) {
                ProviderContext ctx = provider.getContext();

                if( ctx == null ) {
                    throw new CloudException("No valid context is established for this request");
                }
                Map<String,String> parameters = getELBParameters(getContext(), ELBMethod.DEREGISTER_INSTANCES);
                ELBMethod method;

                parameters.put("LoadBalancerName", toLoadBalancerId);
                int i = 1;
                for( String instanceId : instanceIds ) {
                    parameters.put("Instances.member." + (i++) + ".InstanceId", instanceId);
                }
                method = new ELBMethod(provider, ctx, parameters);
                try {
                    method.invoke();
                }
                catch( EC2Exception e ) {
                    logger.error(e.getSummary());
                    throw new CloudException(e);
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean supportsAddingEndpoints() throws CloudException, InternalException {
        return true;
    }

    @Override
    public LoadBalancerHealthCheck createLoadBalancerHealthCheck(@Nonnull LBHealthCheckCreateOptions options)throws CloudException, InternalException{
        APITrace.begin(provider, "LB.configureHealthCheck");
        try{
            ProviderContext ctx = provider.getContext();
            if(ctx == null){
                throw new CloudException("No valid context is established for this request");
            }

            NodeList blocks;
            Document doc;
            Map<String, String> parameters = getELBParameters(getContext(), ELBMethod.CONFIGURE_HEALTH_CHECK);
            ELBMethod method;

            parameters.put("LoadBalancerName", options.getProviderLoadBalancerId());
            parameters.put("HealthCheck.HealthyThreshold", options.getHealthyCount() + "");
            parameters.put("HealthCheck.UnhealthyThreshold", options.getUnhealthyCount() + "");
            String path = "/";
            if(options.getPort() == 0 ){
                throw new CloudException("Port must have a number between 1 and 65535.");
            }
            if(options.getPath() != null || !options.getPath().equals("")){
                path = options.getPath();
            }
            parameters.put("HealthCheck.Target", options.getProtocol().name() + ":" + options.getPort() + path);
            parameters.put("HealthCheck.Interval", options.getInterval().intValue() + "");
            parameters.put("HealthCheck.Timeout", options.getTimeout().intValue() + "");

            method = new ELBMethod(provider, ctx, parameters);
            try{
                doc = method.invoke();
            }
            catch(EC2Exception e){
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("HealthCheck");
            if( blocks.getLength() > 0 ) {
                return toLBHealthCheck(blocks.item(0));
            }
            throw new CloudException("An error occurred while configuring the Health Check.");
        }
        finally{
            APITrace.end();
        }
    }

//TODO: Get instance health
//TODO: remove set as NoOp

    @Override
    public boolean healthCheckRequiresLoadBalancer(){
        return true;
    }

    private LoadBalancerHealthCheck toLBHealthCheck(Node node){
        NodeList attrs = node.getChildNodes();
        Double interval = 0.0;
        LoadBalancerHealthCheck.HCProtocol protocol = null;
        int port = 0;
        String path = "";
        int healthyCount = 0;
        int unHealthyCount = 0;
        Double timeout = 0.0;

        for(int i=0;i<attrs.getLength();i++){
            Node attr = attrs.item(i);
            String name = attr.getNodeName().toLowerCase();

            if(name.equals("interval")){
                interval = Double.valueOf(attr.getFirstChild().getNodeValue());
            }
            else if(name.equals("target")){
                String targetString = attr.getFirstChild().getNodeValue();
                String[] parts = targetString.split(":");
                protocol = LoadBalancerHealthCheck.HCProtocol.valueOf(parts[0]);
                if(parts[1].endsWith("/")){
                    port = Integer.parseInt(parts[1].substring(0, parts[1].length()-1));
                    path = "/";
                }
                else{
                    String[] parts2 = parts[1].split("/");
                    port = Integer.parseInt(parts2[0]);
                    path = "/" + parts2[1];
                }
            }
            else if(name.equals("healthythreshold")){
                healthyCount = Integer.parseInt(attr.getFirstChild().getNodeValue());
            }
            else if(name.equals("unhealthythreshold")){
                unHealthyCount = Integer.parseInt(attr.getFirstChild().getNodeValue());
            }
            else if(name.equals("timeout")){
                timeout = Double.valueOf(attr.getFirstChild().getNodeValue());
            }
        }
        return LoadBalancerHealthCheck.getInstance(protocol, port, path, interval, timeout, healthyCount, unHealthyCount);
    }

    private LbListener toListener(Node node) {
        NodeList attrs = node.getChildNodes();

        LbProtocol protocol = LbProtocol.RAW_TCP;
        int publicPort = 0;
        int privatePort = 0;

        for( int i=0; i<attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;
            
            name = attr.getNodeName().toLowerCase();
            if( name.equals("protocol") ) {
                protocol = toProtocol(attr.getFirstChild().getNodeValue());
            }
            else if( name.equals("loadbalancerport") ) {
                publicPort = Integer.parseInt(attr.getFirstChild().getNodeValue());
            }
            else if( name.equals("instanceport") ) {
                privatePort = Integer.parseInt(attr.getFirstChild().getNodeValue());
            }
        }
        return LbListener.getInstance(protocol,  publicPort, privatePort);
    }
    
    private @Nullable LoadBalancer toLoadBalancer(@Nullable Node node) throws CloudException, InternalException {
        if( node == null ) {
            return null;
        }
        ArrayList<LbListener> listenerList = new ArrayList<LbListener>();
        ArrayList<Integer> portList = new ArrayList<Integer>();
        ArrayList<String> zoneList = new ArrayList<String>();
        ArrayList<String> serverIds = new ArrayList<String>();
        String regionId = getContext().getRegionId();
        String lbName = null, description = null, lbId = null, cname = null;
        long created = 0L;
        LbType type = null;
        ArrayList<String> subnetList = new ArrayList<String>();

        if( regionId == null ) {
            throw new CloudException("No region was set for this context");
        }
        NodeList attrs = node.getChildNodes();

        for( int i=0; i<attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;
            
            name = attr.getNodeName().toLowerCase();
            if( name.equals("listenerdescriptions") ) {
                if( attr.hasChildNodes() ) {
                    NodeList listeners = attr.getChildNodes();
                
                    if( listeners.getLength() > 0 ) {
                        for( int j=0; j<listeners.getLength(); j++ ) {
                          Node item = listeners.item(j);
                          if ( item.getNodeName().equals( "member" ) ) {
                            NodeList listenerMembers = item.getChildNodes();
                            for ( int k = 0; k < listenerMembers.getLength(); k++ ) {
                              Node listenerItem = listenerMembers.item( k );
                              if ( listenerItem.getNodeName().equals( "Listener" ) ) {
                                LbListener l = toListener( listenerItem );

                                if ( l != null ) {
                                  listenerList.add( l );
                                  portList.add( l.getPublicPort() );
                                }
                              }
                            }
                          }
                        }
                    }
                }
            }
            else if( name.equals("loadbalancername") ) {
                lbName = attr.getFirstChild().getNodeValue();
                description = attr.getFirstChild().getNodeValue();
                lbId = attr.getFirstChild().getNodeValue();
            }
            else if( name.equals("instances") ) {
                if( attr.hasChildNodes() ) {
                    NodeList instances = attr.getChildNodes();
                
                    if( instances.getLength() > 0 ) {
                        for( int j=0; j<instances.getLength(); j++ ) {
                            Node instance = instances.item(j);
                            
                            if( instance.getNodeName().equalsIgnoreCase("member") ) {
                                if( instance.hasChildNodes() ) {
                                    NodeList idList = instance.getChildNodes();
                                    
                                    for( int k=0; k<idList.getLength(); k++ ) {
                                        Node n = idList.item(k);
                                        
                                        if( n.getNodeName().equalsIgnoreCase("instanceid") ) {
                                            serverIds.add(n.getFirstChild().getNodeValue());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            else if( name.equals("createdtime") ) {
                try {
                    created = provider.parseTime(attr.getFirstChild().getNodeValue());
                }
                catch( CloudException e ) {
                    logger.warn("Unable to parse time: " + e.getMessage());
                }
            }
            else if( name.equals("healthcheck") ) {
                // unsupported
            }
            else if( name.equals("dnsname") ) {
                cname = attr.getFirstChild().getNodeValue();
            }
            else if( name.equals("availabilityzones") ) {
                if( attr.hasChildNodes() ) {
                    NodeList zones = attr.getChildNodes();
                
                    if( zones.getLength() > 0 ) {
                        for( int j=0; j<zones.getLength(); j++ ) {
                            Node zone = zones.item(j);
                            
                            if( zone.hasChildNodes() ) {
                                zoneList.add(zone.getFirstChild().getNodeValue());
                            }
                        }
                    }
                }                
            }
            else if( name.equals("scheme") ) {
              if ( "internal".equals( provider.getTextValue( attr ) ) ) {
                type = LbType.INTERNAL;
              }
            }
            else if( name.equals("subnets") ) {
              if( attr.hasChildNodes() ) {
                NodeList subnets = attr.getChildNodes();

                if( subnets.getLength() > 0 ) {
                  for( int j=0; j<subnets.getLength(); j++ ) {
                    Node subnet = subnets.item(j);

                    if( subnet.hasChildNodes() ) {
                      subnetList.add( provider.getTextValue( subnet ) );
                    }
                  }
                }
              }
            }

        }
        if( lbId == null || cname == null ) {
            return null;
        }
        if( lbName == null ) {
            lbName= lbId + " (" + cname + ")";
        }
        if( description == null ) {
            description = lbName;
        }
        int[] ports = new int[portList.size()];
        int i =0;

        for( Integer p : portList ) {
            ports[i++] = p;
        }
        LoadBalancer lb = LoadBalancer.getInstance(getContext().getAccountNumber(), regionId, lbId, LoadBalancerState.ACTIVE, lbName, description, LoadBalancerAddressType.DNS, cname, ports).supportingTraffic(IPVersion.IPV4, IPVersion.IPV6).createdAt(created);

        if( !serverIds.isEmpty() ) {
            //noinspection deprecation
            lb.setProviderServerIds(serverIds.toArray(new String[serverIds.size()]));
        }
        if( !listenerList.isEmpty() ) {
            lb.withListeners(listenerList.toArray(new LbListener[listenerList.size()]));
        }
        if( !zoneList.isEmpty() ) {
            lb.operatingIn(zoneList.toArray(new String[zoneList.size()]));
        }
        if ( type != null ) {
            lb.setType( type );
        }
        if( !subnetList.isEmpty() ) {
          lb.withProviderSubnetIds(subnetList.toArray(new String[subnetList.size()]));
        }
        return lb;
    }
    
    private LbProtocol toProtocol(String txt) {
        if( txt.equals("HTTP") ) {
          return LbProtocol.HTTP;
        }
        if( txt.equals("HTTPS") ) {
          return LbProtocol.HTTPS;
        }
        else {
            return LbProtocol.RAW_TCP;
        }
    }

    private @Nullable LoadBalancerEndpoint toEndpoint(@Nullable Node node ) throws CloudException, InternalException {
        if ( node == null ) {
            return null;
        }
        String reason = null, description = null, vmId = null;
        LbEndpointState state = LbEndpointState.ACTIVE;
        NodeList attrs = node.getChildNodes();

        for ( int i = 0; i < attrs.getLength(); i++ ) {
            Node attr = attrs.item( i );
            String name;

            name = attr.getNodeName().toLowerCase();
            if ( name.equals( "instanceid" ) && attr.hasChildNodes() ) {
                vmId = attr.getFirstChild().getNodeValue().trim();
            }
            else if ( name.equals( "state" ) && attr.hasChildNodes() ) {
                if( attr.getFirstChild().getNodeValue().trim().equalsIgnoreCase("InService") ) {
                    state = LbEndpointState.ACTIVE;
                }
                else {
                    state = LbEndpointState.INACTIVE;
                }
            }
            else if ( name.equals( "description" ) && attr.hasChildNodes() ) {
                String value = attr.getFirstChild().getNodeValue().trim();

                if ( !"N/A".equals( value ) ) {
                    description = value;
                }
            }
            else if ( name.equals( "reasoncode" ) && attr.hasChildNodes() ) {
                String value = attr.getFirstChild().getNodeValue().trim();

                if ( !"N/A".equals( value ) ) {
                    reason =  attr.getFirstChild().getNodeValue();
                }
            }
        }
        if( vmId == null ) {
            return null;
        }
        if( description == null ) {
            description = state.toString();
        }
        if( reason == null ) {
            reason = description;
        }
        return LoadBalancerEndpoint.getInstance(LbEndpointType.VM, vmId, state, reason, description);
    }

    private @Nullable LoadBalancerServer toLoadBalancerServer( @Nonnull ProviderContext ctx, @Nullable String loadBalancerId, @Nullable Node node ) {
        if ( node == null ) {
            return null;
        }
        LoadBalancerServer loadBalancerServer = new LoadBalancerServer();
        NodeList attrs = node.getChildNodes();

        loadBalancerServer.setProviderOwnerId( ctx.getAccountNumber() );
        loadBalancerServer.setProviderRegionId( ctx.getRegionId() );
        loadBalancerServer.setProviderLoadBalancerId( loadBalancerId );

        for ( int i = 0; i < attrs.getLength(); i++ ) {
            Node attr = attrs.item( i );
            String name;

            name = attr.getNodeName().toLowerCase();
            if ( name.equals( "instanceid" ) ) {
                loadBalancerServer.setProviderServerId( attr.getFirstChild().getNodeValue() );
            }
            else if ( name.equals( "state" ) ) {
                loadBalancerServer.setCurrentState( toServerState( attr.getFirstChild().getNodeValue() ) );
            }
            else if ( name.equals( "description" ) ) {
                String value = attr.getFirstChild().getNodeValue();
                if ( !"N/A".equals( value ) ) {
                    loadBalancerServer.setCurrentStateDescription( value );
                }
            }
            else if ( name.equals( "reasoncode" ) ) {
                String value = attr.getFirstChild().getNodeValue();
                if ( !"N/A".equals( value ) ) {
                    loadBalancerServer.setCurrentStateReason( attr.getFirstChild().getNodeValue() );
                }
            }
        }

        return loadBalancerServer;
    }

    private LoadBalancerServerState toServerState( String txt ) {
        if ( txt.equals( "InService" ) ) {
            return LoadBalancerServerState.ACTIVE;
        }
        else {
            return LoadBalancerServerState.INACTIVE;
        }
    }

    private @Nullable ResourceStatus toStatus(@Nullable Node node) {
        if( node == null ) {
            return null;
        }
        NodeList attrs = node.getChildNodes();
        String lbId = null;

        for( int i=0; i<attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;

            name = attr.getNodeName().toLowerCase();
            if( name.equals("loadbalancername") ) {
                lbId = attr.getFirstChild().getNodeValue();
                break;
            }
        }
        if( lbId == null ) {
            return null;
        }
        return new ResourceStatus(lbId, LoadBalancerState.ACTIVE);
    }

    private String verifyName(String name) {
        StringBuilder str = new StringBuilder();
        
        for( int i=0; i<name.length(); i++ ) {
            char c = name.charAt(i);
            
            if( Character.isLetterOrDigit(c) ) {
                str.append(c);
            }
            else if( c == '-' && i > 0 ) {
                str.append(c);
            }
        }
        name = str.toString();
        if( name.length() > 32 ) {
            name = name.substring(0,32);
        }
        while( name.charAt(name.length()-1) == '-' ) {
            name = name.substring(0, name.length()-1);
        }
        return name;
    }
}
