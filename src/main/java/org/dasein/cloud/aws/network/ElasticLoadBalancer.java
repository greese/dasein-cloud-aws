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
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.*;
import org.dasein.cloud.util.APITrace;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ElasticLoadBalancer implements LoadBalancerSupport {
    static private final Logger logger = Logger.getLogger(ElasticLoadBalancer.class);
    
    private AWSCloud provider = null;
    
    ElasticLoadBalancer(AWSCloud provider) {
        this.provider = provider;
    }
    
    @Override
    public void addDataCenters(String toLoadBalancerId, String ... availabilityZoneIds) throws CloudException, InternalException {
        APITrace.begin(provider, "addDataCentersToLB");
        try {
            if( availabilityZoneIds != null && availabilityZoneIds.length > 0 ) {
                ProviderContext ctx = provider.getContext();

                if( ctx == null ) {
                    throw new CloudException("No valid context is established for this request");
                }
                Map<String,String> parameters = getELBParameters(provider.getContext(), ELBMethod.ENABLE_AVAILABILITY_ZONES);
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
    public void addServers(String toLoadBalancerId, String ... instanceIds) throws CloudException, InternalException {
        APITrace.begin(provider, "addServersToLB");
        try {
            if( instanceIds != null && instanceIds.length > 0 ) {
                ProviderContext ctx = provider.getContext();

                if( ctx == null ) {
                    throw new CloudException("No valid context is established for this request");
                }
                Map<String,String> parameters = getELBParameters(provider.getContext(), ELBMethod.REGISTER_INSTANCES);
                ELBMethod method;

                LoadBalancer lb = getLoadBalancer(toLoadBalancerId);
                LbListener[] listeners = lb.getListeners();

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
    public String create(String name, String description, String addressId, String[] zoneIds, LbListener[] listeners, String[] serverIds) throws CloudException, InternalException {
        APITrace.begin(provider, "createLoadBalancer");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No valid context is established for this request");
            }
            Map<String,String> parameters = getELBParameters(provider.getContext(), ELBMethod.CREATE_LOAD_BALANCER);
            ELBMethod method;
            NodeList blocks;
            Document doc;

            if( addressId != null ) {
                throw new CloudException("AWS does not support assignment of IP addresses to load balancers.");
            }
            name = verifyName(name);
            parameters.put("LoadBalancerName", name);
            int i = 1;
            for( LbListener listener : listeners ) {
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
            for( String zoneId : zoneIds ) {
                parameters.put("AvailabilityZones.member." + (i++), zoneId);
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
                if( serverIds != null && serverIds.length > 0 ) {
                    addServers(name, serverIds);
                }
                return name;
            }
            throw new CloudException("Unable to create a load balancer and no error message from AWS.");
        }
        finally {
            APITrace.end();
        }
    }

    public LoadBalancerAddressType getAddressType() {
        return LoadBalancerAddressType.DNS;
    }
    
    private Map<String,String> getELBParameters(ProviderContext ctx, String action) throws InternalException {
        APITrace.begin(provider, "getELBParameters");
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
    public LoadBalancer getLoadBalancer(String loadBalancerId) throws CloudException, InternalException {
        APITrace.begin(provider, "getLoadBalancer");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No valid context is established for this request");
            }
            Map<String,String> parameters = getELBParameters(provider.getContext(), ELBMethod.DESCRIBE_LOAD_BALANCERS);
            ELBMethod method;
            NodeList blocks;
            Document doc;

            if( loadBalancerId.length() > 32 ) {
                return null;
            }
            parameters.put("LoadBalancerNames.member.1", loadBalancerId);
            method = new ELBMethod(provider, ctx, parameters);
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
                        LoadBalancer loadBalancer = toLoadBalancer(ctx, item);

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
    public Iterable<LbAlgorithm> listSupportedAlgorithms() {
        if( algorithms == null ) {
            List<LbAlgorithm> list = new ArrayList<LbAlgorithm>();

            list.add(LbAlgorithm.ROUND_ROBIN);
            algorithms = Collections.unmodifiableList(list);
        }
        return algorithms;
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

    static private volatile List<LbProtocol> protocols;
    
    @Override
    public Iterable<LbProtocol> listSupportedProtocols() {
        if( protocols == null ) {
            List<LbProtocol> list = new ArrayList<LbProtocol>();

            list.add(LbProtocol.HTTP);
            list.add(LbProtocol.RAW_TCP);
            protocols = Collections.unmodifiableList(list);
        }
        return protocols;
    }
    
    public String getProviderTermForLoadBalancer(Locale locale) {
        return "load balancer";
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
    public boolean requiresListenerOnCreate() {
        return true;
    }

    @Override
    public boolean requiresServerOnCreate() {
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(provider, "isSubscribedELB");
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
    public boolean supportsMonitoring() {
        return true;
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listLoadBalancerStatus() throws CloudException, InternalException {
        APITrace.begin(provider, "listLoadBalancerStatus");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No valid context is established for this request");
            }

            if(!provider.getEC2Provider().isAWS() ) {
                return Collections.emptyList();
            }

            ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();
            Map<String,String> parameters = getELBParameters(provider.getContext(), ELBMethod.DESCRIBE_LOAD_BALANCERS);
            ELBMethod method;
            NodeList blocks;
            Document doc;

            method = new ELBMethod(provider, ctx, parameters);
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
    public Iterable<LoadBalancer> listLoadBalancers() throws CloudException, InternalException {
        APITrace.begin(provider, "listLoadBalancers");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No valid context is established for this request");
            }

            if(!provider.getEC2Provider().isAWS() ) {
                return Collections.emptyList();
            }

            ArrayList<LoadBalancer> list = new ArrayList<LoadBalancer>();
            Map<String,String> parameters = getELBParameters(provider.getContext(), ELBMethod.DESCRIBE_LOAD_BALANCERS);
            ELBMethod method;
            NodeList blocks;
            Document doc;

            method = new ELBMethod(provider, ctx, parameters);
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
                        LoadBalancer loadBalancer = toLoadBalancer(ctx, item);

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

    @Override
    public Iterable<LoadBalancerServer> getLoadBalancerServerHealth(String loadBalancerId) throws CloudException, InternalException {
        return getLoadBalancerServerHealth(loadBalancerId, null);
    }

    @Override
    public Iterable<LoadBalancerServer> getLoadBalancerServerHealth( String loadBalancerId, String... serverIdsToCheck ) throws CloudException, InternalException {
        APITrace.begin(provider, "getLoadBalancerServerHealth");
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

    @Override
    public void remove(String loadBalancerId) throws CloudException, InternalException {
        APITrace.begin(provider, "removeLoadBalancer");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No valid context is established for this request");
            }
            Map<String,String> parameters = getELBParameters(provider.getContext(), ELBMethod.DELETE_LOAD_BALANCER);
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
    public void removeDataCenters(String toLoadBalancerId, String ... availabilityZoneIds) throws CloudException, InternalException {
        APITrace.begin(provider, "removeDataCentersFromLB");
        try {
            if( availabilityZoneIds != null && availabilityZoneIds.length > 0 ) {
                ProviderContext ctx = provider.getContext();

                if( ctx == null ) {
                    throw new CloudException("No valid context is established for this request");
                }
                Map<String,String> parameters = getELBParameters(provider.getContext(), ELBMethod.DISABLE_AVAILABILITY_ZONES);
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
    public void removeServers(String toLoadBalancerId, String ... instanceIds) throws CloudException, InternalException {
        APITrace.begin(provider, "removeServersFromLB");
        try {
            if( instanceIds != null && instanceIds.length > 0 ) {
                ProviderContext ctx = provider.getContext();

                if( ctx == null ) {
                    throw new CloudException("No valid context is established for this request");
                }
                Map<String,String> parameters = getELBParameters(provider.getContext(), ELBMethod.DEREGISTER_INSTANCES);
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

    private LbListener toListener(Node node) {
        LbListener listener = new LbListener();
        NodeList attrs = node.getChildNodes();
        
        listener.setAlgorithm(LbAlgorithm.ROUND_ROBIN);
        for( int i=0; i<attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;
            
            name = attr.getNodeName().toLowerCase();
            if( name.equals("protocol") ) {
                listener.setNetworkProtocol(toProtocol(attr.getFirstChild().getNodeValue()));
            }
            else if( name.equals("loadbalancerport") ) {
                listener.setPublicPort(Integer.parseInt(attr.getFirstChild().getNodeValue()));
            }
            else if( name.equals("instanceport") ) {
                listener.setPrivatePort(Integer.parseInt(attr.getFirstChild().getNodeValue()));                
            }
        }
        return listener;
    }
    
    private @Nullable LoadBalancer toLoadBalancer(@Nonnull ProviderContext ctx, @Nullable Node node) {
        if( node == null ) {
            return null;
        }
        LoadBalancer loadBalancer = new LoadBalancer();
        NodeList attrs = node.getChildNodes();

        loadBalancer.setListeners(new LbListener[0]);
        loadBalancer.setProviderDataCenterIds(new String[0]);
        loadBalancer.setProviderServerIds(new String[0]);
        loadBalancer.setProviderRegionId(ctx.getRegionId());
        loadBalancer.setAddressType(LoadBalancerAddressType.DNS);
        loadBalancer.setCurrentState(LoadBalancerState.ACTIVE);
        loadBalancer.setProviderOwnerId(ctx.getAccountNumber());
        loadBalancer.setPublicPorts(new int[0]);
        loadBalancer.setSupportedTraffic(new IPVersion[] { IPVersion.IPV4, IPVersion.IPV6 });
        for( int i=0; i<attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;
            
            name = attr.getNodeName().toLowerCase();
            if( name.equals("listeners") ) {
                ArrayList<Integer> ports = new ArrayList<Integer>();
                
                if( attr.hasChildNodes() ) {
                    NodeList listeners = attr.getChildNodes();
                
                    if( listeners.getLength() > 0 ) {
                        ArrayList<LbListener> list = new ArrayList<LbListener>();
                        
                        for( int j=0; j<listeners.getLength(); j++ ) {
                            Node item = listeners.item(j);
                            
                            if( item.getNodeName().equals("member") ) {
                                LbListener l = toListener(item);
                                
                                if( l != null ) {
                                    list.add(l);
                                    ports.add(l.getPublicPort());
                                }
                            }
                        }
                        loadBalancer.setListeners(list.toArray(new LbListener[list.size()]));
                    }
                }
                int[] p = new int[ports.size()];
                int idx = 0;
                
                for( Integer port : ports ) {
                    p[idx++] = port;
                }
                loadBalancer.setPublicPorts(p);
            }
            else if( name.equals("loadbalancername") ) {
                loadBalancer.setName(attr.getFirstChild().getNodeValue());
                loadBalancer.setDescription(attr.getFirstChild().getNodeValue());
                loadBalancer.setProviderLoadBalancerId(attr.getFirstChild().getNodeValue());
            }
            else if( name.equals("instances") ) {
                if( attr.hasChildNodes() ) {
                    NodeList instances = attr.getChildNodes();
                
                    if( instances.getLength() > 0 ) {
                        ArrayList<String> ids = new ArrayList<String>();
                        
                        for( int j=0; j<instances.getLength(); j++ ) {
                            Node instance = instances.item(j);
                            
                            if( instance.getNodeName().equalsIgnoreCase("member") ) {
                                if( instance.hasChildNodes() ) {
                                    NodeList idList = instance.getChildNodes();
                                    
                                    for( int k=0; k<idList.getLength(); k++ ) {
                                        Node n = idList.item(k);
                                        
                                        if( n.getNodeName().equalsIgnoreCase("instanceid") ) {
                                            ids.add(n.getFirstChild().getNodeValue());
                                        }
                                    }
                                }
                            }
                        }
                        String[] tmp = new String[ids.size()];
                        int j=0; 
                        
                        for( String id : ids ) {
                            tmp[j++] = id;
                        }
                        loadBalancer.setProviderServerIds(tmp);
                    }
                }
            }
            else if( name.equals("createdtime") ) {
                try {
                    loadBalancer.setCreationTimestamp(provider.parseTime(attr.getFirstChild().getNodeValue()));
                }
                catch( CloudException e ) {
                    logger.warn("Unable to parse time: " + e.getMessage());
                }
            }
            else if( name.equals("healthcheck") ) {
                // unsupported
            }
            else if( name.equals("dnsname") ) {
                loadBalancer.setAddress(attr.getFirstChild().getNodeValue());
            }
            else if( name.equals("availabilityzones") ) {
                if( attr.hasChildNodes() ) {
                    NodeList zones = attr.getChildNodes();
                
                    if( zones.getLength() > 0 ) {
                        ArrayList<String> ids = new ArrayList<String>();
                        
                        for( int j=0; j<zones.getLength(); j++ ) {
                            Node zone = zones.item(j);
                            
                            if( zone.hasChildNodes() ) {
                                ids.add(zone.getFirstChild().getNodeValue());
                            }
                        }
                        String[] tmp = new String[ids.size()];
                        int j=0; 
                        
                        for( String id : ids ) {
                            tmp[j++] = id;
                        }
                        loadBalancer.setProviderDataCenterIds(tmp);
                    }
                }                
            }
        }
        return loadBalancer;
    }
    
    private LbProtocol toProtocol(String txt) {
        if( txt.equals("HTTP") ) {
            return LbProtocol.HTTP;
        }
        else {
            return LbProtocol.RAW_TCP;
        }
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
