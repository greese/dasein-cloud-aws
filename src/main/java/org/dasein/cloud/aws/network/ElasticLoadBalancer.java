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

import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.AWSResourceNotFoundException;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.aws.identity.IAMMethod;
import org.dasein.cloud.aws.identity.InvalidAmazonResourceNameException;
import org.dasein.cloud.aws.identity.SSLCertificateResourceName;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.*;
import org.dasein.cloud.util.APITrace;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.util.*;

public class ElasticLoadBalancer extends AbstractLoadBalancerSupport<AWSCloud> {
    static private final Logger logger = Logger.getLogger(ElasticLoadBalancer.class);

    private AWSCloud provider = null;
    private volatile transient ElasticLoadBalancerCapabilities capabilities;

    ElasticLoadBalancer( AWSCloud provider ) {
        super(provider);
        this.provider = provider;
    }

    @Override
    public void addDataCenters( @Nonnull String toLoadBalancerId, @Nonnull String... availabilityZoneIds ) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.addDataCenters");
        try {
            //noinspection ConstantConditions
            if( availabilityZoneIds != null && availabilityZoneIds.length > 0 ) {
                ProviderContext ctx = provider.getContext();

                if( ctx == null ) {
                    throw new CloudException("No valid context is established for this request");
                }
                Map<String, String> parameters = getELBParameters(getContext(), ELBMethod.ENABLE_AVAILABILITY_ZONES);
                ELBMethod method;

                parameters.put("LoadBalancerName", toLoadBalancerId);
                int i = 1;
                for( String zoneId : availabilityZoneIds ) {
                    parameters.put("AvailabilityZones.member." + ( i++ ), zoneId);
                }
                method = new ELBMethod(provider, ctx, parameters);
                try {
                    method.invoke();
                } catch( EC2Exception e ) {
                    logger.error(e.getSummary());
                    throw new CloudException(e);
                }
            }
        } finally {
            APITrace.end();
        }
    }

    @Override
    public void addServers( @Nonnull String toLoadBalancerId, @Nonnull String... instanceIds ) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.addServers");
        try {
            //noinspection ConstantConditions
            if( instanceIds != null && instanceIds.length > 0 ) {
                ProviderContext ctx = provider.getContext();

                if( ctx == null ) {
                    throw new CloudException("No valid context is established for this request");
                }
                Map<String, String> parameters = getELBParameters(getContext(), ELBMethod.REGISTER_INSTANCES);
                ELBMethod method;

                LoadBalancer lb = getLoadBalancer(toLoadBalancerId);

                if( lb == null ) {
                    throw new CloudException("No such load balancer: " + toLoadBalancerId);
                }
                LbListener[] listeners = lb.getListeners();

                //noinspection ConstantConditions
                if( listeners == null ) {
                    throw new CloudException("The load balancer " + toLoadBalancerId + " is improperly configured.");
                }
                parameters.put("LoadBalancerName", toLoadBalancerId);
                int i = 1;
                for( String instanceId : instanceIds ) {
                    parameters.put("Instances.member." + ( i++ ) + ".InstanceId", instanceId);
                }
                method = new ELBMethod(provider, ctx, parameters);
                try {
                    method.invoke();
                } catch( EC2Exception e ) {
                    throw new CloudException(e);
                }
            }
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String createLoadBalancer( @Nonnull LoadBalancerCreateOptions options ) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.createLoadBalancer");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No valid context is established for this request");
            }
            Map<String, String> parameters = getELBParameters(provider.getContext(), ELBMethod.CREATE_LOAD_BALANCER);
            ELBMethod method;
            NodeList blocks;
            Document doc;

            if( options.getProviderIpAddressId() != null ) {
                throw new OperationNotSupportedException(getProvider().getCloudName() + " does not support assignment of IP addresses to load balancers.");
            }
            String name = verifyName(options.getName());
            parameters.put("LoadBalancerName", name);
            Map<String, String> certificateName2arn = new HashMap<String, String>();
            int i = 1;
            for( LbListener listener : options.getListeners() ) {
                switch( listener.getNetworkProtocol() ) {
                    case HTTP: parameters.put("Listeners.member." + i + ".Protocol", "HTTP"); break;
                    case HTTPS: parameters.put("Listeners.member." + i + ".Protocol", "HTTPS"); break;
                    case RAW_TCP: parameters.put("Listeners.member." + i + ".Protocol", "TCP"); break;
                    default: throw new CloudException("Invalid protocol: " + listener.getNetworkProtocol());
                }
                parameters.put("Listeners.member." + i + ".LoadBalancerPort", String.valueOf(listener.getPublicPort()));
                parameters.put("Listeners.member." + i + ".InstancePort", String.valueOf(listener.getPrivatePort()));
                if ( listener.getSslCertificateName() != null ) {
                    String certificateName = listener.getSslCertificateName();
                    String arn = certificateName2arn.get( certificateName );
                    if ( arn == null ) {
                        SSLCertificate certificate = getSSLCertificate( certificateName );
                        if ( certificate == null ) {
                            throw new AWSResourceNotFoundException( "Could not find certificate by ID [" +
                                                    certificateName + "] for listener " + listener );
                        }
                        arn = certificate.getProviderCertificateId();
                        certificateName2arn.put( certificateName, arn );
                    }
                    parameters.put("Listeners.member." + i + ".SSLCertificateId", arn);
                }
                i++;
            }
            i = 1;
            for( String zoneId : options.getProviderDataCenterIds() ) {
                parameters.put("AvailabilityZones.member." + ( i++ ), zoneId);
            }
            i = 1;
            for( String subnetId : options.getProviderSubnetIds() ) {
                parameters.put("Subnets.member." + ( i++ ), subnetId);
            }

            if( options.getType() != null && options.getType() == LbType.INTERNAL ) {
                parameters.put("Scheme", "internal");
            }
            method = new ELBMethod(provider, ctx, parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("DNSName");
            if( blocks.getLength() > 0 ) {

                modifyLoadBalancerAttributes(
                        name,
                        AttributesOptions.getInstance(
                                options.getCrossZone(),
                                options.getConnectionDraining(),
                                options.getConnectionDrainingTimeout()
                        )
                );

                for( LoadBalancerEndpoint endpoint : options.getEndpoints() ) {
                    if( endpoint.getEndpointType().equals(LbEndpointType.VM) ) {
                        addServers(name, endpoint.getEndpointValue());
                    }
                    else {
                        addIPEndpoints(name, endpoint.getEndpointValue());
                    }
                }

                if( options.getHealthCheckOptions() != null ) {
                    options.getHealthCheckOptions().setLoadBalancerId(name);
                    options.getHealthCheckOptions().setName(toHCName(name));
                    try {
                        createLoadBalancerHealthCheck(options.getHealthCheckOptions());
                    } catch( CloudException e ) {
                        // let's try and be transactional
                        removeLoadBalancer(name);
                        throw new InternalException(e);
                    } catch( InternalException e ) {
                        // let's try and be transactional
                        removeLoadBalancer(name);
                        throw new InternalException(e);
                    }
                }
                return name;
            }
            throw new CloudException("Unable to create a load balancer and no error message from the cloud.");
        } finally {
            APITrace.end();
        }
    }

    @SuppressWarnings( "deprecation" ) @Override @Deprecated
    public @Nonnull String create( @Nonnull String name, @Nonnull String description, @Nullable String addressId, @Nullable String[] zoneIds, @Nullable LbListener[] listeners, @Nullable String[] serverIds, @Nullable String[] subnetIds, @Nullable LbType type ) throws CloudException, InternalException {
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
        if( type != null ) {
            options.asType(type);
        }
        return createLoadBalancer(options);
    }

    @Override
    public SSLCertificate createSSLCertificate(@Nonnull SSLCertificateCreateOptions options) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.createSSLCertificate");
        try {
            if ( !provider.getEC2Provider().isAWS() ) {
                return null;
            }

            Map<String, String> parameters = provider.getStandardParameters(getContext(),
                                                                IAMMethod.CREATE_SSL_CERTIFICATE, IAMMethod.VERSION);
            provider.putValueIfNotNull(parameters, "CertificateBody", options.getCertificateBody());
            provider.putValueIfNotNull(parameters, "CertificateChain", options.getCertificateChain());
            provider.putValueIfNotNull(parameters, "Path", options.getPath());
            provider.putValueIfNotNull(parameters, "PrivateKey", options.getPrivateKey());
            provider.putValueIfNotNull(parameters, "ServerCertificateName", options.getCertificateName());

            Document doc;
            IAMMethod method = new IAMMethod(provider, parameters);
            try {
                doc = method.invoke();
            }
            catch ( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            NodeList blocks = doc.getElementsByTagName("ServerCertificateMetadata");
            for ( int i = 0; i < blocks.getLength(); i++ ) {
                ServerCertificateMetadata meta = toSSLCertificateMetadata(blocks.item(i));
                if (meta != null) {
                    return SSLCertificate.getInstance(meta.id, meta.arn, meta.uploadDate,
                                      options.getCertificateBody(), options.getCertificateChain(), meta.path);
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull LoadBalancerAddressType getAddressType() {
        return LoadBalancerAddressType.DNS;
    }

    @Nonnull @Override
    public LoadBalancerCapabilities getCapabilities() throws CloudException, InternalException {
        if( capabilities == null ) {
            capabilities = new ElasticLoadBalancerCapabilities(provider);
        }
        return capabilities;
    }

    private @Nonnull Map<String, String> getELBParameters( @Nonnull ProviderContext ctx, @Nonnull String action ) throws InternalException {
        APITrace.begin(provider, "LB.getELBParameters");
        try {
            HashMap<String, String> parameters = new HashMap<String, String>();

            parameters.put(AWSCloud.P_ACTION, action);
            parameters.put(AWSCloud.P_SIGNATURE_VERSION, AWSCloud.SIGNATURE);
            try {
                parameters.put(AWSCloud.P_ACCESS, new String(ctx.getAccessPublic(), "utf-8"));
            } catch( UnsupportedEncodingException e ) {
                logger.error(e);
                e.printStackTrace();
                throw new InternalException(e);
            }
            parameters.put(AWSCloud.P_SIGNATURE_METHOD, AWSCloud.EC2_ALGORITHM);
            parameters.put(AWSCloud.P_TIMESTAMP, provider.getTimestamp(System.currentTimeMillis(), true));
            parameters.put(AWSCloud.P_VERSION, provider.getElbVersion());
            return parameters;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nullable LoadBalancer getLoadBalancer( @Nonnull String loadBalancerId ) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.getLoadBalancer");
        try {
            Map<String, String> parameters = getELBParameters(getContext(), ELBMethod.DESCRIBE_LOAD_BALANCERS);
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
            } catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && code.equals("LoadBalancerNotFound") ) {
                    return null;
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("LoadBalancerDescriptions");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j = 0; j < items.getLength(); j++ ) {
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
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nullable SSLCertificate getSSLCertificate(@Nonnull String certificateName) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.getCertificate");
        try {
            if (!provider.getEC2Provider().isAWS()) {
                return null;
            }

            Map<String, String> parameters = provider.getStandardParameters(getContext(),
                                                                IAMMethod.GET_SSL_CERTIFICATE, IAMMethod.VERSION);
            parameters.put("ServerCertificateName", certificateName);
            Document doc;

            IAMMethod method = new IAMMethod(provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( "NoSuchEntity".equals(code) ) {
                    return null;
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            NodeList blocks = doc.getElementsByTagName("ServerCertificate");
            for ( int i = 0; i < blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                SSLCertificate certificate = toSSLCertificate(item);
                if (certificate != null) {
                    return certificate;
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    public @Nonnull Iterable<SSLCertificate> listSSLCertificates() throws CloudException, InternalException {
        APITrace.begin(provider, "LB.listSSLCertificates");
        try {
            if (!provider.getEC2Provider().isAWS()) {
                return Collections.emptyList();
            }

            List<SSLCertificate> list  = new ArrayList<SSLCertificate>();
            Map<String, String> parameters = provider.getStandardParameters(getContext(),
                                                            IAMMethod.LIST_SSL_CERTIFICATES, IAMMethod.VERSION);
            NodeList blocks;
            Document doc;

            IAMMethod method = new IAMMethod(provider, parameters);
            try {
                doc = method.invoke();
            }
            catch ( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }

            blocks = doc.getElementsByTagName("ServerCertificateMetadataList");
            for ( int i = 0; i < blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for ( int j = 0; j < items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if ( "member".equals(item.getNodeName()) ) {
                        ServerCertificateMetadata meta = toSSLCertificateMetadata(item);
                        if ( meta != null ) {
                            list.add(SSLCertificate.getInstance(meta.id, meta.arn, meta.uploadDate,
                                                                null, null, meta.path));
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
                Map<String, String> parameters = getELBParameters(provider.getContext(), ELBMethod.DESCRIBE_LOAD_BALANCERS);
                ELBMethod method;

                method = new ELBMethod(provider, ctx, parameters);
                try {
                    method.invoke();
                    return true;
                } catch( EC2Exception e ) {
                    if( e.getStatus() == HttpServletResponse.SC_UNAUTHORIZED || e.getStatus() == HttpServletResponse.SC_FORBIDDEN ) {
                        return false;
                    }
                    String code = e.getCode();

                    if( code != null && ( code.equals("SubscriptionCheckFailed") || code.equals("AuthFailure") || code.equals("SignatureDoesNotMatch") || code.equals("InvalidClientTokenId") || code.equals("OptInRequired") ) ) {
                        return false;
                    }
                    throw new CloudException(e);
                }
            } catch( RuntimeException e ) {
                logger.error("Could not check subscription status: " + e.getMessage());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new InternalException(e);
            } catch( Error e ) {
                logger.error("Could not check subscription status: " + e.getMessage());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new InternalException(e);
            }
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<LoadBalancerEndpoint> listEndpoints( @Nonnull String loadBalancerId ) throws CloudException, InternalException {
        //noinspection RedundantArrayCreation
        return listEndpoints(loadBalancerId, LbEndpointType.VM, new String[0]);
    }

    @Override
    public @Nonnull Iterable<LoadBalancerEndpoint> listEndpoints( @Nonnull String loadBalancerId, @Nonnull LbEndpointType type, @Nonnull String... endpoints ) throws CloudException, InternalException {
        if( !type.equals(LbEndpointType.VM) ) {
            return Collections.emptyList();
        }
        APITrace.begin(provider, "LB.listEndpoints");
        try {
            ArrayList<LoadBalancerEndpoint> list = new ArrayList<LoadBalancerEndpoint>();
            Map<String, String> parameters = getELBParameters(getContext(), ELBMethod.DESCRIBE_INSTANCE_HEALTH);
            ELBMethod method;
            NodeList blocks;
            Document doc;

            parameters.put("LoadBalancerName", loadBalancerId);
            if( endpoints.length > 0 ) {
                for( int i = 0; i < endpoints.length; i++ ) {
                    parameters.put("Instances.member." + ( i + 1 ) + ".InstanceId", endpoints[i]);
                }
            }
            method = new ELBMethod(provider, getContext(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("InstanceStates");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j = 0; j < items.getLength(); j++ ) {
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
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listLoadBalancerStatus() throws CloudException, InternalException {
        APITrace.begin(provider, "LB.listLoadBalancerStatus");
        try {
            if( !provider.getEC2Provider().isAWS() ) {
                return Collections.emptyList();
            }

            ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();
            Map<String, String> parameters = getELBParameters(getContext(), ELBMethod.DESCRIBE_LOAD_BALANCERS);
            ELBMethod method;
            NodeList blocks;
            Document doc;

            method = new ELBMethod(provider, getContext(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("LoadBalancerDescriptions");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j = 0; j < items.getLength(); j++ ) {
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
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<LoadBalancer> listLoadBalancers() throws CloudException, InternalException {
        APITrace.begin(provider, "LB.listLoadBalancers");
        try {
            if( !provider.getEC2Provider().isAWS() ) {
                return Collections.emptyList();
            }

            ArrayList<LoadBalancer> list = new ArrayList<LoadBalancer>();
            Map<String, String> parameters = getELBParameters(getContext(), ELBMethod.DESCRIBE_LOAD_BALANCERS);
            ELBMethod method;
            NodeList blocks;
            Document doc;

            method = new ELBMethod(provider, getContext(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("LoadBalancerDescriptions");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j = 0; j < items.getLength(); j++ ) {
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
        } finally {
            APITrace.end();
        }
    }

    @SuppressWarnings( "deprecation" ) @Override @Deprecated
    public @Nonnull Iterable<LoadBalancerServer> getLoadBalancerServerHealth( @Nonnull String loadBalancerId ) throws CloudException, InternalException {
        return getLoadBalancerServerHealth(loadBalancerId, new String[0]);
    }

    @SuppressWarnings( "deprecation" ) @Override @Deprecated
    public @Nonnull Iterable<LoadBalancerServer> getLoadBalancerServerHealth( @Nonnull String loadBalancerId, @Nonnull String... serverIdsToCheck ) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.getLoadBalancerServerHealth");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No valid context is established for this request");
            }

            ArrayList<LoadBalancerServer> list = new ArrayList<LoadBalancerServer>();
            Map<String, String> parameters = getELBParameters(provider.getContext(), ELBMethod.DESCRIBE_INSTANCE_HEALTH);
            ELBMethod method;
            NodeList blocks;
            Document doc;

            parameters.put("LoadBalancerName", loadBalancerId);
            if( serverIdsToCheck != null && serverIdsToCheck.length > 0 ) {
                for( int i = 0; i < serverIdsToCheck.length; i++ ) {
                    parameters.put("Instances.member." + ( i + 1 ) + ".InstanceId", serverIdsToCheck[i]);
                }
            }
            method = new ELBMethod(provider, ctx, parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("InstanceStates");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j = 0; j < items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("member") ) {
                        LoadBalancerServer loadBalancerServer = toLoadBalancerServer(ctx, loadBalancerId, item);
                        if( loadBalancerServer != null ) {
                            list.add(loadBalancerServer);
                        }
                    }
                }
            }
            return list;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction( @Nonnull ServiceAction action ) {
        if( action.equals(LoadBalancerSupport.ANY) ) {
            return new String[]{ELBMethod.ELB_PREFIX + "*"};
        }
        else if( action.equals(LoadBalancerSupport.ADD_DATA_CENTERS) ) {
            return new String[]{ELBMethod.ELB_PREFIX + ELBMethod.ENABLE_AVAILABILITY_ZONES};
        }
        else if( action.equals(LoadBalancerSupport.ADD_VMS) ) {
            return new String[]{ELBMethod.ELB_PREFIX + ELBMethod.REGISTER_INSTANCES};
        }
        else if( action.equals(LoadBalancerSupport.CREATE_LOAD_BALANCER) ) {
            return new String[]{ELBMethod.ELB_PREFIX + ELBMethod.CREATE_LOAD_BALANCER};
        }
        else if( action.equals(LoadBalancerSupport.GET_LOAD_BALANCER) || action.equals(LoadBalancerSupport.LIST_LOAD_BALANCER) ) {
            return new String[]{ELBMethod.ELB_PREFIX + ELBMethod.DESCRIBE_LOAD_BALANCERS};
        }
        else if( action.equals(LoadBalancerSupport.GET_LOAD_BALANCER_SERVER_HEALTH) ) {
            return new String[]{ELBMethod.ELB_PREFIX + ELBMethod.DESCRIBE_INSTANCE_HEALTH};
        }
        else if( action.equals(LoadBalancerSupport.REMOVE_DATA_CENTERS) ) {
            return new String[]{ELBMethod.ELB_PREFIX + ELBMethod.DISABLE_AVAILABILITY_ZONES};
        }
        else if( action.equals(LoadBalancerSupport.REMOVE_LOAD_BALANCER) ) {
            return new String[]{ELBMethod.ELB_PREFIX + ELBMethod.DELETE_LOAD_BALANCER};
        }
        else if( action.equals(LoadBalancerSupport.REMOVE_VMS) ) {
            return new String[]{ELBMethod.ELB_PREFIX + ELBMethod.DEREGISTER_INSTANCES};
        }
        else if( action.equals(LoadBalancerSupport.ATTACH_LB_TO_SUBNETS) ) {
            return new String[]{ELBMethod.ELB_PREFIX + ELBMethod.ATTACH_LB_TO_SUBNETS};
        }
        else if( action.equals(LoadBalancerSupport.DETACH_LB_FROM_SUBNETS) ) {
            return new String[]{ELBMethod.ELB_PREFIX + ELBMethod.DETACH_LB_FROM_SUBNETS};
        }
        else if (action.equals(LoadBalancerSupport.SET_FIREWALLS)) {
            return new String[]{ELBMethod.APPLY_SECURITY_GROUPS_TO_LOAD_BALANCER};
        }
        return new String[0];
    }

    @SuppressWarnings( "deprecation" ) @Override @Deprecated
    public void remove( @Nonnull String loadBalancerId ) throws CloudException, InternalException {
        removeLoadBalancer(loadBalancerId);
    }

    @Override
    public void removeLoadBalancer( @Nonnull String loadBalancerId ) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.remove");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No valid context is established for this request");
            }
            Map<String, String> parameters = getELBParameters(getContext(), ELBMethod.DELETE_LOAD_BALANCER);
            ELBMethod method;

            parameters.put("LoadBalancerName", loadBalancerId);
            method = new ELBMethod(provider, ctx, parameters);
            try {
                method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
        } finally {
            APITrace.end();
        }
    }

    @Override
    public void removeDataCenters( @Nonnull String toLoadBalancerId, @Nonnull String... availabilityZoneIds ) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.removeDataCenters");
        try {
            //noinspection ConstantConditions
            if( availabilityZoneIds != null && availabilityZoneIds.length > 0 ) {
                ProviderContext ctx = provider.getContext();

                if( ctx == null ) {
                    throw new CloudException("No valid context is established for this request");
                }
                Map<String, String> parameters = getELBParameters(getContext(), ELBMethod.DISABLE_AVAILABILITY_ZONES);
                ELBMethod method;

                parameters.put("LoadBalancerName", toLoadBalancerId);
                int i = 1;
                for( String zoneId : availabilityZoneIds ) {
                    parameters.put("AvailabilityZones.member." + ( i++ ), zoneId);
                }
                method = new ELBMethod(provider, ctx, parameters);
                try {
                    method.invoke();
                } catch( EC2Exception e ) {
                    logger.error(e.getSummary());
                    throw new CloudException(e);
                }
            }
        } finally {
            APITrace.end();
        }
    }

    @Override
    public void removeSSLCertificate(@Nonnull String certificateName) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.removeSSLCertificate");
        try {
            if ( !provider.getEC2Provider().isAWS() ) {
                return;
            }

            Map<String, String> parameters = provider.getStandardParameters(getContext(),
                                                                IAMMethod.DELETE_SSL_CERTIFICATE, IAMMethod.VERSION);
            parameters.put("ServerCertificateName", certificateName);
            IAMMethod method = new IAMMethod(provider, parameters);
            try {
                method.invoke();
            }
            catch ( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
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
                Map<String, String> parameters = getELBParameters(getContext(), ELBMethod.DEREGISTER_INSTANCES);
                ELBMethod method;

                parameters.put("LoadBalancerName", toLoadBalancerId);
                int i = 1;
                for( String instanceId : instanceIds ) {
                    parameters.put("Instances.member." + ( i++ ) + ".InstanceId", instanceId);
                }
                method = new ELBMethod(provider, ctx, parameters);
                try {
                    method.invoke();
                } catch( EC2Exception e ) {
                    logger.error(e.getSummary());
                    throw new CloudException(e);
                }
            }
        } finally {
            APITrace.end();
        }
    }

    @Override
    public void setSSLCertificate(@Nonnull SetLoadBalancerSSLCertificateOptions options)
            throws CloudException, InternalException {
        APITrace.begin(provider, "LB.setSSLCertificate");
        try {
            ProviderContext ctx = provider.getContext();
            if (ctx == null) {
                throw new CloudException("No valid context is established for this request");
            }

            // Find the certificate ARN first
            SSLCertificate certificate = getSSLCertificate(options.getSslCertificateName());
            if (certificate == null) {
                throw new AWSResourceNotFoundException("Could not find SSL certificate by ID [" +
                                                               options.getSslCertificateName() + "]");
            }

            Map<String, String> parameters = getELBParameters(ctx, ELBMethod.SET_LB_SSL_CERTIFICATE);
            parameters.put("LoadBalancerName", options.getLoadBalancerName());
            parameters.put("LoadBalancerPort", String.valueOf(options.getSslCertificateAssignToPort()));
            parameters.put("SSLCertificateId", certificate.getProviderCertificateId());

            ELBMethod method = new ELBMethod(provider, ctx, parameters);
            try {
                method.invoke();
            }
            catch ( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public LoadBalancerHealthCheck createLoadBalancerHealthCheck(@Nonnull HealthCheckOptions options)throws CloudException, InternalException{
        APITrace.begin(provider, "LB.configureHealthCheck");
        try {
            ProviderContext ctx = provider.getContext();
            if( ctx == null ) {
                throw new CloudException("No valid context is established for this request");
            }

            if( options.getProviderLoadBalancerId() == null ) {
                throw new InternalException("HealthCheck options must include the load balancer ID");
            }

            NodeList blocks;
            Document doc;
            Map<String, String> parameters = getELBParameters(getContext(), ELBMethod.CONFIGURE_HEALTH_CHECK);
            ELBMethod method;

            parameters.put("LoadBalancerName", options.getProviderLoadBalancerId());
            parameters.put("HealthCheck.HealthyThreshold", options.getHealthyCount() + "");
            parameters.put("HealthCheck.UnhealthyThreshold", options.getUnhealthyCount() + "");
            String path = "";
            if( options.getPort() < 1 || options.getPort() > 65535 ) {
                throw new CloudException("Port must have a number between 1 and 65535.");
            }
            if( options.getProtocol().equals(LoadBalancerHealthCheck.HCProtocol.HTTP) || options.getProtocol().equals(LoadBalancerHealthCheck.HCProtocol.HTTPS)) {
                if( options.getPath() != null && !options.getPath().isEmpty() ) {
                    path = options.getPath();
                } else {
                    path = "/";
                }
            }
            parameters.put("HealthCheck.Target", options.getProtocol().name() + ":" + options.getPort() + path);
            // TODO: these limits should be made available through capabilities
            // and handled by core in HCOptions ctor
            int interval = options.getInterval();
            if( interval > 300 ) {
                interval = 300;
            } else if( interval < 3 ) {
                interval = 3;
            }
            // TODO: same here
            parameters.put("HealthCheck.Interval", String.valueOf(interval));
            int timeout = options.getTimeout();
            if( timeout > 60 ) {
                timeout = 60;
            } else if( timeout < 2 ) {
                timeout = 2;
            }
            // TODO: same here, timeout should be less than interval
            if( timeout >= interval) {
                timeout = interval - 1;
            }
            parameters.put("HealthCheck.Timeout", String.valueOf(timeout));

            method = new ELBMethod(provider, ctx, parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("HealthCheck");
            if( blocks.getLength() > 0 ) {
                return toLBHealthCheck(options.getProviderLoadBalancerId(), blocks.item(0));
            }
            throw new CloudException("An error occurred while configuring the Health Check.");
        } finally {
            APITrace.end();
        }
    }

    //TODO: Get instance health

    @Override
    public LoadBalancerHealthCheck getLoadBalancerHealthCheck( @Nullable String providerLBHealthCheckId, @Nullable String providerLoadBalancerId ) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.getLoadBalancerHealthCheck");
        try {
            Map<String, String> parameters = getELBParameters(getContext(), ELBMethod.DESCRIBE_LOAD_BALANCERS);
            ELBMethod method;
            NodeList blocks;
            Document doc;

            if( providerLoadBalancerId.length() > 32 ) {
                return null;
            }
            parameters.put("LoadBalancerNames.member.1", providerLoadBalancerId);
            method = new ELBMethod(provider, getContext(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && code.equals("LoadBalancerNotFound") ) {
                    return null;
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("HealthCheck");
            if( blocks.getLength() > 0 ) {
                LoadBalancerHealthCheck lbhc = toLBHealthCheck(providerLoadBalancerId, blocks.item(0));
                lbhc.addProviderLoadBalancerId(providerLoadBalancerId);
                lbhc.setName(toHCName(providerLoadBalancerId));
                return lbhc;
            }
            return null;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public void setFirewalls(@Nonnull String loadBalancerId, @Nonnull String... firewallIds) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.setFirewalls");
        try {
            ProviderContext ctx = provider.getContext();

            if (ctx == null) {
                throw new CloudException("No valid context is established for this request");
            }

            Map<String, String> parameters = getELBParameters(ctx, ELBMethod.APPLY_SECURITY_GROUPS_TO_LOAD_BALANCER);

            parameters.put("LoadBalancerName", loadBalancerId);
            for (int i = 0; i < firewallIds.length; i++) {
                parameters.put("SecurityGroups.member." + (i + 1), firewallIds[i]);
            }

            ELBMethod method = new ELBMethod(provider, ctx, parameters);

            try {
                method.invoke();
            } catch (EC2Exception e) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void modifyLoadBalancerAttributes(@Nonnull String id, @Nonnull AttributesOptions options) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.modifyLoadBalancerAttributes");
        try {
            ProviderContext ctx = provider.getContext();
            if (ctx == null) {
                throw new CloudException("No valid context is established for this request");
            }

            Map<String, String> parameters = getELBParameters(ctx, ELBMethod.MODIFY_LOADBALANCER_ATTRIBUTES);

            parameters.put("LoadBalancerName", id);

            if (options.getCrossZone() != null) {
                parameters.put("LoadBalancerAttributes.CrossZoneLoadBalancing.Enabled", options.getCrossZone().toString());
            }

            if (options.getConnectionDraining() != null) {
                parameters.put("LoadBalancerAttributes.ConnectionDraining.Enabled", options.getConnectionDraining().toString());
            }

            if (options.getConnectionDrainingTimeout() != null) {
                parameters.put("LoadBalancerAttributes.ConnectionDraining.Timeout", options.getConnectionDrainingTimeout().toString());
            }

            ELBMethod method = new ELBMethod(provider, ctx, parameters);
            try {
                method.invoke();
            } catch (EC2Exception e) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
        } finally {
            APITrace.end();
        }
    }

    @Override
    public AttributesOptions getLoadBalancerAttributes(@Nonnull String id) throws CloudException, InternalException {
        APITrace.begin(provider,"LB.DescribeLoadBalancerAttributes");
        try {
            ProviderContext ctx = provider.getContext();
            if (ctx == null) {
                throw new CloudException("No valid context is established for this request");
            }

            Map<String, String> parameters = getELBParameters(ctx, ELBMethod.DESCRIBE_LOADBALANCER_ATTRIBUTES);

            parameters.put("LoadBalancerName", id);
            Boolean crossZoneLoadBalancingEnabled = null;
            Boolean connectionDrainingEnabled = null;
            Integer connectionDrainingTimeout = null;

            ELBMethod method = new ELBMethod(provider, ctx, parameters);
            Document doc;
            NodeList blocks;
            try {
                doc = method.invoke();
            } catch (EC2Exception e) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }

            blocks = doc.getElementsByTagName("LoadBalancerAttributes");

            for (int i = 0; i < blocks.getLength(); i++) {
                NodeList items = blocks.item(i).getChildNodes();

                for (int j = 0; j < items.getLength(); j++) {
                    Node item = items.item(j);

                    if (item.getNodeName().equals("ConnectionDraining")) {
                        NodeList attrs = item.getChildNodes();
                        for (int k = 0; k < attrs.getLength(); k++) {
                            Node attr = attrs.item(k);
                            String name = attr.getNodeName();
                            if ("Enabled".equalsIgnoreCase(name)) {
                                connectionDrainingEnabled = Boolean.valueOf(attr.getFirstChild().getNodeValue());
                            } else if ("Timeout".equalsIgnoreCase(name)) {
                                connectionDrainingTimeout = Integer.valueOf(attr.getFirstChild().getNodeValue());
                            }
                        }
                    } else if (item.getNodeName().equals("CrossZoneLoadBalancing")) {
                        NodeList attrs = item.getChildNodes();
                        for (int k = 0; k < attrs.getLength(); k++) {
                            Node attr = attrs.item(k);
                            String name = attr.getNodeName();
                            if ("Enabled".equalsIgnoreCase(name)) {
                                crossZoneLoadBalancingEnabled = Boolean.valueOf(attr.getFirstChild().getNodeValue());
                            }
                        }
                    }
                }
            }

            return AttributesOptions.getInstance(crossZoneLoadBalancingEnabled, connectionDrainingEnabled, connectionDrainingTimeout);
        } finally {
            APITrace.end();
        }
    }

    @Override
    public Iterable<LoadBalancerHealthCheck> listLBHealthChecks( @Nullable HealthCheckFilterOptions opts ) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.listLBHealthChecks");
        try {
            if( !provider.getEC2Provider().isAWS() ) {
                return Collections.emptyList();
            }

            List<LoadBalancerHealthCheck> list = new ArrayList<LoadBalancerHealthCheck>();
            Map<String, String> parameters = getELBParameters(getContext(), ELBMethod.DESCRIBE_LOAD_BALANCERS);
            ELBMethod method;
            NodeList blocks;
            Document doc;

            method = new ELBMethod(provider, getContext(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("LoadBalancerDescriptions");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j = 0; j < items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("member") ) {
                        NodeList attrs = item.getChildNodes();
                        String lbId = null;
                        LoadBalancerHealthCheck lbhc = null;
                        for( int k = 0; k < attrs.getLength(); k++ ) {
                            Node attr = attrs.item(k);
                            String name = attr.getNodeName();
                            if( "LoadBalancerName".equalsIgnoreCase(name) ) {
                                lbId = attr.getFirstChild().getNodeValue();
                            } else if( "HealthCheck".equalsIgnoreCase(name) ) {
                                lbhc = toLBHealthCheck(lbId, attr);
                            }
                        }
                        if( lbhc != null && lbId != null ) {
                            lbhc.addProviderLoadBalancerId(lbId);
                            lbhc.setName(toHCName(lbId));
                            if( opts != null ) {
                                if( opts.matches(lbhc) ) {
                                    list.add(lbhc);
                                }
                            } else {
                                // filter options not set, add all
                                list.add(lbhc);
                            }
                        }
                    }
                }
            }
            return list;
        } finally {
            APITrace.end();
        }
    }

    @Override
    public LoadBalancerHealthCheck modifyHealthCheck( @Nonnull String providerLBHealthCheckId, @Nonnull HealthCheckOptions options ) throws InternalException, CloudException {
        return createLoadBalancerHealthCheck(options);
    }

    @Override
    public void attachLoadBalancerToSubnets(@Nonnull String toLoadBalancerId, @Nonnull String... subnetIdsToAdd) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.attachLoadBalancerToSubnets");
        try {
            ProviderContext ctx = provider.getContext();
            if (ctx == null) {
                throw new CloudException("No valid context is established for this request");
            }

            Map<String, String> parameters = getELBParameters(ctx, ELBMethod.ATTACH_LB_TO_SUBNETS);
            parameters.put("LoadBalancerName", toLoadBalancerId);
            for(int i = 1; i <= subnetIdsToAdd.length; i++) {
                parameters.put("Subnets.member." + i, subnetIdsToAdd[i - 1] );
            }

            ELBMethod method = new ELBMethod(provider, ctx, parameters);
            try {
                method.invoke();
            }
            catch ( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void detachLoadBalancerFromSubnets(@Nonnull String fromLoadBalancerId, @Nonnull String... subnetIdsToDelete) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.detachLoadBalancerFromSubnets");
        try {
            ProviderContext ctx = provider.getContext();
            if (ctx == null) {
                throw new CloudException("No valid context is established for this request");
            }

            Map<String, String> parameters = getELBParameters(ctx, ELBMethod.DETACH_LB_FROM_SUBNETS);
            parameters.put("LoadBalancerName", fromLoadBalancerId);
            for(int i = 1; i <= subnetIdsToDelete.length; i++) {
                parameters.put("Subnets.member." + i, subnetIdsToDelete[i - 1] );
            }

            ELBMethod method = new ELBMethod(provider, ctx, parameters);
            try {
                method.invoke();
            }
            catch ( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    private LoadBalancerHealthCheck toLBHealthCheck( @Nullable String lbId, @Nonnull Node node ) {
        NodeList attrs = node.getChildNodes();
        LoadBalancerHealthCheck.HCProtocol protocol = null;
        int port = 0;
        String path = null;
        int healthyCount = 0;
        int unHealthyCount = 0;
        int timeout = 0;
        int interval = 0;

        for( int i = 0; i < attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name = attr.getNodeName().toLowerCase();

            if( name.equals("interval") ) {
                interval = Integer.parseInt(attr.getFirstChild().getNodeValue());
            }
            else if( name.equals("target") ) {
                String targetString = attr.getFirstChild().getNodeValue();
                String[] parts = targetString.split(":");
                protocol = LoadBalancerHealthCheck.HCProtocol.valueOf(parts[0]);
                if( parts[1].endsWith("/") ) {
                    port = Integer.parseInt(parts[1].substring(0, parts[1].length() - 1));
                    path = "/";
                }
                else {
                    String[] portAndPath = parts[1].split("/");
                    port = Integer.parseInt(portAndPath[0]);
                    if( portAndPath.length > 1 ) {
                        path = "/" + portAndPath[1];
                    }
                }
            }
            else if( name.equals("healthythreshold") ) {
                healthyCount = Integer.parseInt(attr.getFirstChild().getNodeValue());
            }
            else if( name.equals("unhealthythreshold") ) {
                unHealthyCount = Integer.parseInt(attr.getFirstChild().getNodeValue());
            }
            else if( name.equals("timeout") ) {
                timeout = Integer.valueOf(attr.getFirstChild().getNodeValue());
            }
        }
        LoadBalancerHealthCheck lbhc = LoadBalancerHealthCheck.getInstance(lbId, protocol, port, path, interval, timeout, healthyCount, unHealthyCount);
        if( lbId != null ) {
            lbhc.setName(toHCName(lbId));
        }
        return lbhc;
    }

    // this is the only place where we are generating names (please)
    private String toHCName(String lbId) {
        // in AWS we will use LB name for its HC name, since it is synthetic and the relationship is 1:1
        return lbId;
    }

    private LbListener toListener( Node node ) {
        NodeList attrs = node.getChildNodes();

        LbProtocol protocol = LbProtocol.RAW_TCP;
        int publicPort = 0;
        int privatePort = 0;
        String sslCertificateName = null;

        for( int i = 0; i < attrs.getLength(); i++ ) {
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
            else if ( name.equals("sslcertificateid") ) {
                try {
                    SSLCertificateResourceName sslCertificateResourceName = SSLCertificateResourceName.parseArn(attr.getFirstChild().getNodeValue());
                    sslCertificateName = sslCertificateResourceName.getCertificateName();
                } catch (InvalidAmazonResourceNameException e) {
                    logger.error("Invalid amazon resource name: " + e.getInvalidResourceName(), e);
                }
            }
        }
        return LbListener.getInstance(protocol, publicPort, privatePort, sslCertificateName);
    }


    private @Nullable LoadBalancer toLoadBalancer(@Nullable Node node) throws CloudException, InternalException {
        if( node == null ) {
            return null;
        }
        List<LbListener> listenerList = new ArrayList<LbListener>();
        List<Integer> portList = new ArrayList<Integer>();
        List<String> zoneList = new ArrayList<String>();
        List<String> serverIds = new ArrayList<String>();
        List<String> firewallIds = new ArrayList<String>();
        String regionId = getContext().getRegionId();
        String lbName = null, description = null, lbId = null, cname = null;
        boolean withHealthCheck = false;
        long created = 0L;
        LbType type = null;
        ArrayList<String> subnetList = new ArrayList<String>();

        if( regionId == null ) {
            throw new CloudException("No region was set for this context");
        }
        NodeList attrs = node.getChildNodes();

        for( int i = 0; i < attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;

            name = attr.getNodeName().toLowerCase();
            if( name.equals("listenerdescriptions") ) {
                if( attr.hasChildNodes() ) {
                    NodeList listeners = attr.getChildNodes();

                    if( listeners.getLength() > 0 ) {
                        for( int j = 0; j < listeners.getLength(); j++ ) {
                            Node item = listeners.item(j);
                            if( item.getNodeName().equals("member") ) {
                                NodeList listenerMembers = item.getChildNodes();
                                for( int k = 0; k < listenerMembers.getLength(); k++ ) {
                                    Node listenerItem = listenerMembers.item(k);
                                    if( listenerItem.getNodeName().equals("Listener") ) {
                                        LbListener l = toListener(listenerItem);

                                        if( l != null ) {
                                            listenerList.add(l);
                                            portList.add(l.getPublicPort());
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
            } else if(name.equalsIgnoreCase("securitygroups")) {
                if (attr.hasChildNodes()) {
                    NodeList firewalls = attr.getChildNodes();

                    if (firewalls.getLength() > 0) {
                        for (int j = 0; j < firewalls.getLength(); j++) {
                            Node firewall = firewalls.item(j);

                            if (firewall.hasChildNodes()) {
                                firewallIds.add(AWSCloud.getTextValue(firewall));
                            }
                        }
                    }
                }
            }
            else if( name.equals("instances") ) {
                if( attr.hasChildNodes() ) {
                    NodeList instances = attr.getChildNodes();

                    if( instances.getLength() > 0 ) {
                        for( int j = 0; j < instances.getLength(); j++ ) {
                            Node instance = instances.item(j);

                            if( instance.getNodeName().equalsIgnoreCase("member") ) {
                                if( instance.hasChildNodes() ) {
                                    NodeList idList = instance.getChildNodes();

                                    for( int k = 0; k < idList.getLength(); k++ ) {
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
                } catch( CloudException e ) {
                    logger.warn("Unable to parse time: " + e.getMessage());
                }
            }
            else if( name.equals("healthcheck") ) {
                withHealthCheck = true;
            }
            else if( name.equals("dnsname") ) {
                cname = attr.getFirstChild().getNodeValue();
            }
            else if( name.equals("availabilityzones") ) {
                if( attr.hasChildNodes() ) {
                    NodeList zones = attr.getChildNodes();

                    if( zones.getLength() > 0 ) {
                        for( int j = 0; j < zones.getLength(); j++ ) {
                            Node zone = zones.item(j);

                            if( zone.hasChildNodes() ) {
                                zoneList.add(zone.getFirstChild().getNodeValue());
                            }
                        }
                    }
                }
            }
            else if( name.equals("scheme") ) {
                if( "internal".equals(provider.getTextValue(attr)) ) {
                    type = LbType.INTERNAL;
                }
            }
            else if( name.equals("subnets") ) {
                if( attr.hasChildNodes() ) {
                    NodeList subnets = attr.getChildNodes();

                    if( subnets.getLength() > 0 ) {
                        for( int j = 0; j < subnets.getLength(); j++ ) {
                            Node subnet = subnets.item(j);

                            if( subnet.hasChildNodes() ) {
                                subnetList.add(provider.getTextValue(subnet));
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
            lbName = lbId + " (" + cname + ")";
        }
        if( description == null ) {
            description = lbName;
        }
        int[] ports = new int[portList.size()];
        int i = 0;

        for( Integer p : portList ) {
            ports[i++] = p;
        }
        LoadBalancer lb = LoadBalancer.getInstance(getContext().getAccountNumber(), regionId, lbId, LoadBalancerState.ACTIVE, lbName, description, LoadBalancerAddressType.DNS, cname, ports).supportingTraffic(IPVersion.IPV4, IPVersion.IPV6).createdAt(created);

        if (!firewallIds.isEmpty()) {
            lb.setProviderFirewallIds(firewallIds.toArray(new String[firewallIds.size()]));
        }
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
        if( type != null ) {
            lb.setType(type);
        }
        if( !subnetList.isEmpty() ) {
            lb.withProviderSubnetIds(subnetList.toArray(new String[subnetList.size()]));
        }
        if( withHealthCheck ) {
            lb.setProviderLBHealthCheckId(lbId);
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

    private @Nullable LoadBalancerEndpoint toEndpoint( @Nullable Node node ) throws CloudException, InternalException {
        if( node == null ) {
            return null;
        }
        String reason = null, description = null, vmId = null;
        LbEndpointState state = LbEndpointState.ACTIVE;
        NodeList attrs = node.getChildNodes();

        for( int i = 0; i < attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;

            name = attr.getNodeName().toLowerCase();
            if( name.equals("instanceid") && attr.hasChildNodes() ) {
                vmId = attr.getFirstChild().getNodeValue().trim();
            }
            else if( name.equals("state") && attr.hasChildNodes() ) {
                if( attr.getFirstChild().getNodeValue().trim().equalsIgnoreCase("InService") ) {
                    state = LbEndpointState.ACTIVE;
                }
                else {
                    state = LbEndpointState.INACTIVE;
                }
            }
            else if( name.equals("description") && attr.hasChildNodes() ) {
                String value = attr.getFirstChild().getNodeValue().trim();

                if( !"N/A".equals(value) ) {
                    description = value;
                }
            }
            else if( name.equals("reasoncode") && attr.hasChildNodes() ) {
                String value = attr.getFirstChild().getNodeValue().trim();

                if( !"N/A".equals(value) ) {
                    reason = attr.getFirstChild().getNodeValue();
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
        if( node == null ) {
            return null;
        }
        LoadBalancerServer loadBalancerServer = new LoadBalancerServer();
        NodeList attrs = node.getChildNodes();

        loadBalancerServer.setProviderOwnerId(ctx.getAccountNumber());
        loadBalancerServer.setProviderRegionId(ctx.getRegionId());
        loadBalancerServer.setProviderLoadBalancerId(loadBalancerId);

        for( int i = 0; i < attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;

            name = attr.getNodeName().toLowerCase();
            if( name.equals("instanceid") ) {
                loadBalancerServer.setProviderServerId(attr.getFirstChild().getNodeValue());
            }
            else if( name.equals("state") ) {
                loadBalancerServer.setCurrentState(toServerState(attr.getFirstChild().getNodeValue()));
            }
            else if( name.equals("description") ) {
                String value = attr.getFirstChild().getNodeValue();
                if( !"N/A".equals(value) ) {
                    loadBalancerServer.setCurrentStateDescription(value);
                }
            }
            else if( name.equals("reasoncode") ) {
                String value = attr.getFirstChild().getNodeValue();
                if( !"N/A".equals(value) ) {
                    loadBalancerServer.setCurrentStateReason(attr.getFirstChild().getNodeValue());
                }
            }
        }

        return loadBalancerServer;
    }

    private LoadBalancerServerState toServerState( String txt ) {
        if( txt.equals("InService") ) {
            return LoadBalancerServerState.ACTIVE;
        }
        else {
            return LoadBalancerServerState.INACTIVE;
        }
    }

    private @Nullable ResourceStatus toStatus( @Nullable Node node ) {
        if( node == null ) {
            return null;
        }
        NodeList attrs = node.getChildNodes();
        String lbId = null;

        for( int i = 0; i < attrs.getLength(); i++ ) {
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

    private @Nullable ServerCertificateMetadata toSSLCertificateMetadata(@Nullable Node node) {
        if ( node == null ) {
            return null;
        }

        String id = null, path = null, arn = null;
        Long uploadDate = null;
        NodeList attrs = node.getChildNodes();
        for ( int i = 0; i < attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String key = attr.getNodeName();
            String value = attr.getFirstChild() != null ? attr.getFirstChild().getNodeValue() : null;

            if ( "ServerCertificateName".equalsIgnoreCase(key) ) {
                /* ServerCertificateName works as the identifier of the certificate */
                id = value;
            }
            else if ( "Path".equalsIgnoreCase(key) ) {
                path = value;
            }
            else if ( "Arn".equalsIgnoreCase(key) ) {
                arn = value;
            }
            else if ( "UploadDate".equalsIgnoreCase(key) ) {
                try {
                    uploadDate = provider.parseTime(value);
                }
                catch( CloudException e ) {
                    logger.warn("Unable to parse uploadDate of ServerCertificateMetadata: " + e.getMessage());
                }
            }
            else if ( "ServerCertificateId".equals(key) ) {
                /* ServerCertificateId is not used in dasein-cloud-core */
            }
        }

        if (id == null) {
            logger.error("ServerCertificateName was missing in ServerCertificateMetadata");
            return null;
        }
        if (path == null) {
            logger.error("Path was missing in ServerCertificateMetadata");
            return null;
        }
        if (arn == null) {
            logger.error("Arn was missing in ServerCertificateMetadata");
            return null;
        }
        return new ServerCertificateMetadata(arn, path, id, uploadDate);
    }

    private @Nullable SSLCertificate toSSLCertificate(@Nullable Node node) {
        if ( node == null ) {
            return null;
        }

        String body = null, chain = null;
        ServerCertificateMetadata meta = null;

        NodeList attrs = node.getChildNodes();
        for ( int i = 0; i < attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String key = attr.getNodeName();
            if ( "CertificateBody".equalsIgnoreCase(key) ) {
                body = attr.getFirstChild().getNodeValue();
            }
            else if ( "CertificateChain".equalsIgnoreCase(key) ) {
                chain = attr.getFirstChild().getNodeValue();
            }
            else if ( "ServerCertificateMetadata".equalsIgnoreCase(key) ) {
                meta = toSSLCertificateMetadata(attr);
            }
        }
        if (body == null) {
            logger.error("CertificateBody was missing in ServerCertificate response");
            return null;
        }
        if (meta == null) {
            logger.error("ServerCertificateMetadata was missing in ServerCertificate response");
            return null;
        }
        return SSLCertificate.getInstance(meta.id, meta.arn, meta.uploadDate, body, chain, meta.path);
    }

    private String verifyName(String name) {
        StringBuilder str = new StringBuilder();

        for( int i = 0; i < name.length(); i++ ) {
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
            name = name.substring(0, 32);
        }
        while( name.charAt(name.length() - 1) == '-' ) {
            name = name.substring(0, name.length() - 1);
        }
        return name;
    }

    private class ServerCertificateMetadata {
        String arn;
        String path;
        String id;
        Long   uploadDate;

        private ServerCertificateMetadata(String arn, String path, String id, Long uploadDate) {
            this.arn = arn;
            this.path = path;
            this.id = id;
            this.uploadDate = uploadDate;
        }
    }

}
