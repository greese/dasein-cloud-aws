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

package org.dasein.cloud.aws.compute;

import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.compute.*;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.util.APITrace;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorPopulator;
import org.dasein.util.PopulatorThread;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Storage;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class AutoScaling extends AbstractAutoScalingSupport<AWSCloud> {
    static private final Logger logger     = Logger.getLogger(AutoScaling.class);
    public static final  String SERVICE_ID = "autoscaling";

    AutoScaling( AWSCloud provider ) {
        super(provider);
    }

    @Override
    public String createAutoScalingGroup( @Nonnull AutoScalingGroupOptions autoScalingGroupOptions ) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "AutoScaling.createAutoScalingGroup");
        try {
            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.CREATE_AUTO_SCALING_GROUP);
            EC2Method method;

            int minServers = autoScalingGroupOptions.getMinServers();
            if( minServers < 0 ) {
                minServers = 0;
            }
            int maxServers = autoScalingGroupOptions.getMaxServers();
            if( maxServers < minServers ) {
                maxServers = minServers;
            }
            parameters.put("AutoScalingGroupName", autoScalingGroupOptions.getName());
            parameters.put("LaunchConfigurationName", autoScalingGroupOptions.getLaunchConfigurationId());
            parameters.put("MinSize", String.valueOf(minServers));
            parameters.put("MaxSize", String.valueOf(maxServers));
            if( autoScalingGroupOptions.getCooldown() != null ) {
                parameters.put("DefaultCooldown", String.valueOf(autoScalingGroupOptions.getCooldown()));
            }
            if( autoScalingGroupOptions.getDesiredCapacity() != null ) {
                parameters.put("DesiredCapacity", String.valueOf(autoScalingGroupOptions.getDesiredCapacity()));
            }
            if( autoScalingGroupOptions.getHealthCheckGracePeriod() != null ) {
                parameters.put("HealthCheckGracePeriod", String.valueOf(autoScalingGroupOptions.getHealthCheckGracePeriod()));
            }
            if( autoScalingGroupOptions.getHealthCheckType() != null ) {
                parameters.put("HealthCheckType", autoScalingGroupOptions.getHealthCheckType());
            }
            if( autoScalingGroupOptions.getProviderSubnetIds() != null ) {
                StringBuilder vpcZones = new StringBuilder();
                int i = 0;
                for( String subnetId : autoScalingGroupOptions.getProviderSubnetIds() ) {
                    if( i > 0 ) {
                        vpcZones.append(",");
                    }
                    vpcZones.append(subnetId);
                    i++;
                }
                parameters.put("VPCZoneIdentifier", vpcZones.toString());
            }
            if( autoScalingGroupOptions.getProviderDataCenterIds() != null ) {
                int i = 1;
                for( String zoneId : autoScalingGroupOptions.getProviderDataCenterIds() ) {
                    parameters.put("AvailabilityZones.member." + ( i++ ), zoneId);
                }
            }
            if( autoScalingGroupOptions.getProviderLoadBalancerIds() != null ) {
                int i = 1;
                for( String lbId : autoScalingGroupOptions.getProviderLoadBalancerIds() ) {
                    parameters.put("LoadBalancerNames.member." + ( i++ ), lbId);
                }
            }
            if( autoScalingGroupOptions.getTags() != null ) {
                int i = 1;
                for( AutoScalingTag tag : autoScalingGroupOptions.getTags() ) {
                    parameters.put("Tags.member." + i + ".Key", tag.getKey());
                    parameters.put("Tags.member." + i + ".Value", tag.getValue());
                    parameters.put("Tags.member." + i + ".PropagateAtLaunch", String.valueOf(tag.isPropagateAtLaunch()));
                    i++;
                }
            }

            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
            try {
                method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            return autoScalingGroupOptions.getName();
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public String createAutoScalingGroup( @Nonnull String name, @Nonnull String launchConfigurationId, @Nonnull Integer minServers, @Nonnull Integer maxServers, @Nullable Integer cooldown, @Nullable String[] loadBalancerIds, @Nullable Integer desiredCapacity, @Nullable Integer healthCheckGracePeriod, @Nullable String healthCheckType, @Nullable String vpcZones, @Nullable String... zoneIds ) throws InternalException, CloudException {
        AutoScalingGroupOptions options = new AutoScalingGroupOptions(name).withLaunchConfigurationId(launchConfigurationId).withMinServers(minServers).withMaxServers(maxServers).withCooldown(cooldown).withProviderLoadBalancerIds(loadBalancerIds).withDesiredCapacity(desiredCapacity).withHealthCheckGracePeriod(healthCheckGracePeriod).withHealthCheckType(healthCheckType).withProviderSubnetIds(vpcZones != null ? vpcZones.split(",") : new String[]{}).withProviderDataCenterIds(zoneIds);

        return createAutoScalingGroup(options);

    }

    @Override
    public void updateAutoScalingGroup( @Nonnull String scalingGroupId, @Nullable String launchConfigurationId, @Nonnegative Integer minServers, @Nonnegative Integer maxServers, @Nullable Integer cooldown, @Nullable Integer desiredCapacity, @Nullable Integer healthCheckGracePeriod, @Nullable String healthCheckType, @Nullable String vpcZones, @Nullable String... zoneIds ) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "AutoScaling.updateAutoScalingGroup");
        try {
            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.UPDATE_AUTO_SCALING_GROUP);
            EC2Method method;

            if( scalingGroupId != null ) {
                parameters.put("AutoScalingGroupName", scalingGroupId);
            }
            if( launchConfigurationId != null ) {
                parameters.put("LaunchConfigurationName", launchConfigurationId);
            }
            if( minServers != null ) {
                if( minServers < 0 ) {
                    minServers = 0;
                }
                parameters.put("MinSize", String.valueOf(minServers));
            }
            if( maxServers != null ) {
                if( minServers != null && maxServers < minServers ) {
                    maxServers = minServers;
                }
                parameters.put("MaxSize", String.valueOf(maxServers));
            }
            if( cooldown != null ) {
                parameters.put("DefaultCooldown", String.valueOf(cooldown));
            }
            if( desiredCapacity != null ) {
                parameters.put("DesiredCapacity", String.valueOf(desiredCapacity));
            }
            if( healthCheckGracePeriod != null ) {
                parameters.put("HealthCheckGracePeriod", String.valueOf(healthCheckGracePeriod));
            }
            if( healthCheckType != null ) {
                parameters.put("HealthCheckType", healthCheckType);
            }
            if( vpcZones != null ) {
                parameters.put("VPCZoneIdentifier", vpcZones);
            }
            if( zoneIds != null ) {
                int i = 1;
                for( String zoneId : zoneIds ) {
                    parameters.put("AvailabilityZones.member." + ( i++ ), zoneId);
                }
            }
            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
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
    public String createLaunchConfiguration( String name, String imageId, VirtualMachineProduct size, String keyPairName, String userData, String providerRoleId, Boolean detailedMonitoring, String... firewalls ) throws InternalException, CloudException {
        return createLaunchConfiguration(new LaunchConfigurationCreateOptions(name, imageId, size, keyPairName, userData, providerRoleId, detailedMonitoring, firewalls));
    }

    @Override
    public String createLaunchConfiguration( @Nonnull LaunchConfigurationCreateOptions options ) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "AutoScaling.createLaunchConfigursation");
        try {
            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.CREATE_LAUNCH_CONFIGURATION);
            EC2Method method;

            parameters.put("LaunchConfigurationName", options.getName());
            if( options.getImageId() != null ) {
                parameters.put("ImageId", options.getImageId());
            }
            if( options.getKeypairName() != null ) {
                parameters.put("KeyName", options.getKeypairName());
            }
            if( options.getUserData() != null ) {
                parameters.put("UserData", options.getUserData());
            }
            if( options.getProviderRoleId() != null ) {
                parameters.put("IamInstanceProfile", options.getProviderRoleId());
            }
            if( options.getDetailedMonitoring() != null ) {
                parameters.put("InstanceMonitoring.Enabled", options.getDetailedMonitoring().toString());
            }
            if( options.getSize() != null ) {
                parameters.put("InstanceType", options.getSize().getProviderProductId());
            }
            int i = 1;
            if( options.getFirewallIds() != null ) {
                for( String fw : options.getFirewallIds() ) {
                    parameters.put("SecurityGroups.member." + ( i++ ), fw);
                }
            }
            if( options.getAssociatePublicIPAddress() != null ) {
                parameters.put("AssociatePublicIpAddress", options.getAssociatePublicIPAddress().toString());
            }
            if( options.getIOOptimized() != null ) {
                parameters.put("EbsOptimized", options.getIOOptimized().toString());
            }
            if( options.getVolumeAttachment() != null ) {
                int z = 1;
                for( VolumeAttachment va : options.getVolumeAttachment() ) {
                    parameters.put("BlockDeviceMappings.member." + z + ".DeviceName", va.deviceId);
                    EBSVolume ebsv = new EBSVolume(getProvider());
                    String volType = null;
                    try {
                        VolumeProduct prd = ebsv.getVolumeProduct(va.volumeToCreate.getVolumeProductId());
                        volType = prd.getProviderProductId();
                    }
                    catch( Exception e ) {
                        // toss it
                    }
                    if( volType == null ) {
                        if( options.getIOOptimized() && va.volumeToCreate.getIops() > 0 ) {
                            parameters.put("BlockDeviceMappings.member." + z + ".Ebs.VolumeType", "io1");
                        }
                    }
                    else {
                        parameters.put("BlockDeviceMappings.member." + z + ".Ebs.VolumeType", volType);
                    }
                    if( va.volumeToCreate.getIops() > 0 ) {
                        parameters.put("BlockDeviceMappings.member." + z + ".Ebs.Iops", String.valueOf(va.volumeToCreate.getIops()));
                    }
                    if( va.volumeToCreate.getSnapshotId() != null ) {
                        parameters.put("BlockDeviceMappings.member." + z + ".Ebs.SnapshotId", va.volumeToCreate.getSnapshotId());
                    }
                    if( va.volumeToCreate.getVolumeSize().getQuantity().intValue() > 0 ) {
                        parameters.put("BlockDeviceMappings.member." + z + ".Ebs.VolumeSize", String.valueOf(va.volumeToCreate.getVolumeSize().getQuantity().intValue()));
                    }
                    z++;
                }
            }
            if( options.getVirtualMachineIdToClone() != null ) {
                parameters.put("InstanceId", options.getVirtualMachineIdToClone());
            }

            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
            try {
                method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            return options.getName();
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void deleteAutoScalingGroup( String providerAutoScalingGroupId ) throws InternalException, CloudException {
        deleteAutoScalingGroup(new AutoScalingGroupDeleteOptions(providerAutoScalingGroupId));
    }

    @Override
    public void deleteAutoScalingGroup( @Nonnull AutoScalingGroupDeleteOptions options ) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "AutoScaling.deleteAutoScalingGroup");
        try {
            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.DELETE_AUTO_SCALING_GROUP);
            EC2Method method;

            parameters.put("AutoScalingGroupName", options.getProviderAutoScalingGroupId());
            parameters.put("ForceDelete", options.getForceDelete().toString());
            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
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
    public void deleteLaunchConfiguration( String providerLaunchConfigurationId ) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "AutoScaling.deleteLaunchConfiguration");
        try {
            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.DELETE_LAUNCH_CONFIGURATION);
            EC2Method method;

            parameters.put("LaunchConfigurationName", providerLaunchConfigurationId);
            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
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
    public String setTrigger( String name, String scalingGroupId, String statistic, String unitOfMeasure, String metric, int periodInSeconds, double lowerThreshold, double upperThreshold, int lowerIncrement, boolean lowerIncrementAbsolute, int upperIncrement, boolean upperIncrementAbsolute, int breachDuration ) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "AutoScaling.setTrigger");
        try {
            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.CREATE_OR_UPDATE_SCALING_TRIGGER);
            EC2Method method;

            parameters.put("AutoScalingGroupName", scalingGroupId);
            parameters.put("MeasureName", metric);
            parameters.put("Period", String.valueOf(periodInSeconds));
            parameters.put("LowerThreshold", String.valueOf(lowerThreshold));
            parameters.put("UpperThreshold", String.valueOf(upperThreshold));
            parameters.put("UpperBreachScaleIncrement", String.valueOf(upperIncrement));
            parameters.put("LowerBreachScaleIncrement", String.valueOf(lowerIncrement));
            parameters.put("BreachDuration", String.valueOf(breachDuration));
            parameters.put("TriggerName", name);
            parameters.put("Unit", unitOfMeasure);
            parameters.put("Statistic", statistic);
            parameters.put("Dimensions.member.1.Name", "AutoScalingGroupName");
            parameters.put("Dimensions.member.1.Value", scalingGroupId);
            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
            try {
                method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            return name;
        }
        finally {
            APITrace.end();
        }
    }

    private Map<String, String> getAutoScalingParameters( ProviderContext ctx, String action ) throws InternalException {
        APITrace.begin(getProvider(), "AutoScaling.getAutoScalingParameters");
        try {
            HashMap<String, String> parameters = new HashMap<String, String>();

            parameters.put(AWSCloud.P_ACTION, action);
            parameters.put(AWSCloud.P_SIGNATURE_VERSION, AWSCloud.SIGNATURE_V2);
            try {
                parameters.put(AWSCloud.P_ACCESS, new String(ctx.getAccessPublic(), "utf-8"));
            }
            catch( UnsupportedEncodingException e ) {
                logger.error(e);
                e.printStackTrace();
                throw new InternalException(e);
            }
            parameters.put(AWSCloud.P_SIGNATURE_METHOD, AWSCloud.EC2_ALGORITHM);
            parameters.put(AWSCloud.P_TIMESTAMP, getProvider().getTimestamp(System.currentTimeMillis(), true));
            parameters.put(AWSCloud.P_VERSION, getProvider().getAutoScaleVersion());
            return parameters;
        }
        finally {
            APITrace.end();
        }
    }

    private String getAutoScalingUrl() throws CloudException {
        ProviderContext ctx = getProvider().getContext();

        if( ctx == null ) {
            throw new CloudException("No context has been set for this request");
        }
        return "https://autoscaling." + ctx.getRegionId() + ".amazonaws.com";
    }

    @Override
    public LaunchConfiguration getLaunchConfiguration( String providerLaunchConfigurationId ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "AutoScaling.getLaunchConfiguration");
        try {
            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.DESCRIBE_LAUNCH_CONFIGURATIONS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("LaunchConfigurationNames.member.1", providerLaunchConfigurationId);
            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("LaunchConfigurations");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j = 0; j < items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("member") ) {
                        LaunchConfiguration cfg = toLaunchConfiguration(item);

                        if( cfg != null ) {
                            return cfg;
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
    public ScalingGroup getScalingGroup( String providerScalingGroupId ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "AutoScaling.getScalingGroup");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new CloudException("No context has been set for this request");
            }
            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.DESCRIBE_AUTO_SCALING_GROUPS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("AutoScalingGroupNames.member.1", providerScalingGroupId);
            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("AutoScalingGroups");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                NodeList members = blocks.item(i).getChildNodes();

                for( int j = 0; j < members.getLength(); j++ ) {
                    Node item = members.item(j);

                    if( item.getNodeName().equals("member") ) {
                        ScalingGroup group = toScalingGroup(ctx, item);

                        if( group != null ) {
                            return group;
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
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "AutoScaling.isSubscribed");
        try {
            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.DESCRIBE_AUTO_SCALING_GROUPS);
            EC2Method method;

            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
            try {
                method.invoke();
                return true;
            }
            catch( EC2Exception e ) {
                String msg = e.getSummary();

                if( msg != null && msg.contains("not able to validate the provided access credentials") ) {
                    return false;
                }
                logger.error("AWS Error checking subscription: " + e.getCode() + "/" + e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void suspendAutoScaling( String providerScalingGroupId, String[] processesToSuspend ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "AutoScaling.suspendAutoScaling");
        try {
            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.SUSPEND_AUTO_SCALING_GROUP);
            EC2Method method;

            parameters.put("AutoScalingGroupName", providerScalingGroupId);
            int x = 1;
            if( processesToSuspend != null ) {
                for( String process : processesToSuspend ) {
                    parameters.put("ScalingProcesses.member." + ( x++ ), process);
                }
            }
            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
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
    public void resumeAutoScaling( String providerScalingGroupId, String[] processesToResume ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "AutoScaling.resumeAutoScaling");
        try {
            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.RESUME_AUTO_SCALING_GROUP);
            EC2Method method;

            parameters.put("AutoScalingGroupName", providerScalingGroupId);
            int x = 1;
            if( processesToResume != null ) {
                for( String process : processesToResume ) {
                    parameters.put("ScalingProcesses.member." + ( x++ ), process);
                }
            }
            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
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
    public String updateScalingPolicy( String policyName, String adjustmentType, String autoScalingGroupName, Integer cooldown, Integer minAdjustmentStep, Integer scalingAdjustment ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "AutoScaling.updateScalingPolicy");
        try {
            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.PUT_SCALING_POLICY);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("PolicyName", policyName);
            parameters.put("AdjustmentType", adjustmentType);
            parameters.put("AutoScalingGroupName", autoScalingGroupName);
            if( cooldown != null ) {
                parameters.put("Cooldown", String.valueOf(cooldown));
            }
            if( minAdjustmentStep != null ) {
                parameters.put("MinAdjustmentStep", String.valueOf(minAdjustmentStep));
            }
            parameters.put("ScalingAdjustment", String.valueOf(scalingAdjustment));
            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("PolicyARN");
            if( blocks.getLength() > 0 ) {
                return blocks.item(0).getFirstChild().getNodeValue().trim();
            }
            throw new CloudException("Successful POST, but no Policy information was provided");
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void deleteScalingPolicy( @Nonnull String policyName, @Nullable String autoScalingGroupName ) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "AutoScaling.deleteScalingPolicy");
        try {
            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.DELETE_SCALING_POLICY);
            EC2Method method;

            parameters.put("PolicyName", policyName);
            if( autoScalingGroupName != null ) {
                parameters.put("AutoScalingGroupName", autoScalingGroupName);
            }
            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
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
    public Collection<ScalingPolicy> listScalingPolicies( @Nullable String autoScalingGroupName ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "AutoScaling.getScalingPolicies");
        try {
            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.DESCRIBE_SCALING_POLICIES);
            ArrayList<ScalingPolicy> list = new ArrayList<ScalingPolicy>();
            EC2Method method;
            NodeList blocks;
            Document doc;

            if( autoScalingGroupName != null ) {
                parameters.put("AutoScalingGroupName", autoScalingGroupName);
            }
            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("ScalingPolicies");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j = 0; j < items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("member") ) {
                        ScalingPolicy sp = toScalingPolicy(item);

                        if( sp != null ) {
                            list.add(sp);
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
    public ScalingPolicy getScalingPolicy( @Nonnull String policyName ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "AutoScaling.getScalingPolicy");
        try {
            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.DESCRIBE_SCALING_POLICIES);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("PolicyNames.member.1", policyName);

            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            ScalingPolicy sp = null;
            blocks = doc.getElementsByTagName("ScalingPolicies");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j = 0; j < items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("member") ) {
                        sp = toScalingPolicy(item);
                        return sp;
                    }
                }
            }
            return sp;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listLaunchConfigurationStatus() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "AutoScaling.listLaunchConfigurationStatus");
        try {
            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.DESCRIBE_LAUNCH_CONFIGURATIONS);
            ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();
            EC2Method method;
            NodeList blocks;
            Document doc;

            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("LaunchConfigurations");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j = 0; j < items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("member") ) {
                        ResourceStatus status = toLCStatus(item);

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
    public Collection<LaunchConfiguration> listLaunchConfigurations() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "AutoScaling.listLaunchConfigurations");
        try {
            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.DESCRIBE_LAUNCH_CONFIGURATIONS);
            ArrayList<LaunchConfiguration> list = new ArrayList<LaunchConfiguration>();
            EC2Method method;
            NodeList blocks;
            Document doc;

            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("LaunchConfigurations");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j = 0; j < items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("member") ) {
                        LaunchConfiguration cfg = toLaunchConfiguration(item);

                        if( cfg != null ) {
                            list.add(cfg);
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
    public Iterable<ResourceStatus> listScalingGroupStatus() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "AutoScaling.listScalingGroupStatus");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new CloudException("No context has been set for this request");
            }
            ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();

            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.DESCRIBE_AUTO_SCALING_GROUPS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("AutoScalingGroups");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j = 0; j < items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("member") ) {
                        ResourceStatus status = toGroupStatus(item);

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

    /**
     * Provides backwards compatibility
     */
    @Override
    public Collection<ScalingGroup> listScalingGroups() throws CloudException, InternalException {
        return listScalingGroups(AutoScalingGroupFilterOptions.getInstance());
    }

    /**
     * Returns filtered list of auto scaling groups.
     *
     * @param options the filter parameters
     * @return filtered list of scaling groups
     */
    @Override
    public Collection<ScalingGroup> listScalingGroups( AutoScalingGroupFilterOptions options ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "AutoScaling.listScalingGroups");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new CloudException("No context has been set for this request");
            }
            ArrayList<ScalingGroup> list = new ArrayList<ScalingGroup>();

            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.DESCRIBE_AUTO_SCALING_GROUPS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("AutoScalingGroups");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j = 0; j < items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("member") ) {
                        ScalingGroup group = toScalingGroup(ctx, item);

                        if( ( group != null && ( options != null && !options.hasCriteria() ) ) || ( group != null && ( options != null && options.hasCriteria() && options.matches(group) ) ) ) {
                            list.add(group);
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
    public @Nonnull String[] mapServiceAction( @Nonnull ServiceAction action ) {
        if( action.equals(AutoScalingSupport.ANY) ) {
            return new String[]{EC2Method.AUTOSCALING_PREFIX + "*"};
        }
        if( action.equals(AutoScalingSupport.CREATE_LAUNCH_CONFIGURATION) ) {
            return new String[]{EC2Method.AUTOSCALING_PREFIX + EC2Method.CREATE_LAUNCH_CONFIGURATION};
        }
        else if( action.equals(AutoScalingSupport.CREATE_SCALING_GROUP) ) {
            return new String[]{EC2Method.AUTOSCALING_PREFIX + EC2Method.CREATE_AUTO_SCALING_GROUP};
        }
        else if( action.equals(AutoScalingSupport.GET_LAUNCH_CONFIGURATION) ) {
            return new String[]{EC2Method.AUTOSCALING_PREFIX + EC2Method.DESCRIBE_LAUNCH_CONFIGURATIONS};
        }
        else if( action.equals(AutoScalingSupport.GET_SCALING_GROUP) ) {
            return new String[]{EC2Method.AUTOSCALING_PREFIX + EC2Method.DESCRIBE_AUTO_SCALING_GROUPS};
        }
        else if( action.equals(AutoScalingSupport.LIST_LAUNCH_CONFIGURATION) ) {
            return new String[]{EC2Method.AUTOSCALING_PREFIX + EC2Method.DESCRIBE_LAUNCH_CONFIGURATIONS};
        }
        else if( action.equals(AutoScalingSupport.LIST_SCALING_GROUP) ) {
            return new String[]{EC2Method.AUTOSCALING_PREFIX + EC2Method.DESCRIBE_AUTO_SCALING_GROUPS};
        }
        else if( action.equals(AutoScalingSupport.REMOVE_LAUNCH_CONFIGURATION) ) {
            return new String[]{EC2Method.AUTOSCALING_PREFIX + EC2Method.DELETE_LAUNCH_CONFIGURATION};
        }
        else if( action.equals(AutoScalingSupport.REMOVE_SCALING_GROUP) ) {
            return new String[]{EC2Method.AUTOSCALING_PREFIX + EC2Method.DELETE_AUTO_SCALING_GROUP};
        }
        else if( action.equals(AutoScalingSupport.SET_CAPACITY) ) {
            return new String[]{EC2Method.AUTOSCALING_PREFIX + EC2Method.SET_DESIRED_CAPACITY};
        }
        else if( action.equals(AutoScalingSupport.SET_SCALING_TRIGGER) ) {
            return new String[]{EC2Method.AUTOSCALING_PREFIX + EC2Method.CREATE_OR_UPDATE_SCALING_TRIGGER};
        }
        else if( action.equals(AutoScalingSupport.UPDATE_SCALING_GROUP) ) {
            return new String[]{EC2Method.AUTOSCALING_PREFIX + EC2Method.UPDATE_AUTO_SCALING_GROUP};
        }
        else if( action.equals(AutoScalingSupport.SUSPEND_AUTO_SCALING_GROUP) ) {
            return new String[]{EC2Method.AUTOSCALING_PREFIX + EC2Method.SUSPEND_AUTO_SCALING_GROUP};
        }
        else if( action.equals(AutoScalingSupport.RESUME_AUTO_SCALING_GROUP) ) {
            return new String[]{EC2Method.AUTOSCALING_PREFIX + EC2Method.RESUME_AUTO_SCALING_GROUP};
        }
        else if( action.equals(AutoScalingSupport.PUT_SCALING_POLICY) ) {
            return new String[]{EC2Method.AUTOSCALING_PREFIX + EC2Method.PUT_SCALING_POLICY};
        }
        else if( action.equals(AutoScalingSupport.DELETE_SCALING_POLICY) ) {
            return new String[]{EC2Method.AUTOSCALING_PREFIX + EC2Method.DELETE_SCALING_POLICY};
        }
        else if( action.equals(AutoScalingSupport.LIST_SCALING_POLICIES) ) {
            return new String[]{EC2Method.AUTOSCALING_PREFIX + EC2Method.DESCRIBE_SCALING_POLICIES};
        }
        return new String[0];
    }

    @Override
    public void setDesiredCapacity( String scalingGroupId, int capacity ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "AutoScaling.setDesiredCapacity");
        try {
            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.SET_DESIRED_CAPACITY);
            EC2Method method;

            parameters.put("AutoScalingGroupName", scalingGroupId);
            parameters.put("DesiredCapacity", String.valueOf(capacity));
            method = new EC2Method(SERVICE_ID, getProvider(), parameters);
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
    public void updateTags( @Nonnull String[] providerScalingGroupIds, @Nonnull AutoScalingTag... tags ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "AutoScaling.removeTags");
        try {
            handleTagRequest(EC2Method.UPDATE_AUTO_SCALING_GROUP_TAGS, providerScalingGroupIds, tags);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeTags( @Nonnull String[] providerScalingGroupIds, @Nonnull AutoScalingTag... tags ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "AutoScaling.removeTags");
        try {
            handleTagRequest(EC2Method.DELETE_AUTO_SCALING_GROUP_TAGS, providerScalingGroupIds, tags);
        }
        finally {
            APITrace.end();
        }
    }

    private Collection<AutoScalingTag> getTagsForDelete( @Nullable Collection<AutoScalingTag> all, @Nonnull AutoScalingTag[] tags ) {
        Collection<AutoScalingTag> result = null;
        if( all != null ) {
            result = new ArrayList<AutoScalingTag>();
            for( AutoScalingTag tag : all ) {
                if( !isTagInTags(tag, tags) ) {
                    result.add(tag);
                }
            }
        }
        return result;
    }

    private boolean isTagInTags( @Nonnull AutoScalingTag tag, @Nonnull AutoScalingTag[] tags ) {
        for( AutoScalingTag t : tags ) {
            if( t.getKey().equals(tag.getKey()) ) {
                return true;
            }
        }
        return false;
    }

    private void handleTagRequest( @Nonnull String methodName, @Nonnull String[] providerScalingGroupIds, @Nonnull AutoScalingTag... tags ) throws CloudException, InternalException {
        Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), methodName);
        EC2Method method;

        addAutoScalingTagParameters(parameters, providerScalingGroupIds, tags);

        if( parameters.size() == 0 ) {
            return;
        }

        method = new EC2Method(SERVICE_ID, getProvider(), parameters);
        try {
            method.invoke();
        }
        catch( EC2Exception e ) {
            logger.error(e.getSummary());
            throw new CloudException(e);
        }
    }

    @Override
    public void setNotificationConfig( @Nonnull String scalingGroupId, @Nonnull String topicARN, @Nonnull String[] notificationTypes ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "AutoScaling.setNotificationConfig");
        try {

            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.PUT_NOTIFICATION_CONFIGURATION);

            parameters.put("AutoScalingGroupName", scalingGroupId);
            parameters.put("TopicARN", topicARN);
            for( int i = 0; i < notificationTypes.length; i++ ) {
                parameters.put("NotificationTypes.member." + ( i + 1 ), notificationTypes[i]);
            }

            EC2Method method = new EC2Method(SERVICE_ID, getProvider(), parameters);
            method.invoke();

        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Collection<AutoScalingGroupNotificationConfig> listNotificationConfigs( final String[] scalingGroupIds ) throws CloudException, InternalException {
        PopulatorThread<AutoScalingGroupNotificationConfig> populatorThread;

        getProvider().hold();
        populatorThread = new PopulatorThread<AutoScalingGroupNotificationConfig>(new JiteratorPopulator<AutoScalingGroupNotificationConfig>() {
            @Override
            public void populate( @Nonnull Jiterator<AutoScalingGroupNotificationConfig> autoScalingGroupNotificationConfigs ) throws Exception {
                try {
                    populateNotificationConfig(autoScalingGroupNotificationConfigs, null, scalingGroupIds);
                }
                finally {
                    getProvider().release();
                }
            }
        });

        populatorThread.populate();
        return populatorThread.getResult();
    }

    private void populateNotificationConfig( @Nonnull Jiterator<AutoScalingGroupNotificationConfig> asgNotificationConfig, @Nullable String token, String[] scalingGroupIds ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "AutoScaling.listNotificationConfigs");
        try {

            Map<String, String> parameters = getAutoScalingParameters(getProvider().getContext(), EC2Method.DESCRIBE_NOTIFICATION_CONFIGURATIONS);
            AWSCloud.addValueIfNotNull(parameters, "NextToken", token);
            for( int i = 0; i < scalingGroupIds.length; i++ ) {
                AWSCloud.addValueIfNotNull(parameters, "AutoScalingGroupNames.member." + ( i + 1 ), scalingGroupIds[i]);
            }

            EC2Method method = new EC2Method(SERVICE_ID, getProvider(), parameters);
            Document document = method.invoke();

            NodeList blocks = document.getElementsByTagName("NotificationConfigurations");
            if( blocks == null ) return;
            NodeList result = blocks.item(0).getChildNodes();

            for( int i = 0; i < result.getLength(); i++ ) {
                Node configNode = result.item(i);
                if( configNode.getNodeName().equalsIgnoreCase("member") ) {
                    AutoScalingGroupNotificationConfig nc = toASGNotificationConfig(configNode.getChildNodes());
                    if( nc != null ) asgNotificationConfig.push(nc);
                }
            }

            blocks = document.getElementsByTagName("NextToken");
            if( blocks != null && blocks.getLength() == 1 && blocks.item(0).hasChildNodes() ) {
                String nextToken = AWSCloud.getTextValue(blocks.item(0));
                populateNotificationConfig(asgNotificationConfig, nextToken, scalingGroupIds);
            }

        }
        finally {
            APITrace.end();
        }
    }

    private AutoScalingGroupNotificationConfig toASGNotificationConfig( NodeList attributes ) {
        if( attributes != null && attributes.getLength() != 0 ) {
            AutoScalingGroupNotificationConfig result = new AutoScalingGroupNotificationConfig();
            for( int i = 0; i < attributes.getLength(); i++ ) {
                Node attr = attributes.item(i);
                if( attr.getNodeName().equalsIgnoreCase("TopicARN") ) {
                    result.setTopic(attr.getFirstChild().getNodeValue());
                }
                else if( attr.getNodeName().equalsIgnoreCase("AutoScalingGroupName") ) {
                    result.setAutoScalingGroupName(attr.getFirstChild().getNodeValue());
                }
                else if( attr.getNodeName().equalsIgnoreCase("NotificationType") ) {
                    result.setNotificationType(attr.getFirstChild().getNodeValue());
                }
            }
            return result;
        }
        return null;
    }

    static private void addAutoScalingTagParameters( @Nonnull Map<String, String> parameters, @Nonnull String[] providerScalingGroupIds, @Nonnull AutoScalingTag... tags ) {
        /**
         * unlike EC2's regular CreateTags call, for autoscaling we must add a set of tag parameters for each auto scaling group
         * http://docs.aws.amazon.com/AutoScaling/latest/APIReference/API_CreateOrUpdateTags.html
         */
        int tagCounter = 1;
        for( int i = 0; i < providerScalingGroupIds.length; i++ ) {
            for( AutoScalingTag tag : tags ) {
                parameters.put("Tags.member." + tagCounter + ".ResourceType", "auto-scaling-group");
                parameters.put("Tags.member." + tagCounter + ".ResourceId", providerScalingGroupIds[i]);
                parameters.put("Tags.member." + tagCounter + ".Key", tag.getKey());
                if( tag.getValue() != null ) {
                    parameters.put("Tags.member." + tagCounter + ".Value", tag.getValue());
                }
                parameters.put("Tags.member." + tagCounter + ".PropagateAtLaunch", String.valueOf(tag.isPropagateAtLaunch()));

                tagCounter++;
            }
        }
    }

    private @Nullable ResourceStatus toGroupStatus( @Nullable Node item ) {
        if( item == null ) {
            return null;
        }
        NodeList attrs = item.getChildNodes();
        String groupId = null;

        for( int i = 0; i < attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);

            if( attr.getNodeName().equalsIgnoreCase("AutoScalingGroupName") ) {
                groupId = attr.getFirstChild().getNodeValue();
            }
        }
        if( groupId == null ) {
            return null;
        }
        return new ResourceStatus(groupId, true);
    }

    private @Nullable LaunchConfiguration toLaunchConfiguration( @Nullable Node item ) {
        if( item == null ) {
            return null;
        }
        LaunchConfiguration cfg = new LaunchConfiguration();
        NodeList attrs = item.getChildNodes();

        for( int i = 0; i < attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;

            name = attr.getNodeName();
            if( name.equalsIgnoreCase("ImageId") ) {
                if( attr.getFirstChild() != null ) {
                    cfg.setProviderImageId(attr.getFirstChild().getNodeValue());
                }
            }
            else if( name.equalsIgnoreCase("KeyName") ) {
                if( attr.getFirstChild() != null ) {
                    cfg.setProviderKeypairName(attr.getFirstChild().getNodeValue());
                }
            }
            else if( name.equalsIgnoreCase("LaunchConfigurationARN") ) {
                if( attr.getFirstChild() != null ) {
                    cfg.setId(attr.getFirstChild().getNodeValue());
                }
            }
            else if( name.equalsIgnoreCase("UserData") ) {
                if( attr.getFirstChild() != null ) {
                    cfg.setUserData(attr.getFirstChild().getNodeValue());
                }
            }
            else if( name.equalsIgnoreCase("InstanceType") ) {
                if( attr.getFirstChild() != null ) {
                    cfg.setServerSizeId(attr.getFirstChild().getNodeValue());
                }
            }
            else if( name.equalsIgnoreCase("LaunchConfigurationName") ) {
                if( attr.getFirstChild() != null ) {
                    String lcname = attr.getFirstChild().getNodeValue();

                    cfg.setProviderLaunchConfigurationId(lcname);
                    cfg.setName(lcname);
                }
            }
            else if( name.equalsIgnoreCase("CreatedTime") ) {
                SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

                try {
                    cfg.setCreationTimestamp(fmt.parse(attr.getFirstChild().getNodeValue()).getTime());
                }
                catch( ParseException e ) {
                    logger.error("Could not parse timestamp: " + attr.getFirstChild().getNodeValue());
                    cfg.setCreationTimestamp(System.currentTimeMillis());
                }
            }
            else if( name.equalsIgnoreCase("SecurityGroups") ) {
                String[] ids;

                if( attr.hasChildNodes() ) {
                    ArrayList<String> securityIds = new ArrayList<String>();
                    NodeList securityGroups = attr.getChildNodes();

                    for( int j = 0; j < securityGroups.getLength(); j++ ) {
                        Node securityGroup = securityGroups.item(j);

                        if( securityGroup.getNodeName().equalsIgnoreCase("member") ) {
                            if( securityGroup.hasChildNodes() ) {
                                securityIds.add(securityGroup.getFirstChild().getNodeValue());
                            }
                        }
                    }
                    ids = new String[securityIds.size()];
                    int j = 0;
                    for( String securityId : securityIds ) {
                        ids[j++] = securityId;
                    }
                }
                else {
                    ids = new String[0];
                }
                cfg.setProviderFirewallIds(ids);
            }
            else if( name.equalsIgnoreCase("IamInstanceProfile") ) {
                if( attr.getFirstChild() != null ) {
                    String providerId = attr.getFirstChild().getNodeValue();
                    cfg.setProviderRoleId(providerId);
                }
            }
            else if( "InstanceMonitoring".equals(name) && attr.hasChildNodes() ) {
                NodeList details = attr.getChildNodes();

                for( int j = 0; j < details.getLength(); j++ ) {
                    Node detail = details.item(j);

                    name = detail.getNodeName();
                    if( name.equals("Enabled") ) {
                        if( detail.hasChildNodes() ) {
                            String value = detail.getFirstChild().getNodeValue().trim();
                            cfg.setDetailedMonitoring(Boolean.valueOf(value));
                        }
                    }
                }
            }
            else if( name.equalsIgnoreCase("AssociatePublicIpAddress") ) {
                if( attr.getFirstChild() != null ) {
                    cfg.setAssociatePublicIPAddress(Boolean.valueOf(attr.getFirstChild().getNodeValue()));
                }
            }
            else if( name.equalsIgnoreCase("EbsOptimized") ) {
                if( attr.getFirstChild() != null ) {
                    cfg.setIoOptimized(Boolean.valueOf(attr.getFirstChild().getNodeValue()));
                }
            }
            else if( name.equalsIgnoreCase("BlockDeviceMappings") ) {
                ArrayList<VolumeAttachment> VAs = new ArrayList<VolumeAttachment>();
                VolumeAttachment[] VAArray;

                if( attr.hasChildNodes() ) {
                    NodeList blockDeviceMaps = attr.getChildNodes();

                    for( int j = 0; j < blockDeviceMaps.getLength(); j++ ) {
                        Node blockDeviceMap = blockDeviceMaps.item(j);

                        if( blockDeviceMap.getNodeName().equalsIgnoreCase("member") ) {

                            VolumeAttachment va = new VolumeAttachment();

                            if( blockDeviceMap.hasChildNodes() ) {
                                NodeList blockDeviceChildren = blockDeviceMap.getChildNodes();
                                for( int y = 0; y < blockDeviceChildren.getLength(); y++ ) {
                                    Node blockDeviceChild = blockDeviceChildren.item(y);

                                    String blockDeviceChildName = blockDeviceChild.getNodeName();

                                    if( blockDeviceChildName.equalsIgnoreCase("DeviceName") ) {

                                        String value = blockDeviceChild.getFirstChild().getNodeValue().trim();
                                        va.setDeviceId(value);

                                    }
                                    else if( blockDeviceChildName.equalsIgnoreCase("Ebs") ) {
                                        if( blockDeviceChild.hasChildNodes() ) {
                                            NodeList ebsChildren = blockDeviceChild.getChildNodes();
                                            int size = 0;
                                            String iops = null;
                                            String snapShotId = null;
                                            String volumeType = null;

                                            for( int q = 0; q < ebsChildren.getLength(); q++ ) {
                                                Node ebsChild = ebsChildren.item(q);
                                                String ebsChildName = ebsChild.getNodeName();

                                                if( ebsChildName.equalsIgnoreCase("Iops") ) {
                                                    iops = ebsChild.getFirstChild().getNodeValue().trim();
                                                }
                                                else if( ebsChildName.equalsIgnoreCase("VolumeSize") ) {
                                                    String value = ebsChild.getFirstChild().getNodeValue().trim();
                                                    size = Integer.parseInt(value);
                                                }
                                                else if( ebsChildName.equalsIgnoreCase("SnapshotId") ) {
                                                    snapShotId = ebsChild.getFirstChild().getNodeValue().trim();
                                                }
                                                else if( ebsChildName.equalsIgnoreCase("VolumeType") ) {
                                                    volumeType = ebsChild.getFirstChild().getNodeValue().trim();
                                                }
                                            }
                                            VolumeCreateOptions vco = VolumeCreateOptions.getInstance(new Storage<Gigabyte>(size, Storage.GIGABYTE), "", "");
                                            try {
                                                vco.setIops(Integer.parseInt(iops));
                                            }
                                            catch( NumberFormatException nfe ) {
                                                vco.setIops(0);
                                            }
                                            vco.setSnapshotId(snapShotId);
                                            vco.setVolumeProductId(volumeType);
                                            va.setVolumeToCreate(vco);
                                        }
                                    }
                                }
                                VAs.add(va);
                            }
                        }
                    }
                    VAArray = new VolumeAttachment[VAs.size()];
                    int v = 0;
                    for( VolumeAttachment va : VAs ) {
                        VAArray[v++] = va;
                    }
                }
                else {
                    VAArray = new VolumeAttachment[0];
                }
                cfg.setVolumeAttachment(VAArray);
            }
        }
        return cfg;
    }

    private @Nullable ResourceStatus toLCStatus( @Nullable Node item ) {
        if( item == null ) {
            return null;
        }
        NodeList attrs = item.getChildNodes();
        String lcId = null;

        for( int i = 0; i < attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);

            if( attr.getNodeName().equalsIgnoreCase("LaunchConfigurationName") ) {
                lcId = attr.getFirstChild().getNodeValue();
            }
        }
        if( lcId == null ) {
            return null;
        }
        return new ResourceStatus(lcId, true);
    }

    private @Nullable ScalingGroup toScalingGroup( @Nonnull ProviderContext ctx, @Nullable Node item ) {
        if( item == null ) {
            return null;
        }
        NodeList attrs = item.getChildNodes();
        ScalingGroup group = new ScalingGroup();
        group.setProviderOwnerId(ctx.getAccountNumber());
        group.setProviderRegionId(ctx.getRegionId());
        for( int i = 0; i < attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;

            name = attr.getNodeName();
            if( name.equalsIgnoreCase("MinSize") ) {
                group.setMinServers(Integer.parseInt(attr.getFirstChild().getNodeValue()));
            }
            else if( name.equalsIgnoreCase("MaxSize") ) {
                group.setMaxServers(Integer.parseInt(attr.getFirstChild().getNodeValue()));
            }
            else if( name.equalsIgnoreCase("DefaultCooldown") ) {
                group.setDefaultCooldown(Integer.parseInt(attr.getFirstChild().getNodeValue()));
            }
            else if( name.equalsIgnoreCase("CreatedTime") ) {
                SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

                try {
                    group.setCreationTimestamp(fmt.parse(attr.getFirstChild().getNodeValue()).getTime());
                }
                catch( ParseException e ) {
                    logger.error("Could not parse timestamp: " + attr.getFirstChild().getNodeValue());
                    group.setCreationTimestamp(System.currentTimeMillis());
                }
            }
            else if( name.equalsIgnoreCase("DesiredCapacity") ) {
                group.setTargetCapacity(Integer.parseInt(attr.getFirstChild().getNodeValue()));
            }
            else if( name.equalsIgnoreCase("LaunchConfigurationName") ) {
                group.setProviderLaunchConfigurationId(attr.getFirstChild().getNodeValue());
            }
            else if( name.equalsIgnoreCase("AutoScalingGroupARN") ) {
                group.setId(attr.getFirstChild().getNodeValue());
            }
            else if( name.equalsIgnoreCase("HealthCheckGracePeriod") ) {
                group.setHealthCheckGracePeriod(Integer.parseInt(attr.getFirstChild().getNodeValue()));
            }
            else if( name.equalsIgnoreCase("HealthCheckType") ) {
                group.setHealthCheckType(attr.getFirstChild().getNodeValue());
            }
            else if( name.equalsIgnoreCase("Status") ) {
                group.setStatus(attr.getFirstChild().getNodeValue());
            }
            else if( name.equalsIgnoreCase("VPCZoneIdentifier") ) {
                Node subnetChild = attr.getFirstChild();
                if( subnetChild != null ) {
                    String subnets = subnetChild.getNodeValue();
                    group.setProviderSubnetIds(subnets.contains(",") ? subnets.split("\\s*,\\s*") : new String[]{subnets});
                }
            }
            else if( name.equalsIgnoreCase("AutoScalingGroupName") ) {
                String gname = attr.getFirstChild().getNodeValue();

                group.setProviderScalingGroupId(gname);
                group.setName(gname);
                group.setDescription(gname);
            }
            else if( name.equalsIgnoreCase("Instances") ) {
                String[] ids;

                if( attr.hasChildNodes() ) {
                    ArrayList<String> instanceIds = new ArrayList<String>();
                    NodeList instances = attr.getChildNodes();

                    for( int j = 0; j < instances.getLength(); j++ ) {
                        Node instance = instances.item(j);

                        if( instance.getNodeName().equals("member") ) {
                            if( instance.hasChildNodes() ) {
                                NodeList items = instance.getChildNodes();

                                for( int k = 0; k < items.getLength(); k++ ) {
                                    Node val = items.item(k);

                                    if( val.getNodeName().equalsIgnoreCase("InstanceId") ) {
                                        instanceIds.add(val.getFirstChild().getNodeValue());
                                    }
                                }
                            }
                        }
                    }
                    ids = new String[instanceIds.size()];
                    int j = 0;
                    for( String id : instanceIds ) {
                        ids[j++] = id;
                    }
                }
                else {
                    ids = new String[0];
                }
                group.setProviderServerIds(ids);
            }
            else if( name.equalsIgnoreCase("AvailabilityZones") ) {
                String[] ids;

                if( attr.hasChildNodes() ) {
                    ArrayList<String> zoneIds = new ArrayList<String>();
                    NodeList zones = attr.getChildNodes();

                    for( int j = 0; j < zones.getLength(); j++ ) {
                        Node zone = zones.item(j);

                        if( zone.getNodeName().equalsIgnoreCase("member") ) {
                            zoneIds.add(zone.getFirstChild().getNodeValue());
                        }
                    }
                    ids = new String[zoneIds.size()];
                    int j = 0;
                    for( String zoneId : zoneIds ) {
                        ids[j++] = zoneId;
                    }
                }
                else {
                    ids = new String[0];
                }
                group.setProviderDataCenterIds(ids);
            }
            else if( name.equalsIgnoreCase("EnabledMetrics") ) {
                String[] names;

                if( attr.hasChildNodes() ) {
                    ArrayList<String> metricNames = new ArrayList<String>();
                    NodeList metrics = attr.getChildNodes();

                    for( int j = 0; j < metrics.getLength(); j++ ) {
                        Node metric = metrics.item(j);

                        if( metric.getNodeName().equalsIgnoreCase("Metric") ) {
                            metricNames.add(metric.getFirstChild().getNodeValue());
                        }
                    }
                    names = new String[metricNames.size()];
                    int j = 0;
                    for( String metricName : metricNames ) {
                        names[j++] = metricName;
                    }
                }
                else {
                    names = new String[0];
                }
                group.setEnabledMetrics(names);
            }
            else if( name.equalsIgnoreCase("LoadBalancerNames") ) {
                String[] names;

                if( attr.hasChildNodes() ) {
                    ArrayList<String> lbNames = new ArrayList<String>();
                    NodeList loadBalancers = attr.getChildNodes();

                    for( int j = 0; j < loadBalancers.getLength(); j++ ) {
                        Node lb = loadBalancers.item(j);

                        if( lb.getNodeName().equalsIgnoreCase("member") ) {
                            lbNames.add(lb.getFirstChild().getNodeValue());
                        }
                    }
                    names = new String[lbNames.size()];
                    int j = 0;
                    for( String lbName : lbNames ) {
                        names[j++] = lbName;
                    }
                }
                else {
                    names = new String[0];
                }
                group.setProviderLoadBalancerNames(names);
            }
            else if( name.equalsIgnoreCase("SuspendedProcesses") ) {
                Collection<String[]> processes;

                if( attr.hasChildNodes() ) {
                    ArrayList<String[]> processList = new ArrayList<String[]>();
                    NodeList processesList = attr.getChildNodes();

                    for( int j = 0; j < processesList.getLength(); j++ ) {
                        Node processParent = processesList.item(j);
                        ArrayList<String> theProcess = new ArrayList<String>();

                        if( processParent.getNodeName().equals("member") ) {
                            if( processParent.hasChildNodes() ) {
                                NodeList items = processParent.getChildNodes();

                                for( int k = 0; k < items.getLength(); k++ ) {
                                    Node val = items.item(k);
                                    if( val.getNodeName().equalsIgnoreCase("SuspensionReason") ) {
                                        theProcess.add(val.getFirstChild().getNodeValue());
                                    }
                                    // seems to come second...
                                    if( val.getNodeName().equalsIgnoreCase("ProcessName") ) {
                                        theProcess.add(val.getFirstChild().getNodeValue());
                                    }
                                }
                            }
                        }
                        if( theProcess.size() > 0 ) {
                            String[] stringArr = new String[theProcess.size()];
                            stringArr = theProcess.toArray(stringArr);
                            processList.add(stringArr);
                        }
                    }
                    processes = processList;
                }
                else {
                    processes = new ArrayList<String[]>();
                }
                group.setSuspendedProcesses(processes);
            }
            else if( name.equalsIgnoreCase("TerminationPolicies") ) {
                String[] policies;

                if( attr.hasChildNodes() ) {
                    ArrayList<String> subPolicies = new ArrayList<String>();
                    NodeList policyList = attr.getChildNodes();

                    for( int j = 0; j < policyList.getLength(); j++ ) {
                        Node lb = policyList.item(j);

                        if( lb.getNodeName().equalsIgnoreCase("member") ) {
                            subPolicies.add(lb.getFirstChild().getNodeValue());
                        }
                    }
                    policies = new String[subPolicies.size()];
                    int j = 0;
                    for( String policyString : subPolicies ) {
                        policies[j++] = policyString;
                    }
                }
                else {
                    policies = new String[0];
                }
                group.setTerminationPolicies(policies);
            }
            else if( name.equalsIgnoreCase("Tags") && attr.hasChildNodes() ) {
                ArrayList<AutoScalingTag> tags = new ArrayList<AutoScalingTag>();
                NodeList tagList = attr.getChildNodes();

                for( int j = 0; j < tagList.getLength(); j++ ) {
                    Node parent = tagList.item(j);
                    if( parent.getNodeName().equals("member") && parent.hasChildNodes() ) {
                        String key = null;
                        String value = null;
                        Boolean propagateAtLaunch = null;

                        NodeList memberNodes = parent.getChildNodes();

                        for( int k = 0; k < memberNodes.getLength(); k++ ) {
                            Node val = memberNodes.item(k);
                            if( val.getNodeName().equalsIgnoreCase("Key") ) {
                                key = AWSCloud.getTextValue(val);
                            }
                            else if( val.getNodeName().equalsIgnoreCase("Value") ) {
                                value = AWSCloud.getTextValue(val);
                            }
                            else if( val.getNodeName().equalsIgnoreCase("PropagateAtLaunch") ) {
                                propagateAtLaunch = AWSCloud.getBooleanValue(val);
                            }
                        }
                        tags.add(new AutoScalingTag(key, value, propagateAtLaunch));
                    }
                }

                if( tags.size() > 0 ) {
                    group.setTags(tags.toArray(new AutoScalingTag[tags.size()]));
                }
            }
        }
        return group;
    }

    private @Nullable ScalingPolicy toScalingPolicy( @Nullable Node item ) {
        if( item == null ) {
            return null;
        }
        ScalingPolicy sp = new ScalingPolicy();
        NodeList attrs = item.getChildNodes();

        for( int i = 0; i < attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;

            name = attr.getNodeName();
            if( name.equalsIgnoreCase("AdjustmentType") ) {
                if( attr.getFirstChild() != null ) {
                    sp.setAdjustmentType(attr.getFirstChild().getNodeValue());
                }
            }
            else if( name.equalsIgnoreCase("Alarms") ) {
                Collection<Alarm> alarms;

                if( attr.hasChildNodes() ) {
                    ArrayList<Alarm> alarmList = new ArrayList<Alarm>();
                    NodeList alarmsList = attr.getChildNodes();

                    for( int j = 0; j < alarmsList.getLength(); j++ ) {
                        Node alarmParent = alarmsList.item(j);
                        Alarm anAlarm = new Alarm();

                        if( alarmParent.getNodeName().equals("member") ) {
                            if( alarmParent.hasChildNodes() ) {
                                NodeList items = alarmParent.getChildNodes();

                                for( int k = 0; k < items.getLength(); k++ ) {
                                    Node val = items.item(k);
                                    if( val.getNodeName().equalsIgnoreCase("AlarmARN") ) {
                                        anAlarm.setId(val.getFirstChild().getNodeValue());
                                    }
                                    if( val.getNodeName().equalsIgnoreCase("AlarmName") ) {
                                        anAlarm.setName(val.getFirstChild().getNodeValue());
                                    }
                                }
                                alarmList.add(anAlarm);
                            }
                        }
                    }
                    alarms = alarmList;
                }
                else {
                    alarms = new ArrayList<Alarm>();
                }
                Alarm[] alarmArr = new Alarm[alarms.size()];
                alarmArr = alarms.toArray(alarmArr);
                sp.setAlarms(alarmArr);
            }
            else if( name.equalsIgnoreCase("AutoScalingGroupName") ) {
                if( attr.getFirstChild() != null ) {
                    sp.setAutoScalingGroupName(attr.getFirstChild().getNodeValue());
                }
            }
            else if( name.equalsIgnoreCase("Cooldown") ) {
                if( attr.getFirstChild() != null ) {
                    sp.setCoolDown(Integer.parseInt(attr.getFirstChild().getNodeValue()));
                }
            }
            else if( name.equalsIgnoreCase("MinAdjustmentStep") ) {
                if( attr.getFirstChild() != null ) {
                    sp.setMinAdjustmentStep(Integer.parseInt(attr.getFirstChild().getNodeValue()));
                }
            }
            else if( name.equalsIgnoreCase("PolicyARN") ) {
                if( attr.getFirstChild() != null ) {
                    sp.setId(attr.getFirstChild().getNodeValue());
                }
            }
            else if( name.equalsIgnoreCase("PolicyName") ) {
                if( attr.getFirstChild() != null ) {
                    sp.setName(attr.getFirstChild().getNodeValue());
                }
            }
            else if( name.equalsIgnoreCase("ScalingAdjustment") ) {
                if( attr.getFirstChild() != null ) {
                    sp.setScalingAdjustment(Integer.parseInt(attr.getFirstChild().getNodeValue()));
                }
            }
        }
        return sp;
    }

}
