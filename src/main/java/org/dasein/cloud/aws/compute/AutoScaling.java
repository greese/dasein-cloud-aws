/**
 * Copyright (C) 2009-2013 Dell, Inc.
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
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.compute.*;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.util.APITrace;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class AutoScaling implements AutoScalingSupport {
    static private final Logger logger = Logger.getLogger(AutoScaling.class);

    private AWSCloud provider = null;

    AutoScaling(AWSCloud provider) {
        this.provider = provider;
    }

    @Override
    public String createAutoScalingGroup(@Nonnull String name, @Nonnull String launchConfigurationId, @Nonnull Integer minServers, @Nonnull Integer maxServers, @Nullable Integer cooldown, @Nullable String[] loadBalancerIds, @Nullable Integer desiredCapacity, @Nullable Integer healthCheckGracePeriod, @Nullable String healthCheckType, @Nullable String vpcZones, @Nullable String ... zoneIds) throws InternalException, CloudException {
        APITrace.begin(provider, "AutoScaling.createAutoScalingGroup");
        try {
            Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.CREATE_AUTO_SCALING_GROUP);
            EC2Method method;

            if( minServers < 0 ) {
                minServers = 0;
            }
            if( maxServers < minServers ) {
                maxServers = minServers;
            }
            parameters.put("AutoScalingGroupName", name);
            parameters.put("LaunchConfigurationName", launchConfigurationId);
            parameters.put("MinSize", String.valueOf(minServers));
            parameters.put("MaxSize", String.valueOf(maxServers));
            if(cooldown != null) {
              parameters.put("DefaultCooldown", String.valueOf(cooldown));
            }
            if(desiredCapacity != null) {
              parameters.put("DesiredCapacity", String.valueOf(desiredCapacity));
            }
            if(healthCheckGracePeriod != null) {
              parameters.put("HealthCheckGracePeriod", String.valueOf(healthCheckGracePeriod));
            }
            if(healthCheckType != null) {
              parameters.put("HealthCheckType", healthCheckType);
            }
            if(vpcZones != null) {
              parameters.put("VPCZoneIdentifier", vpcZones);
            }
            if( zoneIds != null ) {
              int i = 1;
              for( String zoneId : zoneIds ) {
                  parameters.put("AvailabilityZones.member." + (i++), zoneId);
              }
            }
            if( loadBalancerIds != null ) {
              int x = 1;
              for( String lbId : loadBalancerIds ) {
                parameters.put("LoadBalancerNames.member." + (x++), lbId);
              }
            }
            method = new EC2Method(provider, getAutoScalingUrl(), parameters);
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

    @Override
    public void updateAutoScalingGroup(@Nonnull String scalingGroupId, @Nullable String launchConfigurationId, @Nonnegative Integer minServers, @Nonnegative Integer maxServers, @Nullable Integer cooldown, @Nullable Integer desiredCapacity, @Nullable Integer healthCheckGracePeriod, @Nullable String healthCheckType, @Nullable String vpcZones, @Nullable String ... zoneIds) throws InternalException, CloudException {
      APITrace.begin(provider, "AutoScaling.updateAutoScalingGroup");
      try {
        Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.UPDATE_AUTO_SCALING_GROUP);
        EC2Method method;

        if(scalingGroupId != null) {
          parameters.put("AutoScalingGroupName", scalingGroupId);
        }
        if(launchConfigurationId != null) {
          parameters.put("LaunchConfigurationName", launchConfigurationId);
        }
        if(minServers != null) {
          if( minServers < 0 ) {
            minServers = 0;
          }
          parameters.put("MinSize", String.valueOf(minServers));
        }
        if(maxServers != null) {
          if( minServers != null && maxServers < minServers ) {
            maxServers = minServers;
          }
          parameters.put("MaxSize", String.valueOf(maxServers));
        }
        if(cooldown != null) {
          parameters.put("DefaultCooldown", String.valueOf(cooldown));
        }
        if(desiredCapacity != null) {
          parameters.put("DesiredCapacity", String.valueOf(desiredCapacity));
        }
        if(healthCheckGracePeriod != null) {
          parameters.put("HealthCheckGracePeriod", String.valueOf(healthCheckGracePeriod));
        }
        if(healthCheckType != null) {
          parameters.put("HealthCheckType", healthCheckType);
        }
        if(vpcZones != null) {
          parameters.put("VPCZoneIdentifier", vpcZones);
        }
        if(zoneIds != null) {
          int i = 1;
          for( String zoneId : zoneIds ) {
            parameters.put("AvailabilityZones.member." + (i++), zoneId);
          }
        }
        method = new EC2Method(provider, getAutoScalingUrl(), parameters);
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
    public String createLaunchConfiguration(String name, String imageId, VirtualMachineProduct size, String keyPairName, String userData, String ... firewalls) throws InternalException, CloudException {
        APITrace.begin(provider, "AutoScaling.createLaunchConfigursation");
        try {
            Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.CREATE_LAUNCH_CONFIGURATION);
            EC2Method method;

            parameters.put("LaunchConfigurationName", name);
            parameters.put("ImageId", imageId);
            if(keyPairName != null) {
              parameters.put("KeyName", keyPairName);
            }
            if(userData != null) {
              parameters.put("UserData", userData);
            }
            parameters.put("InstanceType", size.getProviderProductId());
            int i = 1;
            for( String fw : firewalls ) {
                parameters.put("SecurityGroups.member." + (i++), fw);
            }
            method = new EC2Method(provider, getAutoScalingUrl(), parameters);
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

    @Override
    public void deleteAutoScalingGroup(String providerAutoScalingGroupId) throws InternalException, CloudException {
        APITrace.begin(provider, "AutoScaling.deleteAutoScalingGroup");
        try {
            Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.DELETE_AUTO_SCALING_GROUP);
            EC2Method method;

            parameters.put("AutoScalingGroupName", providerAutoScalingGroupId);
            method = new EC2Method(provider, getAutoScalingUrl(), parameters);
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
    public void deleteLaunchConfiguration(String providerLaunchConfigurationId) throws InternalException, CloudException {
        APITrace.begin(provider, "AutoScaling.deleteLaunchConfiguration");
        try {
            Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.DELETE_LAUNCH_CONFIGURATION);
            EC2Method method;

            parameters.put("LaunchConfigurationName", providerLaunchConfigurationId);
            method = new EC2Method(provider, getAutoScalingUrl(), parameters);
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
    public String setTrigger(String name, String scalingGroupId, String statistic, String unitOfMeasure, String metric, int periodInSeconds, double lowerThreshold, double upperThreshold, int lowerIncrement, boolean lowerIncrementAbsolute, int upperIncrement, boolean upperIncrementAbsolute, int breachDuration) throws InternalException, CloudException {
        APITrace.begin(provider, "AutoScaling.setTrigger");
        try {
            Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.CREATE_OR_UPDATE_SCALING_TRIGGER);
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
            method = new EC2Method(provider, getAutoScalingUrl(), parameters);
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

    private Map<String,String> getAutoScalingParameters(ProviderContext ctx, String action) throws InternalException {
        APITrace.begin(provider, "AutoScaling.getAutoScalingParameters");
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
            parameters.put(AWSCloud.P_VERSION, provider.getAutoScaleVersion());
            return parameters;
        }
        finally {
            APITrace.end();
        }
    }

    private String getAutoScalingUrl() throws CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context has been set for this request");
        }
        return "https://autoscaling." + ctx.getRegionId() + ".amazonaws.com";
    }

    @Override
    public LaunchConfiguration getLaunchConfiguration(String providerLaunchConfigurationId) throws CloudException, InternalException {
        APITrace.begin(provider, "AutoScaling.getLaunchConfiguration");
        try {
            Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.DESCRIBE_LAUNCH_CONFIGURATIONS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("LaunchConfigurationNames.member.1", providerLaunchConfigurationId);
            method = new EC2Method(provider, getAutoScalingUrl(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("LaunchConfigurations");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
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
    public ScalingGroup getScalingGroup(String providerScalingGroupId) throws CloudException, InternalException {
        APITrace.begin(provider, "AutoScaling.getScalingGroup");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context has been set for this request");
            }
            Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.DESCRIBE_AUTO_SCALING_GROUPS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("AutoScalingGroupNames.member.1", providerScalingGroupId);
            method = new EC2Method(provider, getAutoScalingUrl(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("AutoScalingGroups");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList members = blocks.item(i).getChildNodes();

                for( int j=0; j<members.getLength(); j++ ) {
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
        APITrace.begin(provider, "AutoScaling.isSubscribed");
        try {
            Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.DESCRIBE_AUTO_SCALING_GROUPS);
            EC2Method method;

            method = new EC2Method(provider, getAutoScalingUrl(), parameters);
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
    public void suspendAutoScaling(String providerScalingGroupId, String[] processesToSuspend) throws CloudException, InternalException {
      APITrace.begin(provider, "AutoScaling.suspendAutoScaling");
      try {
        Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.SUSPEND_AUTO_SCALING_GROUP);
        EC2Method method;

        parameters.put("AutoScalingGroupName", providerScalingGroupId);
        int x = 1;
        if(processesToSuspend != null) {
          for( String process : processesToSuspend ) {
            parameters.put("ScalingProcesses.member." + (x++), process);
          }
        }
        method = new EC2Method(provider, getAutoScalingUrl(), parameters);
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
    public void resumeAutoScaling(String providerScalingGroupId, String[] processesToResume) throws CloudException, InternalException {
      APITrace.begin(provider, "AutoScaling.resumeAutoScaling");
      try {
        Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.RESUME_AUTO_SCALING_GROUP);
        EC2Method method;

        parameters.put("AutoScalingGroupName", providerScalingGroupId);
        int x = 1;
        if(processesToResume != null) {
          for( String process : processesToResume ) {
            parameters.put("ScalingProcesses.member." + (x++), process);
          }
        }
        method = new EC2Method(provider, getAutoScalingUrl(), parameters);
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
    public String updateScalingPolicy(String policyName, String adjustmentType, String autoScalingGroupName, Integer cooldown, Integer minAdjustmentStep, Integer scalingAdjustment) throws CloudException, InternalException {
      APITrace.begin(provider, "AutoScaling.updateScalingPolicy");
      try {
        Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.PUT_SCALING_POLICY);
        EC2Method method;
        NodeList blocks;
        Document doc;

        parameters.put("PolicyName", policyName);
        parameters.put("AdjustmentType", adjustmentType);
        parameters.put("AutoScalingGroupName", autoScalingGroupName);
        if(cooldown != null){
          parameters.put("Cooldown", String.valueOf(cooldown));
        }
        if(minAdjustmentStep != null){
          parameters.put("MinAdjustmentStep", String.valueOf(minAdjustmentStep));
        }
        parameters.put("ScalingAdjustment", String.valueOf(scalingAdjustment));
        method = new EC2Method(provider, getAutoScalingUrl(), parameters);
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
    public void deleteScalingPolicy(@Nonnull String policyName, @Nullable String autoScalingGroupName) throws InternalException, CloudException {
      APITrace.begin(provider, "AutoScaling.deleteScalingPolicy");
      try {
        Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.DELETE_SCALING_POLICY);
        EC2Method method;

        parameters.put("PolicyName", policyName);
        if(autoScalingGroupName != null) {
          parameters.put("AutoScalingGroupName", autoScalingGroupName);
        }
        method = new EC2Method(provider, getAutoScalingUrl(), parameters);
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
    public Collection<ScalingPolicy> listScalingPolicies(@Nullable String autoScalingGroupName) throws CloudException, InternalException {
      APITrace.begin(provider, "AutoScaling.getScalingPolicies");
      try {
        Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.DESCRIBE_SCALING_POLICIES);
        ArrayList<ScalingPolicy> list = new ArrayList<ScalingPolicy>();
        EC2Method method;
        NodeList blocks;
        Document doc;

        if(autoScalingGroupName != null) {
          parameters.put("AutoScalingGroupName", autoScalingGroupName);
        }
        method = new EC2Method(provider, getAutoScalingUrl(), parameters);
        try {
          doc = method.invoke();
        }
        catch( EC2Exception e ) {
          logger.error(e.getSummary());
          throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("ScalingPolicies");
        for( int i=0; i<blocks.getLength(); i++ ) {
          NodeList items = blocks.item(i).getChildNodes();

          for( int j=0; j<items.getLength(); j++ ) {
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
    public ScalingPolicy getScalingPolicy(@Nonnull String policyName) throws CloudException, InternalException {
      APITrace.begin(provider, "AutoScaling.getScalingPolicy");
      try {
        Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.DESCRIBE_SCALING_POLICIES);
        EC2Method method;
        NodeList blocks;
        Document doc;

        parameters.put("PolicyNames.member.1", policyName);

        method = new EC2Method(provider, getAutoScalingUrl(), parameters);
        try {
          doc = method.invoke();
        }
        catch( EC2Exception e ) {
          logger.error(e.getSummary());
          throw new CloudException(e);
        }
        ScalingPolicy sp = null;
        blocks = doc.getElementsByTagName("ScalingPolicies");
        for( int i=0; i<blocks.getLength(); i++ ) {
          NodeList items = blocks.item(i).getChildNodes();

          for( int j=0; j<items.getLength(); j++ ) {
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
        APITrace.begin(provider, "AutoScaling.listLaunchConfigurationStatus");
        try {
            Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.DESCRIBE_LAUNCH_CONFIGURATIONS);
            ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();
            EC2Method method;
            NodeList blocks;
            Document doc;

            method = new EC2Method(provider, getAutoScalingUrl(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("LaunchConfigurations");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
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
        APITrace.begin(provider, "AutoScaling.listLaunchConfigurations");
        try {
            Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.DESCRIBE_LAUNCH_CONFIGURATIONS);
            ArrayList<LaunchConfiguration> list = new ArrayList<LaunchConfiguration>();
            EC2Method method;
            NodeList blocks;
            Document doc;

            method = new EC2Method(provider, getAutoScalingUrl(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("LaunchConfigurations");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
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
        APITrace.begin(provider, "AutoScaling.listScalingGroupStatus");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context has been set for this request");
            }
            ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();

            Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.DESCRIBE_AUTO_SCALING_GROUPS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            method = new EC2Method(provider, getAutoScalingUrl(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("AutoScalingGroups");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
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

    @Override
    public Collection<ScalingGroup> listScalingGroups() throws CloudException, InternalException {
        APITrace.begin(provider, "AutoScaling.listScalingGroups");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context has been set for this request");
            }
            ArrayList<ScalingGroup> list = new ArrayList<ScalingGroup>();

            Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.DESCRIBE_AUTO_SCALING_GROUPS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            method = new EC2Method(provider, getAutoScalingUrl(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("AutoScalingGroups");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("member") ) {
                        ScalingGroup group = toScalingGroup(ctx, item);

                        if( group != null ) {
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
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        if( action.equals(AutoScalingSupport.ANY) ) {
            return new String[] { EC2Method.AUTOSCALING_PREFIX + "*" };
        }
        if( action.equals(AutoScalingSupport.CREATE_LAUNCH_CONFIGURATION) ) {
            return new String[] { EC2Method.AUTOSCALING_PREFIX + EC2Method.CREATE_LAUNCH_CONFIGURATION };
        }
        else if( action.equals(AutoScalingSupport.CREATE_SCALING_GROUP) ) {
            return new String[] { EC2Method.AUTOSCALING_PREFIX + EC2Method.CREATE_AUTO_SCALING_GROUP };
        }
        else if( action.equals(AutoScalingSupport.GET_LAUNCH_CONFIGURATION) ) {
            return new String[] { EC2Method.AUTOSCALING_PREFIX + EC2Method.DESCRIBE_LAUNCH_CONFIGURATIONS };
        }
        else if( action.equals(AutoScalingSupport.GET_SCALING_GROUP) ) {
            return new String[] { EC2Method.AUTOSCALING_PREFIX + EC2Method.DESCRIBE_AUTO_SCALING_GROUPS };
        }
        else if( action.equals(AutoScalingSupport.LIST_LAUNCH_CONFIGURATION) ) {
            return new String[] { EC2Method.AUTOSCALING_PREFIX + EC2Method.DESCRIBE_LAUNCH_CONFIGURATIONS };
        }
        else if( action.equals(AutoScalingSupport.LIST_SCALING_GROUP) ) {
            return new String[] { EC2Method.AUTOSCALING_PREFIX + EC2Method.DESCRIBE_AUTO_SCALING_GROUPS };
        }
        else if( action.equals(AutoScalingSupport.REMOVE_LAUNCH_CONFIGURATION) ) {
            return new String[] { EC2Method.AUTOSCALING_PREFIX + EC2Method.DELETE_LAUNCH_CONFIGURATION };
        }
        else if( action.equals(AutoScalingSupport.REMOVE_SCALING_GROUP) ) {
            return new String[] { EC2Method.AUTOSCALING_PREFIX + EC2Method.DELETE_AUTO_SCALING_GROUP };
        }
        else if( action.equals(AutoScalingSupport.SET_CAPACITY) ) {
            return new String[] { EC2Method.AUTOSCALING_PREFIX + EC2Method.SET_DESIRED_CAPACITY };
        }
        else if( action.equals(AutoScalingSupport.SET_SCALING_TRIGGER) ) {
            return new String[] { EC2Method.AUTOSCALING_PREFIX + EC2Method.CREATE_OR_UPDATE_SCALING_TRIGGER };
        }
        else if( action.equals(AutoScalingSupport.UPDATE_SCALING_GROUP) ) {
            return new String[] { EC2Method.AUTOSCALING_PREFIX + EC2Method.UPDATE_AUTO_SCALING_GROUP };
        }
        else if( action.equals(AutoScalingSupport.SUSPEND_AUTO_SCALING_GROUP) ) {
          return new String[] { EC2Method.AUTOSCALING_PREFIX + EC2Method.SUSPEND_AUTO_SCALING_GROUP };
        }
        else if( action.equals(AutoScalingSupport.RESUME_AUTO_SCALING_GROUP) ) {
          return new String[] { EC2Method.AUTOSCALING_PREFIX + EC2Method.RESUME_AUTO_SCALING_GROUP };
        }
        else if( action.equals(AutoScalingSupport.PUT_SCALING_POLICY) ) {
          return new String[] { EC2Method.AUTOSCALING_PREFIX + EC2Method.PUT_SCALING_POLICY };
        }
        else if( action.equals(AutoScalingSupport.DELETE_SCALING_POLICY) ) {
          return new String[] { EC2Method.AUTOSCALING_PREFIX + EC2Method.DELETE_SCALING_POLICY };
        }
        else if( action.equals(AutoScalingSupport.LIST_SCALING_POLICIES) ) {
          return new String[] { EC2Method.AUTOSCALING_PREFIX + EC2Method.DESCRIBE_SCALING_POLICIES };
        }
        return new String[0];
    }

    @Override
    public void setDesiredCapacity(String scalingGroupId, int capacity) throws CloudException, InternalException {
        APITrace.begin(provider, "AutoScaling.setDesiredCapacity");
        try {
            Map<String,String> parameters = getAutoScalingParameters(provider.getContext(), EC2Method.SET_DESIRED_CAPACITY);
            EC2Method method;

            parameters.put("AutoScalingGroupName", scalingGroupId);
            parameters.put("DesiredCapacity", String.valueOf(capacity));
            method = new EC2Method(provider, getAutoScalingUrl(), parameters);
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

    private @Nullable ResourceStatus toGroupStatus( @Nullable Node item) {
        if( item == null ) {
            return null;
        }
        NodeList attrs = item.getChildNodes();
        String groupId = null;

        for( int i=0; i<attrs.getLength(); i++ ) {
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

    private @Nullable LaunchConfiguration toLaunchConfiguration(@Nullable Node item) {
        if( item == null ) {
            return null;
        }
        LaunchConfiguration cfg = new LaunchConfiguration();
        NodeList attrs = item.getChildNodes();

        for( int i=0; i<attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;

            name = attr.getNodeName();
            if( name.equalsIgnoreCase("ImageId") ) {
              if(attr.getFirstChild() != null){
                cfg.setProviderImageId(attr.getFirstChild().getNodeValue());
              }
            }
            else if( name.equalsIgnoreCase("KeyName") ) {
              if(attr.getFirstChild() != null){
                cfg.setProviderKeypairName(attr.getFirstChild().getNodeValue());
              }
            }
            else if( name.equalsIgnoreCase("LaunchConfigurationARN") ) {
              if(attr.getFirstChild() != null){
                cfg.setId(attr.getFirstChild().getNodeValue());
              }
            }
            else if( name.equalsIgnoreCase("UserData") ) {
              if(attr.getFirstChild() != null){
                cfg.setUserData(attr.getFirstChild().getNodeValue());
              }
            }
            else if( name.equalsIgnoreCase("InstanceType") ) {
              if(attr.getFirstChild() != null){
                cfg.setServerSizeId(attr.getFirstChild().getNodeValue());
              }
            }
            else if( name.equalsIgnoreCase("LaunchConfigurationName") ) {
              if(attr.getFirstChild() != null){
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

                    for( int j=0; j<securityGroups.getLength(); j++ ) {
                        Node securityGroup = securityGroups.item(j);

                        if( securityGroup.getNodeName().equalsIgnoreCase("member") ) {
                            if( securityGroup.hasChildNodes() ) {
                              securityIds.add(securityGroup.getFirstChild().getNodeValue());
                            }
                        }
                    }
                    ids = new String[securityIds.size()];
                    int j=0;
                    for( String securityId : securityIds ) {
                        ids[j++] = securityId;
                    }
                }
                else {
                    ids = new String[0];
                }
                cfg.setProviderFirewallIds(ids);
            }
        }
        return cfg;
    }

    private @Nullable ResourceStatus toLCStatus(@Nullable Node item) {
        if( item == null ) {
            return null;
        }
        NodeList attrs = item.getChildNodes();
        String lcId = null;

        for( int i=0; i<attrs.getLength(); i++ ) {
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

    private @Nullable ScalingGroup toScalingGroup(@Nonnull ProviderContext ctx, @Nullable Node item) {
        if( item == null ) {
            return null;
        }
        NodeList attrs = item.getChildNodes();
        ScalingGroup group = new ScalingGroup();
        group.setProviderOwnerId(ctx.getAccountNumber());
        group.setProviderRegionId(ctx.getRegionId());
        for( int i=0; i<attrs.getLength(); i++ ) {
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
                group.setDefaultCoolcown(Integer.parseInt(attr.getFirstChild().getNodeValue()));
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
              if(subnetChild != null) {
                group.setSubnetIds(subnetChild.getNodeValue());
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

                    for( int j=0; j<instances.getLength(); j++ ) {
                        Node instance = instances.item(j);

                        if( instance.getNodeName().equals("member") ) {
                            if( instance.hasChildNodes() ) {
                                NodeList items = instance.getChildNodes();

                                for( int k=0; k<items.getLength(); k++ ) {
                                    Node val = items.item(k);

                                    if( val.getNodeName().equalsIgnoreCase("InstanceId") ) {
                                        instanceIds.add(val.getFirstChild().getNodeValue());
                                    }
                                }
                            }
                        }
                    }
                    ids = new String[instanceIds.size()];
                    int j=0;
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

                    for( int j=0; j<zones.getLength(); j++ ) {
                        Node zone = zones.item(j);

                        if( zone.getNodeName().equalsIgnoreCase("member") ) {
                            zoneIds.add(zone.getFirstChild().getNodeValue());
                        }
                    }
                    ids = new String[zoneIds.size()];
                    int j=0;
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

                for( int j=0; j< metrics.getLength(); j++ ) {
                  Node metric = metrics.item(j);

                  if( metric.getNodeName().equalsIgnoreCase("Metric") ) {
                    metricNames.add(metric.getFirstChild().getNodeValue());
                  }
                }
                names = new String[metricNames.size()];
                int j=0;
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

                for( int j=0; j< loadBalancers.getLength(); j++ ) {
                  Node lb = loadBalancers.item(j);

                  if( lb.getNodeName().equalsIgnoreCase("member") ) {
                    lbNames.add(lb.getFirstChild().getNodeValue());
                  }
                }
                names = new String[lbNames.size()];
                int j=0;
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

                for( int j=0; j< processesList.getLength(); j++ ) {
                  Node processParent = processesList.item(j);
                  ArrayList<String> theProcess = new ArrayList<String>();

                  if( processParent.getNodeName().equals("member") ) {
                    if( processParent.hasChildNodes() ) {
                      NodeList items = processParent.getChildNodes();

                      for( int k=0; k<items.getLength(); k++ ) {
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
                  if(theProcess.size() > 0){
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

                for( int j=0; j< policyList.getLength(); j++ ) {
                  Node lb = policyList.item(j);

                  if( lb.getNodeName().equalsIgnoreCase("member") ) {
                    subPolicies.add(lb.getFirstChild().getNodeValue());
                  }
                }
                policies = new String[subPolicies.size()];
                int j=0;
                for( String policyString : subPolicies ) {
                  policies[j++] = policyString;
                }
              }
              else {
                policies = new String[0];
              }
              group.setTerminationPolicies(policies);
            }
        }
        return group;
    }

    private @Nullable ScalingPolicy toScalingPolicy(@Nullable Node item) {
      if( item == null ) {
        return null;
      }
      ScalingPolicy sp = new ScalingPolicy();
      NodeList attrs = item.getChildNodes();

      for( int i=0; i<attrs.getLength(); i++ ) {
        Node attr = attrs.item(i);
        String name;

        name = attr.getNodeName();
        if( name.equalsIgnoreCase("AdjustmentType") ) {
          if(attr.getFirstChild() != null){
            sp.setAdjustmentType(attr.getFirstChild().getNodeValue());
          }
        }
        else if( name.equalsIgnoreCase("Alarms") ) {
          Collection<Alarm> alarms;

          if( attr.hasChildNodes() ) {
            ArrayList<Alarm> alarmList = new ArrayList<Alarm>();
            NodeList alarmsList = attr.getChildNodes();

            for( int j=0; j< alarmsList.getLength(); j++ ) {
              Node alarmParent = alarmsList.item(j);
              Alarm anAlarm = new Alarm();

              if( alarmParent.getNodeName().equals("member") ) {
                if( alarmParent.hasChildNodes() ) {
                  NodeList items = alarmParent.getChildNodes();

                  for( int k=0; k<items.getLength(); k++ ) {
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
          if(attr.getFirstChild() != null){
            sp.setAutoScalingGroupName(attr.getFirstChild().getNodeValue());
          }
        }
        else if( name.equalsIgnoreCase("Cooldown") ) {
          if(attr.getFirstChild() != null){
            sp.setCoolDown(Integer.parseInt(attr.getFirstChild().getNodeValue()));
          }
        }
        else if( name.equalsIgnoreCase("MinAdjustmentStep") ) {
          if(attr.getFirstChild() != null){
            sp.setMinAdjustmentStep(Integer.parseInt(attr.getFirstChild().getNodeValue()));
          }
        }
        else if( name.equalsIgnoreCase("PolicyARN") ) {
          if(attr.getFirstChild() != null){
            sp.setId(attr.getFirstChild().getNodeValue());
          }
        }
        else if( name.equalsIgnoreCase("PolicyName") ) {
          if(attr.getFirstChild() != null){
            sp.setName(attr.getFirstChild().getNodeValue());
          }
        }
        else if( name.equalsIgnoreCase("ScalingAdjustment") ) {
          if(attr.getFirstChild() != null){
            sp.setScalingAdjustment(Integer.parseInt(attr.getFirstChild().getNodeValue()));
          }
        }
      }
      return sp;
    }

}
