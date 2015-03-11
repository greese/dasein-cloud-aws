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

package org.dasein.cloud.aws.platform;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.aws.compute.EC2Method;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.platform.*;
import org.dasein.cloud.util.APITrace;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorPopulator;
import org.dasein.util.PopulatorThread;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

/**
 * CloudWatch specific implementation of AbstractMonitoringSupport.
 *
 * @author Cameron Stokes (http://github.com/clstokes)
 * @since 2013-02-18
 */
public class CloudWatch extends AbstractMonitoringSupport {

    public static final String SERVICE_ID = "monitoring";
    public static final String STATE_OK = "OK";
    public static final String STATE_ALARM = "ALARM";
    public static final String STATE_INSUFFICIENT_DATA = "INSUFFICIENT_DATA";
    static private final Logger logger = Logger.getLogger(CloudWatch.class);
    private AWSCloud provider = null;

    CloudWatch( AWSCloud provider ) {
        super(provider);
        this.provider = provider;
    }

    @Override
    public void updateAlarm( @Nonnull AlarmUpdateOptions options ) throws InternalException, CloudException {
        APITrace.begin(provider, "CloudWatch.addAlarm");
        try {
            ProviderContext ctx = provider.getContext();
            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }

            Map<String, String> parameters = provider.getStandardCloudWatchParameters(ctx, EC2Method.PUT_METRIC_ALARM);

            // all required parameters per CloudWatch API Version 2010-08-01
            parameters.put("AlarmName", options.getAlarmName());
            parameters.put("Namespace", options.getMetricNamespace());
            parameters.put("MetricName", options.getMetric());
            parameters.put("Statistic", options.getStatistic());
            parameters.put("ComparisonOperator", options.getComparisonOperator());
            parameters.put("Threshold", String.valueOf(options.getThreshold()));
            parameters.put("Period", String.valueOf(options.getPeriod()));
            parameters.put("EvaluationPeriods", String.valueOf(options.getEvaluationPeriods()));

            // optional parameters per CloudWatch API Version 2010-08-01
            parameters.put("ActionsEnabled", String.valueOf(options.isEnabled()));
            AWSCloud.addValueIfNotNull(parameters, "AlarmDescription", options.getAlarmDescription());
            AWSCloud.addIndexedParameters(parameters, "OKActions.member.", options.getProviderOKActionIds());
            AWSCloud.addIndexedParameters(parameters, "AlarmActions.member.", options.getProviderAlarmActionIds());
            AWSCloud.addIndexedParameters(parameters, "InsufficientDataActions.member.", options.getProviderInsufficentDataActionIds());
            AWSCloud.addIndexedParameters(parameters, "Dimensions.member.", options.getMetadata());

            EC2Method method;
            method = new EC2Method(SERVICE_ID, provider, parameters);
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

    @Override public void removeAlarms( @Nonnull String[] alarmNames ) throws InternalException, CloudException {
        APITrace.begin(provider, "CloudWatch.removeAlarms");
        try {
            updateAlarmAction(alarmNames, EC2Method.DELETE_ALARMS);
        } finally {
            APITrace.end();
        }
    }

    @Override
    public void enableAlarmActions( @Nonnull String[] alarmNames ) throws InternalException, CloudException {
        APITrace.begin(provider, "CloudWatch.enableAlarmActions");
        try {
            updateAlarmAction(alarmNames, EC2Method.ENABLE_ALARM_ACTIONS);
        } finally {
            APITrace.end();
        }
    }

    @Override
    public void disableAlarmActions( @Nonnull String[] alarmNames ) throws InternalException, CloudException {
        APITrace.begin(provider, "CloudWatch.disableAlarmActions");
        try {
            updateAlarmAction(alarmNames, EC2Method.DISABLE_ALARM_ACTIONS);
        } finally {
            APITrace.end();
        }
    }

    private void updateAlarmAction( @Nonnull String[] alarmNames, @Nonnull String action ) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();
        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }

        Map<String, String> parameters = provider.getStandardCloudWatchParameters(ctx, action);

        provider.addIndexedParameters(parameters, "AlarmNames.member.", alarmNames);

        EC2Method method;
        method = new EC2Method(SERVICE_ID, provider, parameters);
        try {
            method.invoke();
        } catch( EC2Exception e ) {
            logger.error(e.getSummary());
            throw new CloudException(e);
        }
    }

    @Override
    public @Nonnull Collection<Alarm> listAlarms( AlarmFilterOptions options ) throws InternalException, CloudException {
        APITrace.begin(provider, "CloudWatch.listAlarms");
        try {
            ProviderContext ctx = provider.getContext();
            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }

            Map<String, String> parameters = provider.getStandardCloudWatchParameters(ctx, EC2Method.DESCRIBE_ALARMS);

            AWSCloud.addExtraParameters(parameters, getAlarmFilterParameters(options));

            List<Alarm> list = new ArrayList<Alarm>();
            NodeList blocks;
            EC2Method method;
            Document doc;

            method = new EC2Method(SERVICE_ID, provider, parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }

            blocks = doc.getElementsByTagName("MetricAlarms");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                NodeList metrics = blocks.item(i).getChildNodes();

                for( int j = 0; j < metrics.getLength(); j++ ) {
                    Node metricNode = metrics.item(j);

                    if( metricNode.getNodeName().equals("member") ) {
                        Alarm alarm = toAlarm(metricNode);
                        if( alarm != null ) {
                            list.add(alarm);
                        }
                    }
                }
            }
            return list;

        } finally {
            APITrace.end();
        }
    }

    private Alarm toAlarm( Node node ) throws CloudException, InternalException {
        if( node == null ) {
            return null;
        }

        Alarm alarm = new Alarm();
        alarm.setFunction(false); // CloudWatch doesn't use a DSL or function for it's alarms. It uses statistic, comparisonOperator, and threshold.

        NodeList attributes = node.getChildNodes();
        for( int i = 0; i < attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);
            String name = attribute.getNodeName();

            if( name.equals("AlarmName") ) {
                alarm.setName(provider.getTextValue(attribute));
            }
            else if( name.equals("AlarmDescription") ) {
                alarm.setDescription(provider.getTextValue(attribute));
            }
            else if( name.equals("Namespace") ) {
                alarm.setMetricNamespace(provider.getTextValue(attribute));
            }
            else if( name.equals("MetricName") ) {
                alarm.setMetric(provider.getTextValue(attribute));
            }
            else if( name.equals("Dimensions") ) {
                Map<String, String> dimensions = toMetadata(attribute.getChildNodes());
                if( dimensions != null ) {
                    alarm.addMetricMetadata(dimensions);
                }
            }
            else if( name.equals("ActionsEnabled") ) {
                alarm.setEnabled("true".equals(provider.getTextValue(attribute)));
            }
            else if( name.equals("AlarmArn") ) {
                alarm.setProviderAlarmId(provider.getTextValue(attribute));
            }
            else if( name.equals("AlarmActions") ) {
                alarm.setProviderAlarmActionIds(getMembersValues(attribute));
            }
            else if( name.equals("InsufficientDataActions") ) {
                alarm.setProviderInsufficientDataActionIds(getMembersValues(attribute));
            }
            else if( name.equals("OKActions") ) {
                alarm.setProviderOKActionIds(getMembersValues(attribute));
            }
            else if( name.equals("Statistic") ) {
                alarm.setStatistic(provider.getTextValue(attribute));
            }
            else if( name.equals("Period") ) {
                alarm.setPeriod(provider.getIntValue(attribute));
            }
            else if( name.equals("EvaluationPeriods") ) {
                alarm.setEvaluationPeriods(provider.getIntValue(attribute));
            }
            else if( name.equals("ComparisonOperator") ) {
                alarm.setComparisonOperator(provider.getTextValue(attribute));
            }
            else if( name.equals("Threshold") ) {
                alarm.setThreshold(provider.getDoubleValue(attribute));
            }
            else if( name.equals("StateReason") ) {
                alarm.setStateReason(provider.getTextValue(attribute));
            }
            else if( name.equals("StateReasonData") ) {
                alarm.setStateReasonData(provider.getTextValue(attribute));
            }
            else if( name.equals("StateUpdatedTimestamp") ) {
                alarm.setStateUpdatedTimestamp(provider.getTimestampValue(attribute));
            }
            else if( name.equals("StateValue") ) {
                String stateValue = provider.getTextValue(attribute);
                if( STATE_OK.equals(stateValue) ) {
                    alarm.setStateValue(AlarmState.OK);
                }
                else if( STATE_ALARM.equals(stateValue) ) {
                    alarm.setStateValue(AlarmState.ALARM);
                }
                else if( STATE_INSUFFICIENT_DATA.equals(stateValue) ) {
                    alarm.setStateValue(AlarmState.INSUFFICIENT_DATA);
                }
            }
        }
        return alarm;
    }

    /**
     * Gets single text values from a "member" set.
     *
     * @param attribute the node to start at
     * @return array of member string values
     */
    private String[] getMembersValues( Node attribute ) {
        List<String> actions = new ArrayList<String>();
        NodeList actionNodes = attribute.getChildNodes();
        for( int j = 0; j < actionNodes.getLength(); j++ ) {
            Node actionNode = actionNodes.item(j);
            if( actionNode.getNodeName().equals("member") ) {
                actions.add(provider.getTextValue(actionNode));
            }
        }
        if( actions.size() == 0 ) {
            return null;
        }
        return actions.toArray(new String[actions.size()]);
    }

    @Override
    public @Nonnull Collection<Metric> listMetrics( final MetricFilterOptions options ) throws InternalException, CloudException {
        PopulatorThread<Metric> populator;

        provider.hold();
        populator = new PopulatorThread<Metric>(new JiteratorPopulator<Metric>() {
            public void populate( @Nonnull Jiterator<Metric> iterator ) throws CloudException, InternalException {
                try {
                    populateMetrics(iterator, null, options);
                } finally {
                    provider.release();
                }
            }
        });
        populator.populate();
        return populator.getResult();
    }

    private void populateMetrics( @Nonnull Jiterator<Metric> iterator, @Nullable String nextToken, MetricFilterOptions options ) throws CloudException, InternalException {
        APITrace.begin(provider, "CloudWatch.listMetrics");
        try {
            ProviderContext ctx = provider.getContext();
            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }

            Map<String, String> parameters = provider.getStandardCloudWatchParameters(ctx, EC2Method.LIST_METRICS);

            AWSCloud.addExtraParameters(parameters, getMetricFilterParameters(options));
            AWSCloud.addValueIfNotNull(parameters, "NextToken", nextToken);

            NodeList blocks;
            EC2Method method;
            Document doc;

            method = new EC2Method(SERVICE_ID, provider, parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }

            blocks = doc.getElementsByTagName("Metrics");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                NodeList metrics = blocks.item(i).getChildNodes();

                for( int j = 0; j < metrics.getLength(); j++ ) {
                    Node metricNode = metrics.item(j);

                    if( metricNode.getNodeName().equals("member") ) {
                        Metric metric = toMetric(metricNode);
                        if( metric != null ) {
                            iterator.push(metric);
                        }
                    }
                }
            }

            blocks = doc.getElementsByTagName("NextToken");
            if( blocks != null && blocks.getLength() == 1 && blocks.item(0).hasChildNodes() ) {
                String newNextToken = provider.getTextValue(blocks.item(0));
                populateMetrics(iterator, newNextToken, options);
            }

        } finally {
            APITrace.end();
        }
    }

    /**
     * Converts the given node to a Metric object.
     *
     * @param node the node to convert
     * @return a Metric object
     */
    private Metric toMetric( Node node ) {
        if( node == null ) {
            return null;
        }

        Metric metric = new Metric();

        NodeList attributes = node.getChildNodes();
        for( int i = 0; i < attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);
            String name = attribute.getNodeName();

            if( name.equals("MetricName") ) {
                metric.setName(provider.getTextValue(attribute));
            }
            else if( name.equals("Namespace") ) {
                metric.setNamespace(provider.getTextValue(attribute));
            }
            else if( name.equals("Dimensions") ) {
                Map<String, String> dimensions = toMetadata(attribute.getChildNodes());
                if( dimensions != null ) {
                    metric.addMetadata(dimensions);
                }
            }
        }
        return metric;
    }

    /**
     * Converts the given NodeList to a metadata map.
     *
     * @param blocks the NodeList to convert
     * @return metadata map
     */
    private Map<String, String> toMetadata( NodeList blocks ) {
        Map<String, String> dimensions = new HashMap<String, String>();

        for( int i = 0; i < blocks.getLength(); i++ ) {
            Node dimensionNode = blocks.item(i);

            if( dimensionNode.getNodeName().equals("member") ) {
                String dimensionName = null;
                String dimensionValue = null;

                NodeList dimensionAttributes = dimensionNode.getChildNodes();
                for( int j = 0; j < dimensionAttributes.getLength(); j++ ) {
                    Node attribute = dimensionAttributes.item(j);
                    String name = attribute.getNodeName();

                    if( name.equals("Name") ) {
                        dimensionName = provider.getTextValue(attribute);
                    }
                    else if( name.equals("Value") ) {
                        dimensionValue = provider.getTextValue(attribute);
                    }
                }
                if( dimensionName != null ) {
                    dimensions.put(dimensionName, dimensionValue);
                }
            }
        }

        if( dimensions.size() == 0 ) {
            return null;
        }
        return dimensions;
    }

    /**
     * Creates map of parameters based on the MetricFilterOptions.
     *
     * @param options options to convert to a parameter map
     * @return parameter map
     */
    private Map<String, String> getMetricFilterParameters( MetricFilterOptions options ) {
        if( options == null ) {
            return null;
        }

        Map<String, String> parameters = new HashMap<String, String>();

        AWSCloud.addValueIfNotNull(parameters, "MetricName", options.getMetricName());
        AWSCloud.addValueIfNotNull(parameters, "Namespace", options.getMetricNamespace());
        AWSCloud.addIndexedParameters(parameters, "Dimensions.member.", options.getMetricMetadata());

        if( parameters.size() == 0 ) {
            return null;
        }

        return parameters;
    }

    /**
     * Creates map of parameters based on the AlarmFilterOptions.
     *
     * @param options options to convert to a parameter map
     * @return parameter map
     */
    private Map<String, String> getAlarmFilterParameters( AlarmFilterOptions options ) {
        if( options == null ) {
            return null;
        }

        Map<String, String> parameters = new HashMap<String, String>();

        if( options.getStateValue() != null ) {
            AWSCloud.addValueIfNotNull(parameters, "StateValue", options.getStateValue().name());
        }
        AWSCloud.addIndexedParameters(parameters, "AlarmNames.member.", options.getAlarmNames());

        if( parameters.size() == 0 ) {
            return null;
        }

        return parameters;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return true;
    }

    /**
     * Returns the cloudwatch endpoint url.
     *
     * @return the cloudwatch endpoint url
     */
    private String getCloudWatchUrl() {
        return ( "https://monitoring." + provider.getContext().getRegionId() + ".amazonaws.com" );
    }

    @Override
    public @Nonnull String[] mapServiceAction( @Nonnull ServiceAction action ) {
        if( action.equals(MonitoringSupport.ANY) ) {
            return new String[]{EC2Method.CW_PREFIX + "*"};
        }
        if( action.equals(MonitoringSupport.LIST_METRICS) ) {
            return new String[]{EC2Method.CW_PREFIX + EC2Method.LIST_METRICS};
        }
        if( action.equals(MonitoringSupport.DESCRIBE_ALARMS) ) {
            return new String[]{EC2Method.CW_PREFIX + EC2Method.DESCRIBE_ALARMS};
        }
        return new String[0];
    }

}
