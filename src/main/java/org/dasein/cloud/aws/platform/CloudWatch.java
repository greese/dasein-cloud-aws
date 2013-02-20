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
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import java.util.*;

/**
 * CloudWatch specific implementation of AbstractMonitoringSupport.
 *
 * @author Cameron Stokes (http://github.com/clstokes)
 * @since 2013-02-18
 */
public class CloudWatch extends AbstractMonitoringSupport {

  static private final Logger logger = Logger.getLogger( CloudWatch.class );

  private AWSCloud provider = null;

  CloudWatch( AWSCloud provider ) {
    super( provider );
    this.provider = provider;
  }

  @Override
  public @Nonnull Collection<Alarm> listAlarms( AlarmFilterOptions options ) throws InternalException, CloudException {
    APITrace.begin( provider, "CloudWatch.listAlarms" );
    try {
      ProviderContext ctx = provider.getContext();
      if ( ctx == null ) {
        throw new CloudException( "No context was set for this request" );
      }

      Map<String, String> parameters = provider.getStandardCloudWatchParameters( provider.getContext(), EC2Method.DESCRIBE_ALARMS );

      provider.putExtraParameters( parameters, getAlarmFilterParameters( options ) );

      List<Alarm> list = new ArrayList<Alarm>();
      NodeList blocks;
      EC2Method method;
      Document doc;

      method = new EC2Method( provider, getCloudWatchUrl(), parameters );
      try {
        doc = method.invoke();
      }
      catch ( EC2Exception e ) {
        logger.error( e.getSummary() );
        throw new CloudException( e );
      }

      blocks = doc.getElementsByTagName( "MetricAlarms" );
      for ( int i = 0; i < blocks.getLength(); i++ ) {
        NodeList metrics = blocks.item( i ).getChildNodes();

        for ( int j = 0; j < metrics.getLength(); j++ ) {
          Node metricNode = metrics.item( j );

          if ( metricNode.getNodeName().equals( "member" ) ) {
            Alarm alarm = toAlarm( metricNode );
            if ( alarm != null ) {
              list.add( alarm );
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

  private Alarm toAlarm( Node node ) throws CloudException, InternalException {
    if ( node == null ) {
      return null;
    }

    Alarm alarm = new Alarm();

    NodeList attributes = node.getChildNodes();
    for ( int i = 0; i < attributes.getLength(); i++ ) {
      Node attribute = attributes.item( i );
      String name = attribute.getNodeName();

      if ( name.equals( "AlarmName" ) ) {
        alarm.setName( getTextValue( attribute ) );
      }
      else if ( name.equals( "AlarmDescription" ) ) {
        alarm.setDescription( getTextValue( attribute ) );
      }
      else if ( name.equals( "Namespace" ) ) {
        alarm.setMetricNamespace( getTextValue( attribute ) );
      }
      else if ( name.equals( "MetricName" ) ) {
        alarm.setMetricName( getTextValue( attribute ) );
      }
      else if ( name.equals( "Dimensions" ) ) {
        Map<String, String> dimensions = toDimensions( attribute.getChildNodes() );
        if ( dimensions != null ) {
          alarm.setDimensions( dimensions );
        }
      }
      else if ( name.equals( "ActionsEnabled" ) ) {
        alarm.setActionsEnabled( "true".equals( getTextValue( attribute ) ) );
      }
      else if ( name.equals( "AlarmArn" ) ) {
        alarm.setProviderAlarmId( getTextValue( attribute ) );
      }
      else if ( name.equals( "AlarmActions" ) ) {
        alarm.setProviderAlarmActionIds( getMembersValues( attribute ) );
      }
      else if ( name.equals( "InsufficientDataActions" ) ) {
        alarm.setProviderInsufficentDataActionIds( getMembersValues( attribute ) );
      }
      else if ( name.equals( "OKActions" ) ) {
        alarm.setProviderOKActionIds( getMembersValues( attribute ) );
      }
      else if ( name.equals( "Statistic" ) ) {
        alarm.setStatistic( getTextValue( attribute ) );
      }
      else if ( name.equals( "Period" ) ) {
        alarm.setPeriod( getTextValue( attribute ) );
      }
      else if ( name.equals( "EvaluationPeriods" ) ) {
        alarm.setEvaluationPeriods( getTextValue( attribute ) );
      }
      else if ( name.equals( "ComparisonOperator" ) ) {
        alarm.setComparisonOperator( getTextValue( attribute ) );
      }
      else if ( name.equals( "Threshold" ) ) {
        alarm.setThreshold( getTextValue( attribute ) );
      }
      else if ( name.equals( "Unit" ) ) {
        alarm.setUnit( getTextValue( attribute ) );
      }
      else if ( name.equals( "StateReason" ) ) {
        alarm.setStateReason( getTextValue( attribute ) );
      }
      else if ( name.equals( "StateReasonData" ) ) {
        alarm.setStateReasonData( getTextValue( attribute ) );
      }
      else if ( name.equals( "StateUpdatedTimestamp" ) ) {
        alarm.setStateUpdatedTimestamp( getTextValue( attribute ) );
      }
      else if ( name.equals( "StateValue" ) ) {
        alarm.setStateValue( getTextValue( attribute ) );
      }
    }
    return alarm;
  }

  private String[] getMembersValues( Node attribute ) {
    List<String> actions = new ArrayList<String>();
    NodeList actionNodes = attribute.getChildNodes();
    for ( int j = 0; j < actionNodes.getLength(); j++ ) {
      Node actionNode = actionNodes.item( j );
      if ( actionNode.getNodeName().equals( "member" ) ) {
        actions.add( getTextValue( actionNode ) );
      }
    }
    if ( actions.size() == 0 ) {
      return null;
    }
    return actions.toArray( new String[] {} );
  }

  @Override
  public @Nonnull Collection<Metric> listMetrics( MetricFilterOptions options ) throws InternalException, CloudException {
    APITrace.begin( provider, "CloudWatch.listMetrics" );
    try {
      ProviderContext ctx = provider.getContext();
      if ( ctx == null ) {
        throw new CloudException( "No context was set for this request" );
      }

      Map<String, String> parameters = provider.getStandardCloudWatchParameters( provider.getContext(), EC2Method.LIST_METRICS );

      provider.putExtraParameters( parameters, getMetricFilterParameters( options ) );

      List<Metric> list = new ArrayList<Metric>();
      NodeList blocks;
      EC2Method method;
      Document doc;

      method = new EC2Method( provider, getCloudWatchUrl(), parameters );
      try {
        doc = method.invoke();
      }
      catch ( EC2Exception e ) {
        logger.error( e.getSummary() );
        throw new CloudException( e );
      }

      blocks = doc.getElementsByTagName( "Metrics" );
      for ( int i = 0; i < blocks.getLength(); i++ ) {
        NodeList metrics = blocks.item( i ).getChildNodes();

        for ( int j = 0; j < metrics.getLength(); j++ ) {
          Node metricNode = metrics.item( j );

          if ( metricNode.getNodeName().equals( "member" ) ) {
            Metric metric = toMetric( metricNode );
            if ( metric != null ) {
              list.add( metric );
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

  private Metric toMetric( Node node ) throws CloudException, InternalException {
    if ( node == null ) {
      return null;
    }

    Metric metric = new Metric();

    NodeList attributes = node.getChildNodes();
    for ( int i = 0; i < attributes.getLength(); i++ ) {
      Node attribute = attributes.item( i );
      String name = attribute.getNodeName();

      if ( name.equals( "MetricName" ) ) {
        metric.setName( getTextValue( attribute ) );
      }
      else if ( name.equals( "Namespace" ) ) {
        metric.setNamespace( getTextValue( attribute ) );
      }
      else if ( name.equals( "Dimensions" ) ) {
        Map<String, String> dimensions = toDimensions( attribute.getChildNodes() );
        if ( dimensions != null ) {
          metric.setDimensions( dimensions );
        }
      }
    }
    return metric;
  }

  private Map<String, String> toDimensions( NodeList blocks ) {
    Map<String, String> dimensions = new HashMap<String, String>();

    for ( int i = 0; i < blocks.getLength(); i++ ) {
      Node dimensionNode = blocks.item( i );

      if ( dimensionNode.getNodeName().equals( "member" ) ) {
        String dimensionName = null;
        String dimensionValue = null;

        NodeList dimensionAttributes = dimensionNode.getChildNodes();
        for ( int j = 0; j < dimensionAttributes.getLength(); j++ ) {
          Node attribute = dimensionAttributes.item( j );
          String name = attribute.getNodeName();

          if ( name.equals( "Name" ) ) {
            dimensionName = getTextValue( attribute );
          }
          else if ( name.equals( "Value" ) ) {
            dimensionValue = getTextValue( attribute );
          }
        }
        if ( dimensionName != null ) {
          dimensions.put( dimensionName, dimensionValue );
        }
      }
    }

    if ( dimensions.size() == 0 ) {
      return null;
    }
    return dimensions;
  }

  private Map<String, String> getMetricFilterParameters( MetricFilterOptions options ) {
    if ( options == null ) {
      return null;
    }

    Map<String, String> parameters = new HashMap<String, String>();

    if ( options.getMetricName() != null ) {
      parameters.put( "MetricName", options.getMetricName() );
    }

    if ( options.getMetricNamespace() != null ) {
      parameters.put( "Namespace", options.getMetricNamespace() );
    }

    Map<String, String> dimensions = options.getMetricDimensions();
    if ( dimensions != null && dimensions.size() > 0 ) {
      int i = 1;
      for ( Map.Entry<String, String> entry : dimensions.entrySet() ) {
        parameters.put( "Dimensions.member." + i + ".Name", entry.getKey() );
        if ( entry.getValue() != null ) {
          parameters.put( "Dimensions.member." + i + ".Value", entry.getValue() );
        }
        i++;
      }
    }

    if ( parameters.size() == 0 ) {
      return null;
    }

    return parameters;
  }

  private Map<String, String> getAlarmFilterParameters( AlarmFilterOptions options ) {
    if ( options == null ) {
      return null;
    }

    Map<String, String> parameters = new HashMap<String, String>();

    if ( options.getStateValue() != null ) {
      parameters.put( "StateValue", options.getStateValue() );
    }

    if ( options.getAlarmNames() != null ) {
      String[] alarmNames = options.getAlarmNames();
      for ( int i = 0; i < options.getAlarmNames().length; i++ ) {
        String alarmName = alarmNames[i];
        parameters.put( "AlarmNames.member." + (i + 1), alarmName );
      }
    }

    if ( parameters.size() == 0 ) {
      return null;
    }

    return parameters;
  }

  private String getTextValue( Node node ) {
    if ( node.getChildNodes().getLength() == 0 ) {
      return null;
    }
    return node.getFirstChild().getNodeValue();
  }

  private String getCloudWatchUrl() throws InternalException, CloudException {
    return ("https://monitoring." + provider.getContext().getRegionId() + ".amazonaws.com");
  }

  @Override
  public @Nonnull String[] mapServiceAction( @Nonnull ServiceAction action ) {
    if ( action.equals( MonitoringSupport.ANY ) ) {
      return new String[] {EC2Method.CW_PREFIX + "*"};
    }
    if ( action.equals( MonitoringSupport.LIST_METRICS ) ) {
      return new String[] {EC2Method.CW_PREFIX + EC2Method.LIST_METRICS};
    }
    if ( action.equals( MonitoringSupport.LIST_ALARMS ) ) {
      return new String[] {EC2Method.CW_PREFIX + EC2Method.DESCRIBE_ALARMS};
    }
    return new String[0];
  }

}
