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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.DataFormat;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.aws.compute.EC2Method;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.platform.*;
import org.dasein.cloud.util.APITrace;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * AWS SNS Support
 *
 * @author George Reese
 * @author Stas Maksimov
 * @version 2014.08 deprecated methods moved to capabilities
 * @since ?
 */
public class SNS implements PushNotificationSupport {
    static private final Logger logger = AWSCloud.getLogger(SNS.class);

    static public final String SERVICE_ID                  = "sns";
    
    static public final String CONFIRM_SUBSCRIPTION        = "ConfirmSubscription";
    static public final String CREATE_TOPIC                = "CreateTopic";
    static public final String DELETE_TOPIC                = "DeleteTopic";
    static public final String GET_TOPIC_ATTRIBUTES        = "GetTopicAttributes";
    static public final String LIST_SUBSCRIPTIONS          = "ListSubscriptions";
    static public final String LIST_SUBSCRIPTIONS_BY_TOPIC = "ListSubscriptionsByTopic";
    static public final String LIST_TOPICS                 = "ListTopics";
    static public final String PUBLISH                     = "Publish";
    static public final String SET_TOPIC_ATTRIBUTES        = "SetTopicAttributes";
    static public final String SUBSCRIBE                   = "Subscribe";
    static public final String UNSUBSCRIBE                 = "Unsubscribe";

    private volatile transient SNSCapabilities capabilities;

    static public @Nonnull ServiceAction[] asSNSServiceAction(@Nonnull String action) {
        if( action.equals(CREATE_TOPIC) ) {
            return new ServiceAction[] { PushNotificationSupport.CREATE_TOPIC };
        }
        else if( action.equals(DELETE_TOPIC) ) {
            return new ServiceAction[] { PushNotificationSupport.REMOVE_TOPIC };
        }
        else if( action.equals(LIST_TOPICS) ) {
            return new ServiceAction[] { PushNotificationSupport.GET_TOPIC, PushNotificationSupport.LIST_TOPIC };
        }
        else if( action.equals(PUBLISH) ) {
            return new ServiceAction[] { PushNotificationSupport.PUBLISH };
        }
        else if( action.equals(SUBSCRIBE) ) {
            return new ServiceAction[] { PushNotificationSupport.SUBSCRIBE };
        }
        return new ServiceAction[0];
    }
    
    private AWSCloud provider = null;
    
    SNS(AWSCloud provider) { this.provider = provider; }
    
    @Override
    public String confirmSubscription(String providerTopicId, String token, boolean authenticateUnsubscribe) throws CloudException, InternalException {
        APITrace.begin(provider, "Notifications.confirmSubscription");
        try {
            Map<String,String> parameters = provider.getStandardSnsParameters(provider.getContext(), CONFIRM_SUBSCRIPTION);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("TopicArn", providerTopicId);
            parameters.put("Token", token);
            if( authenticateUnsubscribe ) {
                parameters.put("AuthenticateOnUnsubscribe", "true");
            }
            method = new EC2Method(SERVICE_ID, provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("ConfirmSubscriptionResult");
            for( int i=0; i<blocks.getLength(); i++ ) {
                Node item = blocks.item(i);

                for( int j=0; j<item.getChildNodes().getLength(); j++ ) {
                    Node attr = item.getChildNodes().item(j);
                    String name;

                    name = attr.getNodeName();
                    if( name.equals("SubscriptionArn") ) {
                        return attr.getFirstChild().getNodeValue().trim();
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
    public Topic createTopic(String name) throws CloudException, InternalException {
        APITrace.begin(provider, "Notifications.createTopic");
        try {
            Map<String,String> parameters = provider.getStandardSnsParameters(provider.getContext(), CREATE_TOPIC);
            Topic topic = null;
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("Name", name);
            method = new EC2Method(SERVICE_ID, provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("CreateTopicResult");
            for( int i=0; i<blocks.getLength(); i++ ) {
                Node item = blocks.item(i);

                topic = toTopic(item);
                if( topic != null ) {
                    try {
                        setTopicAttribute(topic, "DisplayName", name);
                    }
                    catch( Throwable t ) {
                        logger.warn("Unable to set DisplayName for " + name + " [#" + topic.getProviderTopicId() + "]: " + t.getMessage());
                    }
                    topic.setName(name);
                    return topic;
                }
            }
            return topic;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    @Deprecated
    public String getProviderTermForSubscription(Locale locale) {
        try {
            return getCapabilities().getProviderTermForSubscription(locale);
        } catch( InternalException e ) {
        } catch( CloudException e ) {
        }
        return "subscription"; // legacy
    }
    
    @Override
    @Deprecated
    public String getProviderTermForTopic(Locale locale) {
        try {
            return getCapabilities().getProviderTermForTopic(locale);
        } catch( InternalException e ) {
        } catch( CloudException e ) {
        }
        return "topic"; // legacy
    }

    @Override
    public @Nonnull PushNotificationCapabilities getCapabilities() throws InternalException, CloudException {
        if( capabilities == null ) {
            capabilities = new SNSCapabilities(provider);
        }
        return capabilities;
    }

    public String getSNSUrl() throws InternalException, CloudException {
        return ("https://sns." + provider.getContext().getRegionId() + ".amazonaws.com");
    }
    
    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(provider, "Notifications.isSubscribed");
        try {
            Map<String,String> parameters = provider.getStandardSnsParameters(provider.getContext(), LIST_TOPICS);
            EC2Method method;

            method = new EC2Method(SERVICE_ID, provider, parameters);
            try {
                method.invoke();
                return true;
            }
            catch( EC2Exception e ) {
                if( e.getStatus() == HttpStatus.SC_UNAUTHORIZED || e.getStatus() == HttpStatus.SC_FORBIDDEN ) {
                    return false;
                }
                String code = e.getCode();

                if( code != null && (code.equals("SubscriptionCheckFailed") || code.equals("AuthFailure") || code.equals("SignatureDoesNotMatch") || code.equals("InvalidClientTokenId") || code.equals("OptInRequired")) ) {
                    return false;
                }
                logger.warn(e.getSummary());
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
    public Collection<Subscription> listSubscriptions(String optionalTopicId) throws CloudException, InternalException {
        APITrace.begin(provider, "Notifications.listSubscriptions");
        try {
            Map<String,String> parameters = provider.getStandardSnsParameters(provider.getContext(), optionalTopicId == null ? LIST_SUBSCRIPTIONS : LIST_SUBSCRIPTIONS_BY_TOPIC);
            ArrayList<Subscription> list = new ArrayList<Subscription>();
            EC2Method method;
            NodeList blocks;
            Document doc;

            if( optionalTopicId != null ) {
                parameters.put("TopicArn", optionalTopicId);
            }
            method = new EC2Method(SERVICE_ID, provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("Subscriptions");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("member") ) {
                        Subscription subscription = toSubscription(item);

                        if( subscription != null ) {
                            list.add(subscription);
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
    public @Nonnull Iterable<ResourceStatus> listTopicStatus() throws CloudException, InternalException {
        APITrace.begin(provider, "Notifications.listTopicStatus");
        try {
            Map<String,String> parameters = provider.getStandardSnsParameters(provider.getContext(), LIST_TOPICS);
            ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();
            EC2Method method;
            NodeList blocks;
            Document doc;

            method = new EC2Method(SERVICE_ID, provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("Topics");
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
    public Collection<Topic> listTopics() throws CloudException, InternalException {
        APITrace.begin(provider, "Notifications.listTopics");
        try {
            Map<String,String> parameters = provider.getStandardSnsParameters(provider.getContext(), LIST_TOPICS);
            ArrayList<Topic> list = new ArrayList<Topic>();
            EC2Method method;
            NodeList blocks;
            Document doc;

            method = new EC2Method(SERVICE_ID, provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("Topics");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("member") ) {
                        Topic topic = toTopic(item);

                        if( topic != null ) {
                            list.add(topic);
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
        return new String[0];     // TODO: implement me
    }

    @Override
    public String publish(String providerTopicId, String subject, String message) throws CloudException, InternalException {
        APITrace.begin(provider, "Notifications.publish");
        try {
            Map<String,String> parameters = provider.getStandardSnsParameters(provider.getContext(), PUBLISH);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("TopicArn", providerTopicId);
            parameters.put("Subject", subject);
            parameters.put("Message", message);
            method = new EC2Method(SERVICE_ID, provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("PublishResult");
            for( int i=0; i<blocks.getLength(); i++ ) {
                Node item = blocks.item(i);

                for( int j=0; j<item.getChildNodes().getLength(); j++ ) {
                    Node attr = item.getChildNodes().item(j);
                    String name;

                    name = attr.getNodeName();
                    if( name.equals("MessageId") ) {
                        return attr.getFirstChild().getNodeValue().trim();
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
    public void removeTopic(String providerTopicId) throws CloudException, InternalException {
        APITrace.begin(provider, "Notifications.removeTopic");
        try {
            Map<String,String> parameters = provider.getStandardSnsParameters(provider.getContext(), DELETE_TOPIC);
            EC2Method method;

            parameters.put("TopicArn", providerTopicId);
            method = new EC2Method(SERVICE_ID, provider, parameters);
            try {
                method.invoke();
            }
            catch( EC2Exception e ) {
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

  @Override
  public @Nullable Topic getTopic( @Nonnull String providerTopicId ) throws CloudException, InternalException {
    try {
      Topic topic = new Topic();
      topic.setProviderTopicId( providerTopicId );
      setTopicAttributes( topic );
      return topic;
    }
    catch ( CloudException e ) {
      Throwable cause = e.getCause();
      if ( cause instanceof EC2Exception ) {
        String code = ((EC2Exception) cause).getCode();
        if ( "NotFound".equals( code ) || "InvalidParameter".equals(code) ) {
          return null;
        }

      }
      throw e;
    }
  }

    private void setTopicAttribute(Topic topic, final String attributeName, final String attributeValue) throws InternalException, CloudException {
        APITrace.begin(provider, "Notifications.setTopicAttributes");
        try {
            Map<String,String> parameters = provider.getStandardSnsParameters(provider.getContext(), SET_TOPIC_ATTRIBUTES);
            EC2Method method;

            parameters.put("TopicArn", topic.getProviderTopicId());
            parameters.put("AttributeName", attributeName);
            parameters.put("AttributeValue", attributeValue);

            method = new EC2Method(SERVICE_ID, provider, parameters);
            try {
                method.invoke();
            }
            catch( EC2Exception e ) {
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    private void setTopicAttributes(Topic topic) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        APITrace.begin(provider, "Notifications.getTopicAttributes");
        try {
            topic.setProviderRegionId(ctx.getRegionId());
            topic.setProviderOwnerId(ctx.getAccountNumber());
            topic.setActive(true);

            Map<String,String> parameters = provider.getStandardSnsParameters(provider.getContext(), GET_TOPIC_ATTRIBUTES);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("TopicArn", topic.getProviderTopicId());
            method = new EC2Method(SERVICE_ID, provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("Attributes");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("entry") ) {
                        NodeList parts = item.getChildNodes();
                        String name = null;
                        String value = null;

                        if( parts != null ) {
                            for( int k=0; k<parts.getLength(); k++ ) {
                                Node node = parts.item(k);

                                if( node != null ) {
                                    if( node.getNodeName().equals("key") ) {
                                        name = node.getFirstChild().getNodeValue().trim();
                                    }
                                    else if( node.getNodeName().equals("value") ) {
                                        if( node.getFirstChild() != null ) {
                                            value = node.getFirstChild().getNodeValue().trim();
                                        } else {
                                            value = node.getNodeValue();
                                        }
                                    }
                                }
                            }
                        }
                        if( name != null ) {
                            if( name.equals("DisplayName") ) {
                                if( value != null) {
                                    topic.setName(value);
                                    topic.setDescription(value + " (" + topic.getProviderTopicId() + ")");
                                }
                            }
                            else if( name.equals("Owner") && value != null ) {
                                topic.setProviderOwnerId(value);
                            }
                        }
                    }
                }
            }
            if( topic.getName() == null ) {
                String id = topic.getProviderTopicId();
                int idx = id.lastIndexOf(":");

                if( idx > 0 && idx < id.length()-1 ) {
                    id = id.substring(idx+1);
                }
                topic.setName(id);
            }
            if( topic.getDescription() == null ) {
                topic.setDescription(topic.getName() + " (" + topic.getProviderTopicId() + ")");
            }
        }
        finally {
            APITrace.end();
        }
    }
    
    @Override
    public void subscribe(String providerTopicId, EndpointType endpointType, DataFormat dataFormat, String endpoint) throws CloudException, InternalException {
        APITrace.begin(provider, "Notifications.subscribe");
        try {
            Map<String,String> parameters = provider.getStandardSnsParameters(provider.getContext(), SUBSCRIBE);
            EC2Method method;

            parameters.put("TopicArn", providerTopicId);
            switch( endpointType ) {
                case HTTP:
                    parameters.put("Protocol", "http");
                    break;
                case HTTPS:
                    parameters.put("Protocol", "https");
                    break;
                case EMAIL:
                    if( dataFormat.equals(DataFormat.JSON) ) {
                        parameters.put("Protocol", "email-json");
                    }
                    else {
                        parameters.put("Protocol", "email");
                    }
                    break;
                case AWS_SQS:
                    parameters.put("Protocol", "sqs");
                    break;
                case SMS:
                    parameters.put("Protocol", "sms");
                    break;
            }
            parameters.put("Endpoint", endpoint);
            method = new EC2Method(SERVICE_ID, provider, parameters);
            try {
                method.invoke();
            }
            catch( EC2Exception e ) {
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    private @Nullable ResourceStatus toStatus(@Nullable Node fromAws) throws InternalException, CloudException {
        if( fromAws == null ) {
            return null;
        }
        NodeList attrs = fromAws.getChildNodes();
        String topicId = null;

        for( int i=0; i<attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;

            name = attr.getNodeName();
            if( name.equals("TopicArn") ) {
                topicId = attr.getFirstChild().getNodeValue().trim();
            }
        }
        return new ResourceStatus(topicId, true);
    }

    private Subscription toSubscription(Node fromAws) throws InternalException, CloudException {
        Subscription subscription = new Subscription();
        NodeList attrs = fromAws.getChildNodes();
        
        subscription.setProviderRegionId(provider.getContext().getRegionId());
        for( int i=0; i<attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;
            
            name = attr.getNodeName();
            if( name.equals("TopicArn") ) {
                String id = attr.getFirstChild().getNodeValue().trim();
                
                subscription.setProviderTopicId(id);
            }
            else if( name.equals("SubscriptionArn") ) {
                String id = attr.getFirstChild().getNodeValue().trim();
                
                subscription.setProviderSubscriptionId(id);
                subscription.setDescription(id);
                subscription.setName(id);
            }
            else if( name.equals("Owner") ) {
                String id = attr.getFirstChild().getNodeValue().trim();
                
                subscription.setProviderOwnerId(id);
            }
            else if( name.equals("Endpoint") ) {
                String endpoint = attr.getFirstChild().getNodeValue().trim();
                
                subscription.setEndpoint(endpoint);
            }            
            else if( name.equals("Protocol") ) {
                String proto = attr.getFirstChild().getNodeValue().trim();
                
                if( proto != null ) {
                    if( proto.equals("email") ) {
                        subscription.setEndpointType(EndpointType.EMAIL);
                        subscription.setDataFormat(DataFormat.PLAINTEXT);
                    }
                    else if( proto.equals("email-json") ) {
                        subscription.setEndpointType(EndpointType.EMAIL);
                        subscription.setDataFormat(DataFormat.JSON);
                    }
                    else if( proto.equals("http") ) {
                        subscription.setEndpointType(EndpointType.HTTP);
                        subscription.setDataFormat(DataFormat.JSON);
                    }
                    else if( proto.equals("https") ) {
                        subscription.setEndpointType(EndpointType.HTTPS);
                        subscription.setDataFormat(DataFormat.JSON);
                    } 
                    else if( proto.equals("sqs") ) {
                        subscription.setEndpointType(EndpointType.AWS_SQS);
                        subscription.setDataFormat(DataFormat.JSON);
                    }
                    else if( proto.equals("sms") ) {
                        subscription.setEndpointType(EndpointType.SMS);
                        subscription.setDataFormat(DataFormat.PLAINTEXT);
                    }
                }
            }
        }
        if( subscription.getProviderSubscriptionId() == null ) {
            return null;
        }
        return subscription;
    }
    
    private Topic toTopic(Node fromAws) throws InternalException, CloudException {
        NodeList attrs = fromAws.getChildNodes();
        Topic topic = new Topic();
        
        topic.setProviderOwnerId(provider.getContext().getAccountNumber());
        topic.setProviderRegionId(provider.getContext().getRegionId());
        topic.setActive(true);
        for( int i=0; i<attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;
            
            name = attr.getNodeName();
            if( name.equals("TopicArn") ) {
                String id = attr.getFirstChild().getNodeValue().trim();
                
                topic.setProviderTopicId(id);
            }
        }
        if( topic.getProviderTopicId() == null ) {
            return null;
        }
        setTopicAttributes(topic);
        return topic;
    }
    
    @Override
    public void unsubscribe(String providerSubscriptionId) throws CloudException, InternalException {
        APITrace.begin(provider, "Notifications.unsubscribe");
        try {
            Map<String,String> parameters = provider.getStandardSnsParameters(provider.getContext(), UNSUBSCRIBE);
            EC2Method method;

            parameters.put("SubscriptionArn", providerSubscriptionId);
            method = new EC2Method(SERVICE_ID, provider, parameters);
            try {
                method.invoke();
            }
            catch( EC2Exception e ) {
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }
}
