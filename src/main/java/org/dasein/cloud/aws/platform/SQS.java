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

import java.util.Locale;
import java.util.Map;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.aws.compute.EC2Method;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.platform.AbstractMQSupport;
import org.dasein.cloud.platform.MQCreateOptions;
import org.dasein.cloud.platform.MQMessageIdentifier;
import org.dasein.cloud.platform.MQMessageReceipt;
import org.dasein.cloud.platform.MQSupport;
import org.dasein.cloud.platform.MessageQueue;
import org.dasein.util.uom.time.Second;
import org.dasein.util.uom.time.TimePeriod;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SQS extends AbstractMQSupport<AWSCloud> {
    static public final String SET_QUEUE_ATTRIBUTES = "SetQueueAttributes";

    static public final String SERVICE_ID = "sqs";

    public static ServiceAction[] asSQSServiceAction(String action) {
        return new ServiceAction[0];
    }

    public SQS(AWSCloud provider) { super(provider); }

    @Override
    public @Nonnull String createMessageQueue(@Nonnull MQCreateOptions options) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull String getProviderTermForMessageQueue(@Nonnull Locale locale) {
        return "message queue";
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull Iterable<MessageQueue> listMessageQueues() throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listMessageQueueStatus() throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull Iterable<MQMessageReceipt> receiveMessages(@Nonnull String mqId, @Nullable TimePeriod<Second> waitTime, @Nonnegative int count, @Nullable TimePeriod<Second> visibilityTimeout) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void removeMessageQueue(@Nonnull String mqId, @Nullable String reason) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull MQMessageIdentifier sendMessage(@Nonnull String mqId, @Nonnull String message) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private void setQueueAttribute(@Nonnull String endpoint, @Nonnull String name, @Nonnull String value) throws InternalException, CloudException {
        Map<String,String> parameters = getProvider().getStandardSqsParameters(getContext(), SET_QUEUE_ATTRIBUTES);
        EC2Method method;

        parameters.put("Attribute.Name", name);
        parameters.put("Attribute.Value", value);
        method = new EC2Method(SERVICE_ID, getProvider(), parameters);
        try {
            method.invoke();
        }
        catch( EC2Exception e ) {
            throw new CloudException(e);
        }
    }
}
