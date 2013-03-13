/**
 * Copyright (C) 2009-2013 Enstratius, Inc.
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

import java.util.Map;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.aws.compute.EC2Method;
import org.dasein.cloud.identity.ServiceAction;

import javax.annotation.Nonnull;

public class SQS {
    static public final String SET_QUEUE_ATTRIBUTES = "SetQueueAttributes";


    static public @Nonnull ServiceAction[] asSQSServiceAction(@Nonnull String action) {
        return null; // TODO: implement me
    }
    
    private AWSCloud provider;
    
    SQS(AWSCloud provider) {
        this.provider = provider;
    }
    
    public String createQueue(String name, String description, int timeoutInSeconds) throws InternalException, CloudException {
        return null; // TODO: implement me
    }
    
    public void getQueue(String queueId) throws InternalException, CloudException {
        // TODO: implement me
    }
    
    public Map<String,String> getQueueAttributes(String queueId, String attribute) throws InternalException, CloudException {
        return null; // TODO: implement me
    }
    
    public void listQueues() throws InternalException, CloudException {
        // TODO: implement me
    }


    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];     // TODO: implement me
    }

    public void receiveMessages(String queueId, int maxMessages, int timeoutInSeconds) throws InternalException, CloudException {
        // TODO: implement me
    }
    
    public void removeMessage(String messageId) throws InternalException, CloudException {
        // TODO: implement me
    }
    
    public void removeQueue(String queueId) throws InternalException, CloudException {
        // TODO: implement me
    }
    
    public String sendMessage(String queueId, String message) throws InternalException, CloudException {
        return null; // TODO: implement me
    }
    
    public void setQueueAttribute(String queueId, String name, String value) throws InternalException, CloudException {
        Map<String,String> parameters = provider.getStandardSqsParameters(provider.getContext(), SET_QUEUE_ATTRIBUTES);
        EC2Method method;

        parameters.put("Attribute.Name", name);
        parameters.put("Attribute.Value", value);
        method = new EC2Method(provider, "https://queue.amazonaws.com/" + provider.getContext().getAccountNumber() + "/" + queueId + "/", parameters);
        try {
            method.invoke();
        }
        catch( EC2Exception e ) {
            throw new CloudException(e);
        };
    }
    
    public void setTimeout(String messageId, int timeoutInMinutes) throws InternalException, CloudException {
        // TODO: implement me
    }
    
    /*
     * stuff on how to configure SNS access
        parameters.put("Attribute.Name", "Policy");
        parameters.put("Attribute.Value", "");
        method = new Ec2Method(provider, "https://queue.amazonaws.com/" + provider.getContext().getAccountNumber() + "/" + queueId + "/", parameters);
        try {
            method.invoke();
        }
        catch( Ec2Exception e ) {
            throw new CloudException(e);
        };
        parameters = provider.getStandardSqsParameters(provider.getContext(), SET_QUEUE_ATTRIBUTES);
        parameters.put("Attribute.Name", "Policy");
        parameters.put("Attribute.Value", getFile(others[4]));
        method = new Ec2Method(provider, "https://queue.amazonaws.com/" + others[0] + "/" + others[3] + "/", parameters);
        try {
            method.invoke();
        }
        catch( Ec2Exception e ) {
            throw new CloudException(e);
        };
    }
    
    static private String getFile(String fileName) throws IOException {
        File f= new File(fileName);
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
        
        return reader.readLine().trim();
    }
    */
}
