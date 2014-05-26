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

package org.dasein.cloud.aws.identity;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.admin.PrepaymentSupport;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.aws.compute.EC2Method;
import org.dasein.cloud.aws.network.ELBMethod;
import org.dasein.cloud.aws.network.Route53Method;
import org.dasein.cloud.aws.platform.CloudFrontMethod;
import org.dasein.cloud.aws.platform.RDS;
import org.dasein.cloud.aws.platform.SNS;        
import org.dasein.cloud.aws.platform.SQS;
import org.dasein.cloud.aws.platform.SimpleDB;
import org.dasein.cloud.aws.storage.S3Method;
import org.dasein.cloud.compute.AutoScalingSupport;
import org.dasein.cloud.compute.ComputeServices;
import org.dasein.cloud.compute.MachineImageSupport;
import org.dasein.cloud.compute.SnapshotSupport;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.compute.VolumeSupport;
import org.dasein.cloud.identity.AccessKey;
import org.dasein.cloud.identity.CloudGroup;
import org.dasein.cloud.identity.CloudPermission;
import org.dasein.cloud.identity.CloudPolicy;
import org.dasein.cloud.identity.CloudUser;
import org.dasein.cloud.identity.IdentityAndAccessSupport;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.identity.ShellKeySupport;
import org.dasein.cloud.network.DNSSupport;
import org.dasein.cloud.network.FirewallSupport;
import org.dasein.cloud.network.IpAddressSupport;
import org.dasein.cloud.network.LoadBalancerSupport;
import org.dasein.cloud.platform.CDNSupport;
import org.dasein.cloud.platform.KeyValueDatabaseSupport;
import org.dasein.cloud.platform.MQSupport;
import org.dasein.cloud.platform.PushNotificationSupport;
import org.dasein.cloud.platform.RelationalDatabaseSupport;
import org.dasein.cloud.storage.BlobStoreSupport;
import org.dasein.cloud.util.APITrace;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of the AWS IAM APIs based on the Dasein Cloud identity and access support.
 * @author George Reese (george.reese@imaginary.com)
 * @since 2012.02
 * @version 2012.02
 */
public class IAM implements IdentityAndAccessSupport {
    static private final Logger logger = AWSCloud.getLogger(IAM.class);
    
    private AWSCloud provider;

    public IAM(@Nonnull AWSCloud cloud) {
        provider = cloud;
    }
    
    @Override
    public void addUserToGroups(@Nonnull String providerUserId, @Nonnull String... providerGroupIds) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.addUserToGroups");
        try {
            ProviderContext ctx = provider.getContext();
    
            if( ctx == null ) {
                logger.error("No context was established for the attempt at addUserToGroups()");
                throw new InternalException("No context was established for this call.");
            }
            if( logger.isInfoEnabled() ) {
                logger.info("Adding " + providerUserId + " to " + providerGroupIds.length + " groups...");
            }
            for( String groupId : providerGroupIds ) {
                addUserToGroup(ctx, providerUserId, groupId);
            }
            if( logger.isInfoEnabled() ) {
                logger.info("User " + providerUserId + " successfully added to all groups.");
            }
        }
        finally {
            APITrace.end();
        }
    }

    /**
     * Executes the function of adding a user to a specific group as AWS does not support bulk adding of a user to a group.
     * @param ctx the current cloud context
     * @param providerUserId the user to be added
     * @param providerGroupId the group to which the user will be added
     * @throws CloudException an error occurred in the cloud provider adding this user to the specified group
     * @throws InternalException an error occurred within Dasein Cloud adding the user to the group
     */
    private void addUserToGroup(@Nonnull ProviderContext ctx, @Nonnull String providerUserId, @Nonnull String providerGroupId) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER: " + IAM.class.getName() + ".addUserToGroup(" + ctx + "," + providerUserId + "," + providerGroupId + ")");
        }
        try {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.ADD_USER_TO_GROUP, IAMMethod.VERSION);
            EC2Method method;

            CloudUser user = getUser(providerUserId);
            
            if( user == null ) {
                throw new CloudException("No such user: " + providerUserId);
            }
            
            CloudGroup group = getGroup(providerGroupId);
            
            if( group == null ) {
                throw new CloudException("No such group: " + providerGroupId);
            }
            parameters.put("GroupName", group.getName());
            parameters.put("UserName", user.getUserName());
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                if( logger.isInfoEnabled() ) {
                    logger.info("Adding " + providerUserId + " to " + providerGroupId + "...");
                }
                method.invoke();
                if( logger.isInfoEnabled() ) {
                    logger.info("Added.");
                }
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT: " + IAM.class.getName() + ".addUserToGroup()");
            }
        }
    }

    @Override
    public @Nonnull CloudGroup createGroup(@Nonnull String groupName, @Nullable String path, boolean asAdminGroup) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.createGroup");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for the attempt at createGroup()");
                throw new InternalException("No context was established for this call.");
            }

            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.CREATE_GROUP, IAMMethod.VERSION);
            EC2Method method;
            NodeList blocks;
            Document doc;

            groupName = validateName(groupName);
            parameters.put("GroupName", groupName);
            if( path != null ) {
                if( !path.endsWith("/") ) {
                    path = path + "/";
                }
                parameters.put("Path", path);
            }
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                if( logger.isInfoEnabled() ) {
                    logger.info("Creating group " + groupName + " in " + path + "...");
                }
                doc = method.invoke();
                blocks = doc.getElementsByTagName("Group");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    CloudGroup cloudGroup = toGroup(ctx, blocks.item(i));
                    
                    if( logger.isDebugEnabled() ) {
                        logger.debug("cloudGroup=" + cloudGroup);
                    }
                    if( cloudGroup != null ) {
                        if( logger.isInfoEnabled() ) {
                            logger.info("Created.");
                        }
                        if( asAdminGroup ) {
                            logger.info("Setting up admin group rights for new group " + cloudGroup);
                            saveGroupPolicy(cloudGroup.getProviderGroupId(), "AdminGroup", CloudPermission.ALLOW, null, null);
                        }
                        return cloudGroup;
                    }                    
                }
                logger.error("No group was created as a result of the request");
                throw new CloudException("No group was created as a result of the request");
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
    public @Nonnull CloudUser createUser(@Nonnull String userName, @Nullable String path, @Nullable String... autoJoinGroupIds) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.createUser");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for the attempt at createUser()");
                throw new InternalException("No context was established for this call.");
            }

            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.CREATE_USER, IAMMethod.VERSION);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("UserName", userName);
            if( path != null ) {
                if( !path.endsWith("/") ) {
                    path = path + "/";
                }
                parameters.put("Path", path);
            }
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                if( logger.isInfoEnabled() ) {
                    logger.info("Creating user " + userName + " in " + path + "...");
                }
                doc = method.invoke();
                blocks = doc.getElementsByTagName("User");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    CloudUser cloudUser = toUser(ctx, blocks.item(i));

                    if( logger.isDebugEnabled() ) {
                        logger.debug("cloudUser=" + cloudUser);
                    }
                    if( cloudUser != null ) {
                        if( logger.isInfoEnabled() ) {
                            logger.info("Created.");
                        }
                        return cloudUser;
                    }
                }
                logger.error("No user was created as a result of the request");
                throw new CloudException("No user was created as a result of the request");
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
    public @Nonnull  AccessKey enableAPIAccess(@Nonnull String providerUserId) throws CloudException, InternalException {
        APITrace.begin(provider, "enableAPIAccess");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for this request.");
                throw new InternalException("No context was established for this request");
            }

            CloudUser user = getUser(providerUserId);

            if( user == null ) {
                throw new CloudException("No such user: " + providerUserId);
            }
            
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.CREATE_ACCESS_KEY, IAMMethod.VERSION);
            IAMMethod method;
            NodeList blocks;
            Document doc;

            parameters.put("UserName", user.getUserName());
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                if( logger.isInfoEnabled() ) {
                    logger.info("Creating access keys for " + providerUserId);
                }
                doc = method.invoke();
                blocks = doc.getElementsByTagName("AccessKey");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    AccessKey key = toAccessKey(ctx, blocks.item(i));

                    if( logger.isDebugEnabled() ) {
                        logger.debug("key=" + key);
                    }
                    if( key != null ) {
                        if( logger.isInfoEnabled() ) {
                            logger.info("Created.");
                        }
                        return key;
                    }
                }
                logger.error("No access key was created as a result of the request");
                throw new CloudException("No access key was created as a result of the request");
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
    public void enableConsoleAccess(@Nonnull String providerUserId, @Nonnull byte[] password) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.enableConsoleAccess");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for this request.");
                throw new InternalException("No context was established for this request");
            }

            CloudUser user = getUser(providerUserId);

            if( user == null ) {
                throw new CloudException("No such user: " + providerUserId);
            }
            
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.CREATE_LOGIN_PROFILE, IAMMethod.VERSION);
            IAMMethod method;
            NodeList blocks;
            Document doc;

            parameters.put("UserName", user.getUserName());
            try {
                parameters.put("Password", new String(password, "utf-8"));
            }
            catch( UnsupportedEncodingException e ) {
                throw new InternalException(e);
            }
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=[omitted due to password sensitivity]");
            }
            method = new IAMMethod(provider, parameters);
            try {
                if( logger.isInfoEnabled() ) {
                    logger.info("Creating console access for " + providerUserId);
                }
                doc = method.invoke();
                blocks = doc.getElementsByTagName("LoginProfile");
                if( blocks.getLength() < 1 ) {
                    logger.error("No console access was created as a result of the request");
                    throw new CloudException("No console access was created as a result of the request");
                }
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
    public @Nullable CloudGroup getGroup(@Nonnull String providerGroupId) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.getGroup");
        try {
            for( CloudGroup group : listGroups(null) ) {
                if( providerGroupId.equals(group.getProviderGroupId()) ) {
                    return group;
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    private @Nullable CloudPolicy[] getGroupPolicy(@Nonnull CloudGroup group, @Nonnull String policyName) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER: " + IAM.class.getName() + ".getGroupPolicy(" + group + "," + policyName + ")");
        }
        try {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.GET_GROUP_POLICY, IAMMethod.VERSION);
            EC2Method method;
            NodeList blocks;
            Document doc;
    
            parameters.put("GroupName", group.getName());
            parameters.put("PolicyName", policyName);
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                doc = method.invoke();
                blocks = doc.getElementsByTagName("GetGroupPolicyResult");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    Node policyNode = blocks.item(i);
    
                    if( policyNode.hasChildNodes() ) {
                        NodeList attrs = policyNode.getChildNodes();
    
                        for( int j=0; j<attrs.getLength(); j++ ) {
                            Node attr = attrs.item(j);
                            
                            if( attr.getNodeName().equalsIgnoreCase("PolicyDocument") ) {
                                String json = URLDecoder.decode(attr.getFirstChild().getNodeValue().trim(), "utf-8");
                                JSONObject stmt = new JSONObject(json);
                                
                                if( stmt.has("Statement") ) {
                                    return toPolicy(policyName, stmt.getJSONArray("Statement"));
                                }
                            }
                        }
                    }
                }
                return null;
            }
            catch( EC2Exception e ) {
                if( e.getStatus() == 404 ) {
                    return null;
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            catch( JSONException e ) {
                logger.error("Failed to parse policy statement: " + e.getMessage());
                throw new CloudException(e);
            }
            catch( UnsupportedEncodingException e ) {
                logger.error("Unknown encoding in utf-8: " + e.getMessage());
                throw new InternalException(e);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT: " + IAM.class.getName() + ".getGroupPolicy()");
            }
        }
    }

    @Override
    public @Nullable CloudUser getUser(@Nonnull String providerUserId) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.getUser");
        try {
            for( CloudUser user : this.listUsersInPath(null) ) {
                if( providerUserId.equals(user.getProviderUserId()) ) {
                    return user;
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    private @Nullable CloudUser getUserByName(@Nonnull String userName) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER: " + IAM.class.getName() + ".getUserByName(" + userName + ")");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for this request.");
                throw new InternalException("No context was established for this request");
            }

            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.LIST_USERS, IAMMethod.VERSION);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("UserName", userName);
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                doc = method.invoke();
                blocks = doc.getElementsByTagName("member");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    CloudUser cloudUser = toUser(ctx, blocks.item(i));

                    if( cloudUser != null ) {
                        if( logger.isDebugEnabled() ) {
                            logger.debug("cloudUser=" + cloudUser);
                        }
                        return cloudUser;
                    }
                }
                if( logger.isDebugEnabled() ) {
                    logger.debug("cloudUser=null");
                }
                return null;
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT: " + IAM.class.getName() + ".getUserByName()");
            }
        }        
    }
    
    private @Nullable CloudPolicy[] getUserPolicy(@Nonnull CloudUser user, @Nonnull String policyName) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER: " + IAM.class.getName() + ".getUserPolicy(" + user + "," + policyName + ")");
        }
        try {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.GET_USER_POLICY, IAMMethod.VERSION);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("UserName", user.getUserName());
            parameters.put("PolicyName", policyName);
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                doc = method.invoke();
                blocks = doc.getElementsByTagName("GetUserPolicyResult");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    Node policyNode = blocks.item(i);

                    if( policyNode.hasChildNodes() ) {
                        NodeList attrs = policyNode.getChildNodes();

                        for( int j=0; j<attrs.getLength(); j++ ) {
                            Node attr = attrs.item(j);

                            if( attr.getNodeName().equalsIgnoreCase("PolicyDocument") ) {
                                String json = URLDecoder.decode(attr.getFirstChild().getNodeValue().trim(), "utf-8");
                                JSONObject stmt = new JSONObject(json);

                                if( stmt.has("Statement") ) {
                                    return toPolicy(policyName, stmt.getJSONArray("Statement"));
                                }
                            }
                        }
                    }
                }
                return null;
            }
            catch( EC2Exception e ) {
                if( e.getStatus() == 404 ) {
                    return null;
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            catch( JSONException e ) {
                logger.error("Failed to parse policy statement: " + e.getMessage());
                throw new CloudException(e);
            }
            catch( UnsupportedEncodingException e ) {
                logger.error("Unknown encoding in utf-8: " + e.getMessage());
                throw new InternalException(e);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT: " + IAM.class.getName() + ".getUserPolicy()");
            }
        }
    }
    
    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.isSubscribed");
        try {
            ComputeServices svc = provider.getComputeServices();

            if( svc == null ) {
                return false;
            }
            VirtualMachineSupport support = svc.getVirtualMachineSupport();

            return (support != null && support.isSubscribed());
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<CloudGroup> listGroups(@Nullable String pathBase) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.listGroups");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for this request.");
                throw new InternalException("No context was established for this request");
            }

            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.LIST_GROUPS, IAMMethod.VERSION);
            EC2Method method;
            NodeList blocks;
            Document doc;

            if( pathBase != null ) {
                parameters.put("PathPrefix", pathBase);
            }
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                ArrayList<CloudGroup> groups = new ArrayList<CloudGroup>();

                doc = method.invoke();
                blocks = doc.getElementsByTagName("member");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    CloudGroup cloudGroup = toGroup(ctx, blocks.item(i));

                    if( cloudGroup != null ) {
                        groups.add(cloudGroup);
                    }
                }
                if( logger.isDebugEnabled() ) {
                    logger.debug("groups=" + groups);
                }
                return groups;
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
    public @Nonnull Iterable<CloudGroup> listGroupsForUser(@Nonnull String providerUserId) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.listGroupsForUser");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for this request.");
                throw new InternalException("No context was established for this request");
            }

            CloudUser user = getUser(providerUserId);

            if( user == null ) {
                throw new CloudException("No such user: " + providerUserId);
            }
            
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.LIST_GROUPS_FOR_USER, IAMMethod.VERSION);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("UserName", user.getUserName());
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                ArrayList<CloudGroup> groups = new ArrayList<CloudGroup>();

                doc = method.invoke();
                blocks = doc.getElementsByTagName("member");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    CloudGroup cloudGroup = toGroup(ctx, blocks.item(i));

                    if( cloudGroup != null ) {
                        groups.add(cloudGroup);
                    }
                }
                if( logger.isDebugEnabled() ) {
                    logger.debug("groups=" + groups);
                }
                return groups;
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
    public @Nonnull Iterable<CloudPolicy> listPoliciesForGroup(@Nonnull String providerGroupId) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.listPoliciesForGroup");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for this request.");
                throw new InternalException("No context was established for this request");
            }

            CloudGroup group = getGroup(providerGroupId);

            if( group == null ) {
                throw new CloudException("No such group: " + providerGroupId);
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.LIST_GROUP_POLICIES, IAMMethod.VERSION);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("GroupName", group.getName());
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                ArrayList<String> names = new ArrayList<String>();

                doc = method.invoke();
                blocks = doc.getElementsByTagName("member");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    Node member = blocks.item(i);
                    
                    if( member.hasChildNodes() ) {
                        String name = member.getFirstChild().getNodeValue().trim();
                        
                        if( name.length() > 0 ) {
                            names.add(name);
                        }
                    }
                }
                ArrayList<CloudPolicy> policies = new ArrayList<CloudPolicy>();

                for( String name : names ) {
                    Collections.addAll(policies, getGroupPolicy(group, name));
                }
                if( logger.isDebugEnabled() ) {
                    logger.debug("policies=" + policies);
                }
                return policies;
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
    public @Nonnull Iterable<CloudPolicy> listPoliciesForUser(@Nonnull String providerUserId) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.listPoliciesForUser");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for this request.");
                throw new InternalException("No context was established for this request");
            }

            CloudUser user = getUser(providerUserId);

            if( user == null ) {
                throw new CloudException("No such user: " + providerUserId);
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.LIST_USER_POLICIES, IAMMethod.VERSION);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("UserName", user.getUserName());
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                ArrayList<String> names = new ArrayList<String>();

                doc = method.invoke();
                blocks = doc.getElementsByTagName("member");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    Node member = blocks.item(i);

                    if( member.hasChildNodes() ) {
                        String name = member.getFirstChild().getNodeValue().trim();

                        if( name.length() > 0 ) {
                            names.add(name);
                        }
                    }
                }
                ArrayList<CloudPolicy> policies = new ArrayList<CloudPolicy>();

                for( String name : names ) {
                    Collections.addAll(policies, getUserPolicy(user, name));
                }
                if( logger.isDebugEnabled() ) {
                    logger.debug("policies=" + policies);
                }
                return policies;
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
    public @Nonnull Iterable<CloudUser> listUsersInGroup(@Nonnull String inProviderGroupId) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.listUsersInGroup");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for this request.");
                throw new InternalException("No context was established for this request");
            }

            CloudGroup group = getGroup(inProviderGroupId);

            if( group == null ) {
                throw new CloudException("No such group: " + inProviderGroupId);
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.GET_GROUP, IAMMethod.VERSION);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("GroupName", group.getName());
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                ArrayList<CloudUser> users = new ArrayList<CloudUser>();
                
                doc = method.invoke();
                blocks = doc.getElementsByTagName("member");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    CloudUser user = toUser(ctx, blocks.item(i));

                    if( user != null ) {
                        users.add(user);
                    }
                }
                if( logger.isDebugEnabled() ) {
                    logger.debug("users=" + users);
                }
                return users;
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
    public @Nonnull Iterable<CloudUser> listUsersInPath(@Nullable String pathBase) throws CloudException, InternalException {
        APITrace.begin(provider, "listUsersInPath");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for this request.");
                throw new InternalException("No context was established for this request");
            }

            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.LIST_USERS, IAMMethod.VERSION);
            EC2Method method;
            NodeList blocks;
            Document doc;

            if( pathBase != null ) {
                parameters.put("PathPrefix", pathBase);
            }
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                ArrayList<CloudUser> users = new ArrayList<CloudUser>();

                doc = method.invoke();
                blocks = doc.getElementsByTagName("member");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    CloudUser cloudUser = toUser(ctx, blocks.item(i));

                    if( cloudUser != null ) {
                        users.add(cloudUser);
                    }
                }
                if( logger.isDebugEnabled() ) {
                    logger.debug("users=" + users);
                }
                return users;
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
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        if( action.equals(IdentityAndAccessSupport.ANY) ) {
            return new String[] { IAMMethod.IAM_PREFIX + "*" };
        }
        else if( action.equals(IdentityAndAccessSupport.ADD_GROUP_ACCESS) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.PUT_GROUP_POLICY };
        }
        else if( action.equals(IdentityAndAccessSupport.ADD_USER_ACCESS) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.PUT_USER_POLICY };
        }
        else if( action.equals(IdentityAndAccessSupport.CREATE_GROUP) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.CREATE_GROUP };
        }
        else if( action.equals(IdentityAndAccessSupport.CREATE_USER) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.CREATE_USER };
        }
        else if( action.equals(IdentityAndAccessSupport.DISABLE_API) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.DELETE_ACCESS_KEY };
        }
        else if( action.equals(IdentityAndAccessSupport.DISABLE_CONSOLE) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.DELETE_LOGIN_PROFILE };
        }
        else if( action.equals(IdentityAndAccessSupport.DROP_FROM_GROUP) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.REMOVE_USER_FROM_GROUP };
        }
        else if( action.equals(IdentityAndAccessSupport.ENABLE_API) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.CREATE_ACCESS_KEY };
        }
        else if( action.equals(IdentityAndAccessSupport.ENABLE_CONSOLE) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.CREATE_LOGIN_PROFILE };
        }
        else if( action.equals(IdentityAndAccessSupport.GET_ACCESS_KEY) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.GET_ACCESS_KEY };
        }
        else if( action.equals(IdentityAndAccessSupport.GET_GROUP) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.GET_GROUP };
        }
        else if( action.equals(IdentityAndAccessSupport.GET_GROUP_POLICY) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.GET_GROUP_POLICY, IAMMethod.IAM_PREFIX + IAMMethod.LIST_GROUP_POLICIES };
        }
        else if( action.equals(IdentityAndAccessSupport.GET_USER) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.GET_USER };
        }
        else if( action.equals(IdentityAndAccessSupport.GET_USER_POLICY) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.GET_USER_POLICY, IAMMethod.IAM_PREFIX + IAMMethod.LIST_USER_POLICIES };
        }
        else if( action.equals(IdentityAndAccessSupport.JOIN_GROUP) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.ADD_USER_TO_GROUP };
        }
        else if( action.equals(IdentityAndAccessSupport.LIST_ACCESS_KEY) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.LIST_ACCESS_KEY };
        }
        else if( action.equals(IdentityAndAccessSupport.LIST_GROUP) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.LIST_GROUPS + "*" };
        }
        else if( action.equals(IdentityAndAccessSupport.LIST_USER) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.LIST_USERS };
        }
        else if( action.equals(IdentityAndAccessSupport.REMOVE_GROUP) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.DELETE_GROUP };
        }
        else if( action.equals(IdentityAndAccessSupport.REMOVE_GROUP_ACCESS) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.PUT_GROUP_POLICY };
        }
        else if( action.equals(IdentityAndAccessSupport.REMOVE_USER) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.DELETE_USER };
        }
        else if( action.equals(IdentityAndAccessSupport.REMOVE_USER_ACCESS) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.PUT_USER_POLICY };
        }
        else if( action.equals(IdentityAndAccessSupport.UPDATE_GROUP) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.UPDATE_GROUP };
        }
        else if( action.equals(IdentityAndAccessSupport.UPDATE_USER) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.UPDATE_USER };
        }

        /* SSL certificates were explicitly requested to be included into LoadBalancingSupport
         * by the upstream author */
        else if( action.equals(LoadBalancerSupport.LIST_SSL_CERTIFICATES) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.LIST_SSL_CERTIFICATES };
        }
        else if( action.equals(LoadBalancerSupport.GET_SSL_CERTIFICATE) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.GET_SSL_CERTIFICATE };
        }
        else if( action.equals(LoadBalancerSupport.CREATE_SSL_CERTIFICATE) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.CREATE_SSL_CERTIFICATE };
        }
        else if( action.equals(LoadBalancerSupport.DELETE_SSL_CERTIFICATE) ) {
            return new String[] { IAMMethod.IAM_PREFIX + IAMMethod.DELETE_SSL_CERTIFICATE };
        }
        return new String[0];
    }

    public void removeAccessKey(@Nonnull String sharedKeyPart) throws CloudException, InternalException {
    	//nothing for aws
    }
    
    @Override
    public void removeAccessKey(@Nonnull String sharedKeyPart, @Nonnull String providerUserId) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.removeAccessKey");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for this request.");
                throw new InternalException("No context was established for this request");
            }
            CloudUser user = getUser(providerUserId);

            if( user == null ) {
                return;
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.DELETE_ACCESS_KEY, IAMMethod.VERSION);
            IAMMethod method;

            parameters.put("AccessKeyId", sharedKeyPart);
            parameters.put("UserName", user.getUserName());
            
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                if( logger.isInfoEnabled() ) {
                    logger.info("Removing access key for " + sharedKeyPart);
                }
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
    public void removeConsoleAccess(@Nonnull String providerUserId) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.removeConsoleAccess");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for this request.");
                throw new InternalException("No context was established for this request");
            }
            CloudUser user = getUser(providerUserId);

            if( user == null ) {
                throw new CloudException("No such user: " + providerUserId);
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.DELETE_LOGIN_PROFILE, IAMMethod.VERSION);
            IAMMethod method;

            parameters.put("UserName", user.getUserName());
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                if( logger.isInfoEnabled() ) {
                    logger.info("Removing console access for " + providerUserId);
                }
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
    public void removeGroup(@Nonnull String providerGroupId) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.removeGroup");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for this request.");
                throw new InternalException("No context was established for this request");
            }
            CloudGroup group = getGroup(providerGroupId);
            
            if( group == null ) {
                throw new CloudException("No such group: " + providerGroupId);
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.DELETE_GROUP, IAMMethod.VERSION);
            IAMMethod method;

            parameters.put("GroupName", group.getName());
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                if( logger.isInfoEnabled() ) {
                    logger.info("Removing group " + providerGroupId);
                }
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
    public void removeGroupPolicy(@Nonnull String providerGroupId, @Nonnull String providerPolicyId) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.removeGroupPolicy");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for this request.");
                throw new InternalException("No context was established for this request");
            }
            CloudGroup group = getGroup(providerGroupId);

            if( group == null ) {
                throw new CloudException("No such group: " + providerGroupId);
            }

            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.DELETE_GROUP_POLICY, IAMMethod.VERSION);
            EC2Method method;

            parameters.put("GroupName", group.getName());
            parameters.put("PolicyName", providerPolicyId);

            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                if( logger.isInfoEnabled() ) {
                    logger.info("Removing policy for group " + providerGroupId);
                }
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
    public void removeUser(@Nonnull String providerUserId) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.removeUser");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for this request.");
                throw new InternalException("No context was established for this request");
            }
            CloudUser user = getUser(providerUserId);
            
            if( user == null ) {
                throw new CloudException("No such user: " + providerUserId);
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.DELETE_USER, IAMMethod.VERSION);
            IAMMethod method;

            parameters.put("UserName", user.getUserName());
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                if( logger.isInfoEnabled() ) {
                    logger.info("Removing user " + providerUserId);
                }
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
    public void removeUserPolicy(@Nonnull String providerUserId, @Nonnull String providerPolicyId) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.removeUserPolicy");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for this request.");
                throw new InternalException("No context was established for this request");
            }
            CloudUser user = getUser(providerUserId);

            if( user == null ) {
                throw new CloudException("No such user: " + providerUserId);
            }

            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.DELETE_USER_POLICY, IAMMethod.VERSION);
            EC2Method method;

            parameters.put("UserName", user.getUserName());
            parameters.put("PolicyName", providerPolicyId);

            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                if( logger.isInfoEnabled() ) {
                    logger.info("Removing policy for user " + providerUserId);
                }
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
    public void removeUserFromGroup(@Nonnull String providerUserId, @Nonnull String providerGroupId) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.removeUserFromGroup");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for this request.");
                throw new InternalException("No context was established for this request");
            }

            CloudUser user = getUser(providerUserId);

            if( user == null ) {
                throw new CloudException("No such user: " + providerUserId);
            }

            CloudGroup group = getGroup(providerGroupId);

            if( group == null ) {
                throw new CloudException("No such group: " + providerGroupId);
            }

            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.REMOVE_USER_FROM_GROUP, IAMMethod.VERSION);
            IAMMethod method;

            parameters.put("UserName", user.getUserName());
            parameters.put("GroupName", group.getName());
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                if( logger.isInfoEnabled() ) {
                    logger.info("Removing user " + providerUserId + " from " + providerGroupId);
                }
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
    public void saveGroup(@Nonnull String providerGroupId, @Nullable String newGroupName, @Nullable String newPath) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.saveGroup");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for this request.");
                throw new InternalException("No context was established for this request");
            }

            CloudGroup group = getGroup(providerGroupId);

            if( group == null ) {
                throw new CloudException("No such group: " + providerGroupId);
            }

            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.UPDATE_GROUP, IAMMethod.VERSION);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("GroupName", group.getName());
            if( newGroupName != null) {
                parameters.put("NewGroupName", providerGroupId);
            }
            if( newPath != null ) {
                parameters.put("NewPath", newPath);
            }
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                if( logger.isInfoEnabled() ) {
                    logger.info("Updating group " + providerGroupId + " with " + newPath + " - " + newGroupName + "...");
                }
                doc = method.invoke();
                blocks = doc.getElementsByTagName("Group");
                if( blocks.getLength() < 1 ) {
                    logger.error("No group was updated as a result of the request");
                    throw new CloudException("No group was updated as a result of the request");
                }
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
    public @Nonnull String[] saveGroupPolicy(@Nonnull String providerGroupId, @Nonnull String name, @Nonnull CloudPermission permission, @Nullable ServiceAction action, @Nullable String resourceId) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.saveGroupPolicy");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for this request.");
                throw new InternalException("No context was established for this request");
            }
            CloudGroup group = getGroup(providerGroupId);

            if( group == null ) {
                throw new CloudException("No such group: " + providerGroupId);
            }

            String[] actions = (action == null ? new String[] { "*" } : action.map(provider));
            String[] ids = new String[actions.length];
            int i = 0;

            for( String actionId : actions ) {
                Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.PUT_GROUP_POLICY, IAMMethod.VERSION);
                String policyName = name + "+" + (actionId.equals("*") ? "ANY" : actionId.replaceAll(":", "_"));

                EC2Method method;

                parameters.put("GroupName", group.getName());
                parameters.put("PolicyName", policyName);
    
                ArrayList<Map<String,Object>> policies = new ArrayList<Map<String, Object>>();
                HashMap<String,Object> statement = new HashMap<String, Object>();
                HashMap<String,Object> policy = new HashMap<String, Object>();
    
                policy.put("Effect", permission.equals(CloudPermission.ALLOW) ? "Allow" : "Deny");
                policy.put("Action", actionId);
                policy.put("Resource", resourceId == null ? "*" : resourceId);
                policies.add(policy);
                statement.put("Statement", policies);
                
                parameters.put("PolicyDocument", (new JSONObject(statement)).toString());
                if( logger.isDebugEnabled() ) {
                    logger.debug("parameters=" + parameters);
                }
                method = new IAMMethod(provider, parameters);
                try {
                    if( logger.isInfoEnabled() ) {
                        logger.info("Updating policy for group " + providerGroupId);
                    }
                    method.invoke();
                }
                catch( EC2Exception e ) {
                    logger.error(e.getSummary());
                    throw new CloudException(e);
                }
                ids[i++] = policyName;
            }
            return ids;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public String[] saveUserPolicy(@Nonnull String providerUserId, @Nonnull String name, @Nonnull CloudPermission permission, @Nullable ServiceAction action, @Nullable String resourceId) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.saveUserPolicy");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for this request.");
                throw new InternalException("No context was established for this request");
            }
            CloudUser user = getUser(providerUserId);

            if( user == null ) {
                throw new CloudException("No such user: " + providerUserId);
            }

            String[] actions = (action == null ? new String[] { "*" } : action.map(provider));
            String[] ids = new String[actions.length];
            int i = 0;

            for( String actionId : actions ) {
                Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.PUT_USER_POLICY, IAMMethod.VERSION);
                String policyName = name + "+" + (actionId.equals("*") ? "ANY" : actionId.replaceAll(":", "_"));
                EC2Method method;

                parameters.put("UserName", user.getUserName());
                parameters.put("PolicyName", policyName);

                ArrayList<Map<String,Object>> policies = new ArrayList<Map<String, Object>>();
                HashMap<String,Object> statement = new HashMap<String, Object>();
                HashMap<String,Object> policy = new HashMap<String, Object>();

                policy.put("Effect", permission.equals(CloudPermission.ALLOW) ? "Allow" : "Deny");
                policy.put("Action", actionId);
                policy.put("Resource", resourceId == null ? "*" : resourceId);
                policies.add(policy);
                statement.put("Statement", policies);

                parameters.put("PolicyDocument", (new JSONObject(statement)).toString());
                if( logger.isDebugEnabled() ) {
                    logger.debug("parameters=" + parameters);
                }
                method = new IAMMethod(provider, parameters);
                try {
                    if( logger.isInfoEnabled() ) {
                        logger.info("Updating policy for user " + providerUserId);
                    }
                    method.invoke();
                }
                catch( EC2Exception e ) {
                    logger.error(e.getSummary());
                    throw new CloudException(e);
                }
                ids[i++] = policyName;
            }
            return ids;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void saveUser(@Nonnull String providerUserId, @Nullable String newUserName, @Nullable String newPath) throws CloudException, InternalException {
        APITrace.begin(provider, "IAM.saveUser");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was established for the request.");
                throw new InternalException("No context was established for this request");
            }
            CloudUser user = getUser(providerUserId);

            if( user == null ) {
                throw new CloudException("No such user: " + providerUserId);
            }

            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IAMMethod.UPDATE_USER, IAMMethod.VERSION);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("UserName", user.getUserName());
            if( newUserName != null ) {
                parameters.put("NewUserName", newUserName);
            }
            if( newPath != null ) {
                parameters.put("NewPath", newPath);
            }
            if( logger.isDebugEnabled() ) {
                logger.debug("parameters=" + parameters);
            }
            method = new IAMMethod(provider, parameters);
            try {
                if( logger.isInfoEnabled() ) {
                    logger.info("Updating user " + providerUserId + " with " + newPath + " - " + newUserName + "...");
                }
                doc = method.invoke();
                blocks = doc.getElementsByTagName("User");
                if( blocks.getLength() < 1 ) {
                    logger.error("No user was updated as a result of the request");
                    throw new CloudException("No user was updated as a result of the request");
                }
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
    public boolean supportsAccessControls() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean supportsConsoleAccess() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean supportsAPIAccess() throws CloudException, InternalException {
        return true;
    }
    
    private @Nullable AccessKey toAccessKey(@Nonnull ProviderContext ctx, @Nullable Node node) throws CloudException, InternalException {
        if( node == null ) {
            return null;
        }
        NodeList attributes = node.getChildNodes();
        AccessKey key = new AccessKey();

        key.setProviderOwnerId(ctx.getAccountNumber());

        String userName = null;
        
        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);
            String attrName = attribute.getNodeName();

            if( attrName.equalsIgnoreCase("UserName") && attribute.hasChildNodes() ) {
                userName = attribute.getFirstChild().getNodeValue().trim();
            }
            else if( attrName.equalsIgnoreCase("AccessKeyId") && attribute.hasChildNodes() ) {
                key.setSharedPart(attribute.getFirstChild().getNodeValue().trim());
            }
            else if( attrName.equalsIgnoreCase("SecretAccessKey") && attribute.hasChildNodes() ) {
                try {
                    key.setSecretPart(attribute.getFirstChild().getNodeValue().trim().getBytes("utf-8"));
                }
                catch( UnsupportedEncodingException e ) {
                    throw new InternalException(e);
                }
            }
            else if( attrName.equalsIgnoreCase("Status") ) {
                if( !attribute.hasChildNodes() || !attribute.getFirstChild().getNodeValue().trim().equalsIgnoreCase("Active") ) {
                    return null;
                }
            }
        }
        if( userName != null ) {
            CloudUser user = getUserByName(userName);
            
            if( user == null ) {
                logger.warn("Found key " + key.getSharedPart() + " belonging to " + userName + ", but no matching user");
                return null;
            }
            String userId = user.getProviderUserId();

            if( userId == null ) {
                logger.warn("Found key " + key.getSharedPart() + " belonging to " + userName + ", but no matching user");
                return null;
            }
            key.setProviderUserId(userId);
        }
        if( key.getSharedPart() == null || key.getSecretPart() == null ) {
            return null;
        }
        return key;
    }

    private @Nullable CloudGroup toGroup(@Nonnull ProviderContext ctx, @Nullable Node node) throws CloudException, InternalException {
        if( node == null ) {
            return null;
        }
        NodeList attributes = node.getChildNodes();
        CloudGroup group = new CloudGroup();

        group.setPath("/");
        group.setProviderOwnerId(ctx.getAccountNumber());
        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);
            String attrName = attribute.getNodeName();
            
            if( attrName.equalsIgnoreCase("Path") && attribute.hasChildNodes() ) {
                group.setPath(attribute.getFirstChild().getNodeValue().trim());
            }
            else if( attrName.equalsIgnoreCase("GroupId") && attribute.hasChildNodes() ) {
                group.setProviderGroupId(attribute.getFirstChild().getNodeValue().trim());
            }
            else if( attrName.equalsIgnoreCase("GroupName") && attribute.hasChildNodes() ) {
                group.setName(attribute.getFirstChild().getNodeValue().trim());
            }
        }
        if( group.getName() == null || group.getProviderGroupId() == null ) {
            return null;
        }
        return group;
    }


    private @Nullable CloudPolicy[] toPolicy(@Nonnull String policyName, @Nonnull JSONArray policyStatements) throws JSONException {
        ArrayList<CloudPolicy> policyList = new ArrayList<CloudPolicy>();
        
        for( int i=0; i<policyStatements.length(); i++ ) {
            JSONObject policyStatement = policyStatements.getJSONObject(i);
            String effect = (policyStatement.has("Effect") ? policyStatement.getString("Effect") : null);
            String action = (policyStatement.has("Action") ? policyStatement.getString("Action") : null);
            String resource = (policyStatement.has("Resource") ? policyStatement.getString("Resource") : null);
            
            if( effect == null ) {
                return null;
            }
            CloudPermission permission = (effect.equalsIgnoreCase("allow") ? CloudPermission.ALLOW : CloudPermission.DENY);
            ServiceAction[] serviceActions = null;
            String resourceId = null;
            
            if( action != null ) {
                if( action.equals("*") ) {
                    serviceActions = null;
                }
                else {
                    int idx = action.indexOf(":");
                    String svc;
                    
                    if( idx < 1 ) {
                        svc = "ec2";
                        if( idx == 0 ) {
                            if( action.length() > 1 ) {
                                action = action.substring(1);                            
                            }
                            else {
                                action = "*";
                            }
                        }
                    }
                    else if( idx == action.length()-1 ) {
                        svc = action.substring(0, idx);
                        action = "*";
                    }
                    else {
                        svc = action.substring(0, idx);
                        action = action.substring(idx+1);                    
                    }
                    if( action.equals("*") ) {
                        action = null;
                    }
                    svc = svc + ":";
                    if( svc.equals(IAMMethod.IAM_PREFIX) ) {
                        if( action == null ) {
                            serviceActions = new ServiceAction[] { IdentityAndAccessSupport.ANY };
                        }
                        else {
                            serviceActions = IAMMethod.asIAMServiceAction(action);
                        }
                    }
                    else if( svc.equals(EC2Method.EC2_PREFIX) ) {
                        if( action == null ) {
                            serviceActions = new ServiceAction[] {
                                    PrepaymentSupport.ANY,
                                    VirtualMachineSupport.ANY, 
                                    MachineImageSupport.ANY, 
                                    VolumeSupport.ANY,
                                    SnapshotSupport.ANY, 
                                    IpAddressSupport.ANY, 
                                    FirewallSupport.ANY, 
                                    ShellKeySupport.ANY
                            };
                        }
                        else {
                            serviceActions = EC2Method.asEC2ServiceAction(action);
                        }
                    }
                    else if( svc.equals(Route53Method.R53_PREFIX) ) {
                        if( action == null ) {
                            serviceActions = new ServiceAction[] { DNSSupport.ANY };
                        }
                        else {
                            serviceActions = Route53Method.asRoute53ServiceAction(action);
                        }
                    }
                    else if( svc.equals(ELBMethod.ELB_PREFIX) ) {
                        if( action == null ) {
                            serviceActions = new ServiceAction[] { LoadBalancerSupport.ANY };
                        }
                        else {
                            serviceActions = ELBMethod.asELBServiceAction(action);
                        }
                    }
                    else if( svc.equals(CloudFrontMethod.CF_PREFIX) ) {
                        if( action == null ) {
                            serviceActions = new ServiceAction[] { CDNSupport.ANY };
                        }
                        else {
                            serviceActions = CloudFrontMethod.asCloudFrontServiceAction(action);
                        }
                    }
                    else if( svc.equals(EC2Method.AUTOSCALING_PREFIX) ) {
                        if( action == null ) {
                            serviceActions = new ServiceAction[] { AutoScalingSupport.ANY };
                        }
                        else {
                            serviceActions = EC2Method.asAutoScalingServiceAction(action);
                        }
                    }
                    else if( svc.equals(EC2Method.RDS_PREFIX) ) {
                        if( action == null ) {
                            serviceActions = new ServiceAction[] { RelationalDatabaseSupport.ANY };
                        }
                        else {
                            serviceActions = RDS.asRDSServiceAction(action);
                        }
                    }
                    else if( svc.equals(EC2Method.SDB_PREFIX) ) {
                        if( action == null ) {
                            serviceActions = new ServiceAction[] { KeyValueDatabaseSupport.ANY };
                        }
                        else {
                            serviceActions = SimpleDB.asSimpleDBServiceAction(action);
                        }
                    }
                    else if( svc.equals(EC2Method.SNS_PREFIX) ) {
                        if( action == null ) {
                            serviceActions = new ServiceAction[] { PushNotificationSupport.ANY };
                        }
                        else {
                            serviceActions = SNS.asSNSServiceAction(action);
                        }
                    }
                    else if( svc.equals(EC2Method.SQS_PREFIX) ) {
                        if( action == null ) {
                            serviceActions = new ServiceAction[] { MQSupport.ANY };
                        }
                        else {
                            serviceActions = SQS.asSQSServiceAction(action);
                        }
                    }
                    else if( svc.equals(S3Method.S3_PREFIX) ) {
                        if( action == null ) {
                            serviceActions = new ServiceAction[] { BlobStoreSupport.ANY };
                        }
                        else {
                            serviceActions = S3Method.asS3ServiceAction(action);
                        }
                    }
                    else {
                        serviceActions = new ServiceAction[0];
                    }
                }
            }
            if( resource != null && !resource.equals("*") ) {
                resourceId = resource;
            }
            if( serviceActions == null ) {
                return new CloudPolicy[] { CloudPolicy.getInstance(policyName, policyName, permission, null, resourceId) };
            }
            for( ServiceAction sa : serviceActions ) {
                policyList.add(CloudPolicy.getInstance(policyName, policyName, permission, sa, resourceId));
            }
        }
        return policyList.toArray(new CloudPolicy[policyList.size()]);
    }
    
    private @Nullable CloudUser toUser(@Nonnull ProviderContext ctx, @Nullable Node node) throws CloudException, InternalException {
        if( node == null ) {
            return null;
        }
        NodeList attributes = node.getChildNodes();
        CloudUser user = new CloudUser();

        user.setPath("/");
        user.setProviderOwnerId(ctx.getAccountNumber());
        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);
            String attrName = attribute.getNodeName();

            if( attrName.equalsIgnoreCase("Path") && attribute.hasChildNodes() ) {
                user.setPath(attribute.getFirstChild().getNodeValue().trim());
            }
            else if( attrName.equalsIgnoreCase("UserId") && attribute.hasChildNodes() ) {
                user.setProviderUserId(attribute.getFirstChild().getNodeValue().trim());
            }
            else if( attrName.equalsIgnoreCase("UserName") && attribute.hasChildNodes() ) {
                user.setUserName(attribute.getFirstChild().getNodeValue().trim());
            }
        }
        if( user.getUserName() == null || user.getProviderUserId() == null ) {
            return null;
        }
        return user;
    }
    
    private @Nonnull String validateName(@Nonnull String name) {
        // It must contain only alphanumeric characters and/or the following: +=,.@_-
        StringBuilder str = new StringBuilder();
        
        for( int i=0; i< name.length(); i++ ) {
            char c = name.charAt(i);
            
            if( Character.isLetterOrDigit(c) ) {
                str.append(c);
            }
            else if( c == '+' || c == '=' || c == ',' || c == '.' || c == '@' || c == '_' || c == '-' ) {
                if( i == 0 ) {
                    str.append("a");
                }
                str.append(c);
            }
            else if( c == ' ' ) {
                str.append("-");
            }
        }
        if( str.length() < 1 ) {
            return String.valueOf(System.currentTimeMillis());
        }
        return str.toString();
    }
}
