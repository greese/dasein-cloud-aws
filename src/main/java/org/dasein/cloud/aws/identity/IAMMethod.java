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

package org.dasein.cloud.aws.identity;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.compute.EC2Method;
import org.dasein.cloud.identity.IdentityAndAccessSupport;
import org.dasein.cloud.identity.ServiceAction;

import javax.annotation.Nonnull;
import java.util.Map;

public class IAMMethod extends EC2Method {
    static public final String IAM_PREFIX = "iam:";
    static public final String IAM_URL    = "https://iam.amazonaws.com";
    static public final String VERSION    = "2010-05-08";

    static public final String ADD_USER_TO_GROUP      = "AddUserToGroup";
    static public final String CREATE_ACCESS_KEY      = "CreateAccessKey";
    static public final String CREATE_GROUP           = "CreateGroup";
    static public final String CREATE_LOGIN_PROFILE   = "CreateLoginProfile";
    static public final String CREATE_USER            = "CreateUser";
    static public final String DELETE_ACCESS_KEY      = "DeleteAccessKey";
    static public final String DELETE_GROUP           = "DeleteGroup";
    static public final String DELETE_GROUP_POLICY    = "DeleteGroupPolicy";
    static public final String DELETE_LOGIN_PROFILE   = "DeleteLoginProfile";
    static public final String DELETE_USER            = "DeleteUser";
    static public final String GET_ACCESS_KEY         = "GetAccessKey";
    static public final String GET_GROUP              = "GetGroup";
    static public final String GET_GROUP_POLICY       = "GetGroupPolicy";
    static public final String GET_USER               = "GetUser";
    static public final String GET_USER_POLICY        = "GetPolicy";
    static public final String LIST_ACCESS_KEY        = "ListAccessKey";
    static public final String LIST_GROUP_POLICIES    = "ListGroupPolicies";
    static public final String LIST_GROUPS            = "ListGroups";
    static public final String LIST_GROUPS_FOR_USER   = "ListGroupsForUser";
    static public final String LIST_USER_POLICIES     = "ListUserPolicies";
    static public final String LIST_USERS             = "ListUsers";
    static public final String PUT_GROUP_POLICY       = "PutGroupPolicy";
    static public final String PUT_USER_POLICY        = "PutUserPolicy";
    static public final String DELETE_USER_POLICY     = "DeleteUserPolicy";
    static public final String REMOVE_USER_FROM_GROUP = "RemoveUserFromGroup";
    static public final String UPDATE_GROUP           = "UpdateGroup";
    static public final String UPDATE_USER            = "UpdateUser";

    static public @Nonnull ServiceAction[] asIAMServiceAction(@Nonnull String action) {
        if( action.equals(ADD_USER_TO_GROUP) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.JOIN_GROUP };
        }
        else if( action.equals(CREATE_ACCESS_KEY) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.ENABLE_API };
        }
        else if( action.equals(CREATE_GROUP) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.CREATE_GROUP };
        }
        else if( action.equals(CREATE_LOGIN_PROFILE) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.ENABLE_CONSOLE };
        }
        else if( action.equals(CREATE_USER) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.CREATE_USER };
        }
        else if( action.equals(DELETE_ACCESS_KEY) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.DISABLE_API };
        }
        else if( action.equals(DELETE_GROUP) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.REMOVE_GROUP };
        }
        else if( action.equals(DELETE_LOGIN_PROFILE) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.DISABLE_CONSOLE };
        }
        else if( action.equals(DELETE_USER) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.REMOVE_USER };
        }
        else if( action.equals(GET_ACCESS_KEY) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.GET_ACCESS_KEY };
        }
        /*
    static public final String PUT_GROUP_POLICY       = "PutGroupPolicy";
    static public final String PUT_USER_POLICY        = "PutUserPolicy";
    static public final String REMOVE_USER_FROM_GROUP = "RemoveUserFromGroup";
    static public final String UPDATE_GROUP           = "UpdateGroup";
    static public final String UPDATE_USER            = "UpdateUser";
         */
        else if( action.equals(GET_GROUP) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.GET_GROUP };
        }
        else if( action.equals(GET_GROUP_POLICY) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.GET_GROUP_POLICY };
        }
        else if( action.equals(GET_USER) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.GET_USER };
        }
        else if( action.equals(GET_USER_POLICY) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.GET_USER_POLICY };
        }
        else if( action.equals(LIST_ACCESS_KEY) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.LIST_ACCESS_KEY };
        }
        else if( action.equals(LIST_GROUP_POLICIES) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.GET_GROUP_POLICY };            
        }
        else if( action.equals(LIST_GROUPS) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.LIST_GROUP };
        }
        else if( action.equals(LIST_GROUPS_FOR_USER) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.GET_USER };
        }
        else if( action.equals(LIST_USER_POLICIES) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.GET_USER_POLICY };
        }
        else if( action.equals(LIST_USERS) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.LIST_USER };
        }
        else if( action.equals(PUT_GROUP_POLICY) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.ADD_GROUP_ACCESS };
        }
        else if( action.equals(PUT_USER_POLICY) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.ADD_USER_ACCESS };
        }
        else if( action.equals(REMOVE_USER_FROM_GROUP) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.DROP_FROM_GROUP };
        }
        else if( action.equals(UPDATE_GROUP) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.UPDATE_GROUP };
        }
        else if( action.equals(UPDATE_USER) ) {
            return new ServiceAction[] { IdentityAndAccessSupport.UPDATE_USER };
        }
        return new ServiceAction[0];
    }

    public IAMMethod(AWSCloud provider, Map<String,String> parameters) throws CloudException, InternalException {
        super(provider, IAM_URL, parameters);
    }

}