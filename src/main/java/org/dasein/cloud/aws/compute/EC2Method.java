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

package org.dasein.cloud.aws.compute;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.admin.PrepaymentSupport;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.compute.*;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.identity.ShellKeySupport;
import org.dasein.cloud.network.*;
import org.dasein.cloud.platform.MonitoringSupport;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.XMLParser;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EC2Method {
    static private final Logger logger = AWSCloud.getLogger(EC2Method.class);
    static private final Logger wire = AWSCloud.getWireLogger(EC2Method.class);

    static public final String AUTOSCALING_PREFIX = "autoscaling:";

    // Auto-scaling operations
    static public final String CREATE_AUTO_SCALING_GROUP        = "CreateAutoScalingGroup";
    static public final String CREATE_LAUNCH_CONFIGURATION      = "CreateLaunchConfiguration";
    static public final String CREATE_OR_UPDATE_SCALING_TRIGGER = "CreateOrUpdateScalingTrigger";
    static public final String DELETE_AUTO_SCALING_GROUP        = "DeleteAutoScalingGroup";
    static public final String DELETE_LAUNCH_CONFIGURATION      = "DeleteLaunchConfiguration";
    static public final String DELETE_SCALING_POLICY            = "DeletePolicy";
    static public final String DESCRIBE_AUTO_SCALING_GROUPS     = "DescribeAutoScalingGroups";
    static public final String SUSPEND_AUTO_SCALING_GROUP       = "SuspendProcesses";
    static public final String RESUME_AUTO_SCALING_GROUP        = "ResumeProcesses";
    static public final String PUT_SCALING_POLICY               = "PutScalingPolicy";
    static public final String DESCRIBE_SCALING_POLICIES        = "DescribePolicies";
    static public final String DESCRIBE_LAUNCH_CONFIGURATIONS   = "DescribeLaunchConfigurations";
    static public final String SET_DESIRED_CAPACITY             = "SetDesiredCapacity";
    static public final String UPDATE_AUTO_SCALING_GROUP        = "UpdateAutoScalingGroup";
    static public final String UPDATE_AUTO_SCALING_GROUP_TAGS   = "CreateOrUpdateTags";
    static public final String DELETE_AUTO_SCALING_GROUP_TAGS   = "DeleteTags";

    static public @Nonnull ServiceAction[] asAutoScalingServiceAction(@Nonnull String action) {
        if( action.equals(CREATE_AUTO_SCALING_GROUP) ) {
            return new ServiceAction[] { AutoScalingSupport.CREATE_SCALING_GROUP };
        }
        else if( action.equals(CREATE_LAUNCH_CONFIGURATION) ) {
            return new ServiceAction[] { AutoScalingSupport.CREATE_LAUNCH_CONFIGURATION };
        }
        else if( action.equals(CREATE_OR_UPDATE_SCALING_TRIGGER) ) {
            return new ServiceAction[] { AutoScalingSupport.SET_SCALING_TRIGGER };
        }
        else if( action.equals(DELETE_AUTO_SCALING_GROUP) ) {
            return new ServiceAction[] { AutoScalingSupport.REMOVE_SCALING_GROUP };
        }
        else if( action.equals(DELETE_LAUNCH_CONFIGURATION) ) {
            return new ServiceAction[] { AutoScalingSupport.REMOVE_LAUNCH_CONFIGURATION };
        }
        else if( action.equals(DESCRIBE_AUTO_SCALING_GROUPS) ) {
            return new ServiceAction[] { AutoScalingSupport.GET_SCALING_GROUP, AutoScalingSupport.LIST_SCALING_GROUP };
        }
        else if( action.equals(DESCRIBE_LAUNCH_CONFIGURATIONS) ) {
            return new ServiceAction[] { AutoScalingSupport.GET_LAUNCH_CONFIGURATION, AutoScalingSupport.LIST_LAUNCH_CONFIGURATION };
        }
        else if( action.equals(SET_DESIRED_CAPACITY) ) {
            return new ServiceAction[] { AutoScalingSupport.SET_CAPACITY };
        }
        else if( action.equals(UPDATE_AUTO_SCALING_GROUP) ) {
            return new ServiceAction[] { AutoScalingSupport.UPDATE_SCALING_GROUP };
        }
        else if( action.equals(SUSPEND_AUTO_SCALING_GROUP) ) {
          return new ServiceAction[] { AutoScalingSupport.SUSPEND_AUTO_SCALING_GROUP };
        }
        else if( action.equals(RESUME_AUTO_SCALING_GROUP) ) {
          return new ServiceAction[] { AutoScalingSupport.RESUME_AUTO_SCALING_GROUP };
        }
        else if( action.equals(PUT_SCALING_POLICY) ) {
          return new ServiceAction[] { AutoScalingSupport.PUT_SCALING_POLICY };
        }
        else if( action.equals(DELETE_SCALING_POLICY) ) {
          return new ServiceAction[] { AutoScalingSupport.DELETE_SCALING_POLICY };
        }
        else if( action.equals(DESCRIBE_SCALING_POLICIES) ) {
          return new ServiceAction[] { AutoScalingSupport.LIST_SCALING_POLICIES };
        }
        return new ServiceAction[0];
    }

    static public final String EC2_PREFIX = "ec2:";
    static public final String RDS_PREFIX = "rds:";
    static public final String SDB_PREFIX = "sdb:";
    static public final String SNS_PREFIX = "sns:";
    static public final String SQS_PREFIX = "sqs:";
    static public final String CW_PREFIX  = "cloudwatch:";

    // AMI operations
    static public final String BUNDLE_INSTANCE          = "BundleInstance";
    static public final String CREATE_IMAGE             = "CreateImage";
    static public final String DESCRIBE_BUNDLE_TASKS    = "DescribeBundleTasks";
    static public final String DEREGISTER_IMAGE         = "DeregisterImage";
    static public final String DESCRIBE_IMAGE_ATTRIBUTE = "DescribeImageAttribute";
    static public final String DESCRIBE_IMAGES          = "DescribeImages";
    static public final String MODIFY_IMAGE_ATTRIBUTE   = "ModifyImageAttribute";
    static public final String REGISTER_IMAGE           = "RegisterImage";

    // EBS operations
    static public final String ATTACH_VOLUME    = "AttachVolume";
    static public final String CREATE_VOLUME    = "CreateVolume";
    static public final String DELETE_VOLUME    = "DeleteVolume";
    static public final String DETACH_VOLUME    = "DetachVolume";
    static public final String DESCRIBE_VOLUMES = "DescribeVolumes";

    // Elastic IP operations
    static public final String ALLOCATE_ADDRESS     = "AllocateAddress";
    static public final String ASSOCIATE_ADDRESS    = "AssociateAddress";
    static public final String DESCRIBE_ADDRESSES   = "DescribeAddresses";
    static public final String DISASSOCIATE_ADDRESS = "DisassociateAddress";
    static public final String RELEASE_ADDRESS      = "ReleaseAddress";

    // Instance operations
    static public final String DESCRIBE_INSTANCES          = "DescribeInstances";
    static public final String GET_CONSOLE_OUTPUT          = "GetConsoleOutput";
    static public final String GET_METRIC_STATISTICS       = "GetMetricStatistics";
    static public final String GET_PASSWORD_DATA           = "GetPasswordData";
    static public final String MONITOR_INSTANCES           = "MonitorInstances";
    static public final String REBOOT_INSTANCES            = "RebootInstances";
    static public final String RUN_INSTANCES               = "RunInstances";
    static public final String START_INSTANCES             = "StartInstances";
    static public final String STOP_INSTANCES              = "StopInstances";
    static public final String TERMINATE_INSTANCES         = "TerminateInstances";
    static public final String UNMONITOR_INSTANCES         = "UnmonitorInstances";
    static public final String MODIFY_INSTANCE_ATTRIBUTE   = "ModifyInstanceAttribute";
	static public final String DESCRIBE_INSTANCE_ATTRIBUTE = "DescribeInstanceAttribute";
    static public final String DESCRIBE_INSTANCE_STATUS    = "DescribeInstanceStatus";

    // Keypair operations
    static public final String CREATE_KEY_PAIR    = "CreateKeyPair";
    static public final String DELETE_KEY_PAIR    = "DeleteKeyPair";
    static public final String DESCRIBE_KEY_PAIRS = "DescribeKeyPairs";
    static public final String IMPORT_KEY_PAIR    = "ImportKeyPair";

    // Reserved instances operations
    static public final String DESCRIBE_RESERVED_INSTANCES           = "DescribeReservedInstances";
    static public final String DESCRIBE_RESERVED_INSTANCES_OFFERINGS = "DescribeReservedInstancesOfferings";
    static public final String PURCHASE_RESERVED_INSTANCES_OFFERING  = "PurchaseReservedInstancesOffering";

    // Security group operations
    static public final String AUTHORIZE_SECURITY_GROUP_INGRESS = "AuthorizeSecurityGroupIngress";
    static public final String AUTHORIZE_SECURITY_GROUP_EGRESS  = "AuthorizeSecurityGroupEgress";
    static public final String CREATE_SECURITY_GROUP            = "CreateSecurityGroup";
    static public final String DELETE_SECURITY_GROUP            = "DeleteSecurityGroup";
    static public final String DESCRIBE_SECURITY_GROUPS         = "DescribeSecurityGroups";
    static public final String REVOKE_SECURITY_GROUP_EGRESS     = "RevokeSecurityGroupEgress";
    static public final String REVOKE_SECURITY_GROUP_INGRESS    = "RevokeSecurityGroupIngress";

    // Snapshot operations
    static public final String COPY_SNAPSHOT               = "CopySnapshot";
    static public final String CREATE_SNAPSHOT             = "CreateSnapshot";
    static public final String DELETE_SNAPSHOT             = "DeleteSnapshot";
    static public final String DESCRIBE_SNAPSHOTS          = "DescribeSnapshots";
    static public final String DESCRIBE_SNAPSHOT_ATTRIBUTE = "DescribeSnapshotAttribute";
    static public final String MODIFY_SNAPSHOT_ATTRIBUTE   = "ModifySnapshotAttribute";

    // VPC operations
    static public final String ASSOCIATE_DHCP_OPTIONS  = "AssociateDhcpOptions";
    static public final String ASSOCIATE_ROUTE_TABLE   = "AssociateRouteTable";
    static public final String ATTACH_INTERNET_GATEWAY = "AttachInternetGateway";
    static public final String CREATE_DHCP_OPTIONS     = "CreateDhcpOptions";
    static public final String CREATE_INTERNET_GATEWAY = "CreateInternetGateway";
    static public final String CREATE_ROUTE            = "CreateRoute";
    static public final String CREATE_ROUTE_TABLE      = "CreateRouteTable";
    static public final String CREATE_SUBNET           = "CreateSubnet";
    static public final String CREATE_VPC              = "CreateVpc";
    static public final String DELETE_INTERNET_GATEWAY = "DeleteInternetGateway";
    static public final String DELETE_SUBNET           = "DeleteSubnet";
    static public final String DELETE_VPC              = "DeleteVpc";
    static public final String DESCRIBE_DHCP_OPTIONS   = "DescribeDhcpOptions";
    static public final String DESCRIBE_INTERNET_GATEWAYS = "DescribeInternetGateways";
    static public final String DELETE_ROUTE            = "DeleteRoute";
    static public final String DELETE_ROUTE_TABLE      = "DeleteRouteTable";
    static public final String DESCRIBE_ROUTE_TABLES   = "DescribeRouteTables";
    static public final String DESCRIBE_SUBNETS        = "DescribeSubnets";
    static public final String DESCRIBE_VPCS           = "DescribeVpcs";
    static public final String DETACH_INTERNET_GATEWAY = "DetachInternetGateway";
    static public final String DISASSOCIATE_ROUTE_TABLE = "DisassociateRouteTable";
    static public final String REPLACE_ROUTE_TABLE_ASSOCIATION = "ReplaceRouteTableAssociation";

    // network ACL operations
    static public final String CREATE_NETWORK_ACL        = "CreateNetworkAcl";
    static public final String DESCRIBE_NETWORK_ACLS     = "DescribeNetworkAcls";
    static public final String DELETE_NETWORK_ACL        = "DeleteNetworkAcl";
    static public final String CREATE_NETWORK_ACL_ENTRY  = "CreateNetworkAclEntry";
    static public final String DELETE_NETWORK_ACL_ENTRY  = "DeleteNetworkAclEntry";
    static public final String REPLACE_NETWORK_ACL_ENTRY = "ReplaceNetworkAclEntry";
    static public final String REPLACE_NETWORK_ACL_ASSOC = "ReplaceNetworkAclAssociation";

    // network interface operations
    static public final String ATTACH_NIC             = "AttachNetworkInterface";
    static public final String CREATE_NIC             = "CreateNetworkInterface";
    static public final String DELETE_NIC             = "DeleteNetworkInterface";
    static public final String DETACH_NIC             = "DetachNetworkInterface";
    static public final String DESCRIBE_NICS          = "DescribeNetworkInterfaces";

    // VPN operations
    static public final String ATTACH_VPN_GATEWAY          = "AttachVpnGateway";
    static public final String CREATE_CUSTOMER_GATEWAY     = "CreateCustomerGateway";
    static public final String CREATE_VPN_CONNECTION       = "CreateVpnConnection";
    static public final String CREATE_VPN_GATEWAY          = "CreateVpnGateway";
    static public final String DELETE_CUSTOMER_GATEWAY     = "DeleteCustomerGateway";
    static public final String DELETE_VPN_GATEWAY          = "DeleteVpnGateway";
    static public final String DELETE_VPN_CONNECTION       = "DeleteVpnConnection";
    static public final String DESCRIBE_CUSTOMER_GATEWAYS  = "DescribeCustomerCateways";
    static public final String DESCRIBE_VPN_CONNECTIONS    = "DescribeVpnConnections";
    static public final String DESCRIBE_VPN_GATEWAYS       = "DescribeVpnGateways";
    static public final String DETACH_VPN_GATEWAY          = "DetachVpnGateway";

    // CloudWatch operations
    static public final String LIST_METRICS = "ListMetrics";
    static public final String DESCRIBE_ALARMS = "DescribeAlarms";
    static public final String PUT_METRIC_ALARM = "PutMetricAlarm";
    static public final String DELETE_ALARMS = "DeleteAlarms";
    static public final String ENABLE_ALARM_ACTIONS = "EnableAlarmActions";
    static public final String DISABLE_ALARM_ACTIONS = "DisableAlarmActions";

    // Account operations
    static public final String DESCRIBE_ACCOUNT_ATTRIBUTES      = "DescribeAccountAttributes";


    static public @Nonnull ServiceAction[] asEC2ServiceAction(@Nonnull String action) {
        // TODO: implement me
        // AMI operations
        if( action.equals(BUNDLE_INSTANCE) ) {
            return new ServiceAction[] { MachineImageSupport.IMAGE_VM };
        }
        else if( action.equals(CREATE_IMAGE) || action.equals(REGISTER_IMAGE) ) {
            return new ServiceAction[] { MachineImageSupport.REGISTER_IMAGE };
        }
        else if( action.equals(DESCRIBE_BUNDLE_TASKS) ) {
            return new ServiceAction[0];
        }
        else if( action.equals(DEREGISTER_IMAGE) ) {
            return new ServiceAction[] { MachineImageSupport.REMOVE_IMAGE };
        }
        else if( action.equals(DESCRIBE_IMAGE_ATTRIBUTE) || action.equals(DESCRIBE_IMAGES) ) {
            return new ServiceAction[] { MachineImageSupport.GET_IMAGE, MachineImageSupport.LIST_IMAGE };
        }
        else if( action.equals(MODIFY_IMAGE_ATTRIBUTE) ) {
            return new ServiceAction[] { MachineImageSupport.MAKE_PUBLIC, MachineImageSupport.SHARE_IMAGE };
        }
        // EBS operations
        if( action.equals(ATTACH_VOLUME) ) {
            return new ServiceAction[] { VolumeSupport.ATTACH };
        }
        else if( action.equals(CREATE_VOLUME) ) {
            return new ServiceAction[] { VolumeSupport.CREATE_VOLUME };
        }
        else if( action.equals(DELETE_VOLUME) ) {
            return new ServiceAction[] { VolumeSupport.REMOVE_VOLUME };
        }
        else if( action.equals(DETACH_VOLUME) ) {
            return new ServiceAction[] { VolumeSupport.DETACH };
        }
        else if( action.equals(DESCRIBE_VOLUMES) ) {
            return new ServiceAction[] { VolumeSupport.GET_VOLUME, VolumeSupport.LIST_VOLUME };
        }
        // elastic IP operations
        if( action.equals(ALLOCATE_ADDRESS) ) {
            return new ServiceAction[] { IpAddressSupport.CREATE_IP_ADDRESS };
        }
        else if( action.equals(ASSOCIATE_ADDRESS) ) {
            return new ServiceAction[] { IpAddressSupport.ASSIGN };
        }
        else if( action.equals(DESCRIBE_ADDRESSES) ) {
            return new ServiceAction[] { IpAddressSupport.GET_IP_ADDRESS, IpAddressSupport.LIST_IP_ADDRESS };
        }
        else if( action.equals(DISASSOCIATE_ADDRESS) ) {
            return new ServiceAction[] { IpAddressSupport.RELEASE };
        }
        else if( action.equals(RELEASE_ADDRESS) ) {
            return new ServiceAction[] { IpAddressSupport.REMOVE_IP_ADDRESS };
        }
        // instance operations
        if( action.equals(DESCRIBE_INSTANCES) ) {
            return new ServiceAction[] { VirtualMachineSupport.GET_VM, VirtualMachineSupport.LIST_VM };
        }
        else if( action.equals(GET_CONSOLE_OUTPUT) ) {
            return new ServiceAction[] { VirtualMachineSupport.VIEW_CONSOLE };
        }
        else if( action.equals(GET_METRIC_STATISTICS) ) {
            return new ServiceAction[] { VirtualMachineSupport.VIEW_ANALYTICS };
        }
        else if( action.equals(GET_PASSWORD_DATA) ) {
            return new ServiceAction[] { VirtualMachineSupport.GET_VM };
        }
        else if( action.equals(MONITOR_INSTANCES) || action.equals(UNMONITOR_INSTANCES) ) {
            return new ServiceAction[] { VirtualMachineSupport.TOGGLE_ANALYTICS };
        }
        else if( action.equals(REBOOT_INSTANCES) ) {
            return new ServiceAction[] { VirtualMachineSupport.REBOOT };
        }
        else if( action.equals(RUN_INSTANCES) ) {
            return new ServiceAction[] { VirtualMachineSupport.CREATE_VM };
        }
        else if( action.equals(START_INSTANCES) ) {
            return new ServiceAction[] { VirtualMachineSupport.BOOT };
        }
        else if( action.equals(STOP_INSTANCES) ) {
            return new ServiceAction[] { VirtualMachineSupport.PAUSE };
        }
        else if( action.equals(TERMINATE_INSTANCES) ) {
            return new ServiceAction[] { VirtualMachineSupport.REMOVE_VM };
        }
        // keypair operations
        if( action.equals(CREATE_KEY_PAIR) ) {
            return new ServiceAction[] { ShellKeySupport.CREATE_KEYPAIR };
        }
        else if( action.equals(DELETE_KEY_PAIR) ) {
            return new ServiceAction[] { ShellKeySupport.REMOVE_KEYPAIR };
        }
        else if( action.equals(IMPORT_KEY_PAIR) ) {
            return new ServiceAction[] { ShellKeySupport.CREATE_KEYPAIR };
        }
        else if( action.equals(DESCRIBE_KEY_PAIRS) ) {
            return new ServiceAction[] { ShellKeySupport.GET_KEYPAIR, ShellKeySupport.LIST_KEYPAIR };
        }
        // reserved instance operations
        if( action.equals(DESCRIBE_RESERVED_INSTANCES) ) {
            return new ServiceAction[] { PrepaymentSupport.GET_PREPAYMENT, PrepaymentSupport.LIST_PREPAYMENT };
        }
        else if( action.equals(DESCRIBE_RESERVED_INSTANCES_OFFERINGS) ) {
            return new ServiceAction[] { PrepaymentSupport.GET_OFFERING, PrepaymentSupport.LIST_OFFERING };
        }
        else if( action.equals(PURCHASE_RESERVED_INSTANCES_OFFERING) ) {
            return new ServiceAction[] { PrepaymentSupport.PREPAY };
        }
        // security group operations
        if( action.equals(AUTHORIZE_SECURITY_GROUP_INGRESS) ) {
            return new ServiceAction[] { FirewallSupport.AUTHORIZE };
        }
        else if( action.equals(AUTHORIZE_SECURITY_GROUP_EGRESS) ) {
            return new ServiceAction[] { FirewallSupport.AUTHORIZE };
        }
        else if( action.equals(CREATE_SECURITY_GROUP) ) {
            return new ServiceAction[] { FirewallSupport.CREATE_FIREWALL };
        }
        else if( action.equals(DELETE_SECURITY_GROUP) ) {
            return new ServiceAction[] { FirewallSupport.REMOVE_FIREWALL };
        }
        else if( action.equals(DESCRIBE_SECURITY_GROUPS) ) {
            return new ServiceAction[] { FirewallSupport.GET_FIREWALL, FirewallSupport.LIST_FIREWALL };
        }
        else if( action.equals(REVOKE_SECURITY_GROUP_INGRESS) ) {
            return new ServiceAction[] { FirewallSupport.REVOKE };
        }
        else if( action.equals(REVOKE_SECURITY_GROUP_EGRESS) ) {
            return new ServiceAction[] { FirewallSupport.REVOKE };
        }

        // network ACL operations
        if( action.equals(CREATE_NETWORK_ACL_ENTRY) || action.equals(REPLACE_NETWORK_ACL_ENTRY) ) {
            return new ServiceAction[] { NetworkFirewallSupport.AUTHORIZE };
        }
        else if( action.equals(REPLACE_NETWORK_ACL_ASSOC) ) {
            return new ServiceAction[] { NetworkFirewallSupport.ASSOCIATE };
        }
        else if( action.equals(CREATE_NETWORK_ACL) ) {
            return new ServiceAction[] { NetworkFirewallSupport.CREATE_FIREWALL };
        }
        else if( action.equals(DELETE_NETWORK_ACL) ) {
            return new ServiceAction[] { NetworkFirewallSupport.REMOVE_FIREWALL };
        }
        else if( action.equals(DESCRIBE_NETWORK_ACLS) ) {
            return new ServiceAction[] { NetworkFirewallSupport.GET_FIREWALL, NetworkFirewallSupport.LIST_FIREWALL };
        }
        else if( action.equals(DELETE_NETWORK_ACL_ENTRY) ) {
            return new ServiceAction[] { NetworkFirewallSupport.REVOKE };
        }

        // snapshot operations
        if( action.equals(COPY_SNAPSHOT) ) {
            return new ServiceAction[] { SnapshotSupport.CREATE_SNAPSHOT };
        }
        else if( action.equals(CREATE_SNAPSHOT) ) {
            return new ServiceAction[] { SnapshotSupport.CREATE_SNAPSHOT };
        }
        else if( action.equals(DELETE_SNAPSHOT) ) {
            return new ServiceAction[] { SnapshotSupport.REMOVE_SNAPSHOT };
        }
        else if( action.equals(DESCRIBE_SNAPSHOTS) ) {
            return new ServiceAction[] { SnapshotSupport.GET_SNAPSHOT, SnapshotSupport.LIST_SNAPSHOT };
        }
        else if( action.equals(DESCRIBE_SNAPSHOT_ATTRIBUTE) ) {
            return new ServiceAction[] { SnapshotSupport.GET_SNAPSHOT };
        }
        else if( action.equals(MODIFY_SNAPSHOT_ATTRIBUTE) ) {
            return new ServiceAction[] { SnapshotSupport.MAKE_PUBLIC, SnapshotSupport.SHARE_SNAPSHOT };
        }
        // VPC operations
        if( action.equals(ASSOCIATE_DHCP_OPTIONS) ) {
            return new ServiceAction[0];
        }
        else if( action.equals(ASSOCIATE_ROUTE_TABLE) ) {
            return new ServiceAction[] { VLANSupport.ASSIGN_ROUTE_TO_SUBNET };
        }
        else if( action.equals(CREATE_DHCP_OPTIONS) ) {
            return new ServiceAction[0];
        }
        else if( action.equals(CREATE_ROUTE_TABLE) ) {
            return new ServiceAction[] { VLANSupport.CREATE_ROUTING_TABLE };
        }
        else if( action.equals(CREATE_ROUTE) ) {
            return new ServiceAction[] { VLANSupport.ADD_ROUTE };
        }
        else if( action.equals(CREATE_SUBNET) ) {
            return new ServiceAction[] { VLANSupport.CREATE_SUBNET };
        }
        else if( action.equals(CREATE_VPC) ) {
            return new ServiceAction[] { VLANSupport.CREATE_VLAN};
        }
        else if( action.equals(DELETE_INTERNET_GATEWAY) ) {
            return new ServiceAction[] { VLANSupport.REMOVE_INTERNET_GATEWAY};
        }
        else if( action.equals(DELETE_ROUTE) ) {
            return new ServiceAction[] { VLANSupport.REMOVE_ROUTE };
        }
        else if( action.equals(DELETE_ROUTE_TABLE) ) {
            return new ServiceAction[] { VLANSupport.REMOVE_ROUTING_TABLE };
        }
        else if( action.equals(DELETE_SUBNET) ) {
            return new ServiceAction[] { VLANSupport.REMOVE_SUBNET };
        }
        else if( action.equals(DELETE_VPC) ) {
            return new ServiceAction[] { VLANSupport.REMOVE_VLAN };
        }
        else if( action.equals(DESCRIBE_DHCP_OPTIONS) ) {
            return new ServiceAction[0];
        }
        else if( action.equalsIgnoreCase(DESCRIBE_ROUTE_TABLES) ) {
            return new ServiceAction[] { VLANSupport.GET_ROUTING_TABLE, VLANSupport.LIST_ROUTING_TABLE };
        }
        else if( action.equals(DESCRIBE_SUBNETS) ) {
            return new ServiceAction[] { VLANSupport.GET_SUBNET, VLANSupport.LIST_SUBNET };
        }
        else if( action.equals(DESCRIBE_VPCS) ) {
            return new ServiceAction[] { VLANSupport.GET_VLAN, VLANSupport.LIST_VLAN };
        }
        else if( action.equals(CREATE_INTERNET_GATEWAY) ) {
            return new ServiceAction[] { VLANSupport.CREATE_VLAN };
        }
        else if( action.equals(ATTACH_INTERNET_GATEWAY) ) {
            return new ServiceAction[] { VLANSupport.CREATE_VLAN };
        }

        // NIC operations
        if( action.equals(CREATE_NIC) ) {
            return new ServiceAction[] { VLANSupport.CREATE_NIC };
        }
        else if( action.equals(ATTACH_NIC) ) {
            return new ServiceAction[] { VLANSupport.ATTACH_NIC };
        }
        else if( action.equals(DETACH_NIC) ) {
            return new ServiceAction[] { VLANSupport.DETACH_NIC };
        }
        else if( action.equals(DELETE_NIC) ) {
            return new ServiceAction[] { VLANSupport.REMOVE_NIC };
        }
        else if( action.equals(DESCRIBE_NICS) ) {
            return new ServiceAction[] { VLANSupport.GET_NIC, VLANSupport.LIST_NIC };
        }
        // VPN operations
        if( action.equals(CREATE_CUSTOMER_GATEWAY) ) {
            return new ServiceAction[] {VPNSupport.CREATE_GATEWAY };
        }
        else if( action.equals(ATTACH_VPN_GATEWAY) ) {
            return new ServiceAction[] { VPNSupport.ATTACH };
        }
        else if( action.equals(CREATE_VPN_GATEWAY) ) {
            return new ServiceAction[] {VPNSupport.CREATE_VPN };
        }
        else if( action.equals(DELETE_CUSTOMER_GATEWAY) ) {
            return new ServiceAction[] { VPNSupport.REMOVE_GATEWAY };
        }
        else if( action.equals(DELETE_VPN_GATEWAY) ) {
            return new ServiceAction[] { VPNSupport.REMOVE_VPN };
        }
        else if( action.equals(DESCRIBE_CUSTOMER_GATEWAYS) ) {
            return new ServiceAction[] { VPNSupport.LIST_GATEWAY, VPNSupport.GET_GATEWAY };
        }
        else if( action.equals(DESCRIBE_VPN_CONNECTIONS) ) {
            return new ServiceAction[] { VPNSupport.LIST_GATEWAY, VPNSupport.GET_GATEWAY, VPNSupport.LIST_VPN, VPNSupport.GET_VPN  };
        }
        else if( action.equals(DESCRIBE_VPN_GATEWAYS) ) {
            return new ServiceAction[] { VPNSupport.LIST_VPN, VPNSupport.GET_VPN };
        }
        else if( action.equals(CREATE_VPN_CONNECTION) ) {
            return new ServiceAction[] { VPNSupport.CONNECT_GATEWAY };
        }
        else if( action.equals(DELETE_VPN_CONNECTION) ) {
            return new ServiceAction[] { VPNSupport.DISCONNECT_GATEWAY };
        }
        else if( action.equals(DETACH_INTERNET_GATEWAY) ) {
            return new ServiceAction[] { VPNSupport.REMOVE_GATEWAY };
        }
        else if( action.equals(DETACH_VPN_GATEWAY) ) {
            return new ServiceAction[] { VPNSupport.DETACH };
        }

        // CloudWatch operations
        if( action.equals(LIST_METRICS) ) {
          return new ServiceAction[] {MonitoringSupport.LIST_METRICS};
        }
        else if ( action.equals( DESCRIBE_ALARMS ) ) {
          return new ServiceAction[] {MonitoringSupport.DESCRIBE_ALARMS};
        }
        else if ( action.equals( PUT_METRIC_ALARM ) ) {
          return new ServiceAction[] {MonitoringSupport.UPDATE_ALARM};
        }
        else if ( action.equals( DELETE_ALARMS ) ) {
          return new ServiceAction[] {MonitoringSupport.REMOVE_ALARMS};
        }
        else if ( action.equals( ENABLE_ALARM_ACTIONS ) ) {
          return new ServiceAction[] {MonitoringSupport.ENABLE_ALARM_ACTIONS};
        }
        else if ( action.equals( DISABLE_ALARM_ACTIONS ) ) {
          return new ServiceAction[] {MonitoringSupport.DISABLE_ALARM_ACTIONS};
        }

        return new ServiceAction[0];
    }

	private int                attempts    = 0;
	private Map<String,String> parameters  = null;
	private AWSCloud           provider    = null;
	private String             url         = null;

	public EC2Method(AWSCloud provider, String url, Map<String,String> parameters) throws InternalException, CloudException {
		this.url = url;
		this.parameters = parameters;
		this.provider = provider;
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("Provider context is necessary for this request");
        }
		parameters.put(AWSCloud.P_SIGNATURE, provider.signEc2(ctx.getAccessPrivate(), url, parameters));
	}

    public void checkSuccess(NodeList returnNodes) throws CloudException {
        if( returnNodes.getLength() > 0 ) {
            if( !returnNodes.item(0).getFirstChild().getNodeValue().equalsIgnoreCase("true") ) {
                throw new CloudException("Failed to revoke security group rule without explanation.");
            }
        }
    }

    public Document invoke() throws EC2Exception, CloudException, InternalException {
        return invoke(false);
    }

    public Document invoke(boolean debug) throws InternalException, CloudException, EC2Exception {
        return this.invoke(debug, null);
    }

    /**
     * The invoke method which isn't itself parsing the successful response,
     * but relies on the callback to parse it.
     *
     * @param callback
     * @throws InternalException
     * @throws CloudException
     * @throws EC2Exception
     */
    public void invoke(XmlStreamParser callback) throws InternalException, CloudException, EC2Exception {
        this.invoke(false, callback);
    }

    private Document invoke(boolean debug, XmlStreamParser callback) throws EC2Exception, CloudException, InternalException {
	    if( logger.isTraceEnabled() ) {
	        logger.trace("ENTER - " + EC2Method.class.getName() + ".invoke(" + debug + ")");
	    }
        if( wire.isDebugEnabled() ) {
            wire.debug("");
            wire.debug("--------------------------------------------------------------------------------------");
        }
        HttpClient client = null;
	    try {
    		if( logger.isDebugEnabled() ) {
    			logger.debug("Talking to server at " + url);
    		}

            HttpPost post = new HttpPost(url);
            client = provider.getClient();

            HttpResponse response;

            attempts++;
            post.addHeader("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");

            List<NameValuePair> params = new ArrayList<NameValuePair>();

            for( Map.Entry<String, String> entry : parameters.entrySet() ) {
                params.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }
            try {
                post.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
            }
            catch( UnsupportedEncodingException e ) {
                throw new InternalException(e);
            }
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getRequestLine().toString());
                for( Header header : post.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");

                try { wire.debug(EntityUtils.toString(post.getEntity())); }
                catch( IOException ignore ) { }

                wire.debug("");
            }
            try {
                APITrace.trace(provider, parameters.get(AWSCloud.P_ACTION));
                response = client.execute(post);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                }
            }
            catch( IOException e ) {
                logger.error("I/O error from server communications: " + e.getMessage());
                throw new InternalException(e);
            }
            int status = response.getStatusLine().getStatusCode();
            if( status == HttpServletResponse.SC_OK ) {
                try {
                    HttpEntity entity = response.getEntity();

                    if( entity == null ) {
                        throw EC2Exception.create(status);
                    }
                    InputStream input = entity.getContent();

                    try {
                        // When callback is passed, callback will parse the response, and therefore there
                        // will be no DOM document created. The callback will likely take a list to populate
                        // the results with.
                        if (callback != null) {
                            callback.parse(input);
                            return null;
                        }
                        else {
                            return parseResponse(input);
                        }
                    }
                    finally {
                        input.close();
                    }
                }
                catch( IOException e ) {
                    logger.error("Error parsing response from AWS: " + e.getMessage());
                    throw new CloudException(CloudErrorType.COMMUNICATION, status, null, e.getMessage());
                }
            }
            else if( status == HttpServletResponse.SC_FORBIDDEN ) {
                String msg = "API Access Denied (403)";

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity == null ) {
                        throw EC2Exception.create(status);
                    }
                    InputStream input = entity.getContent();

                    try {
                        BufferedReader in = new BufferedReader(new InputStreamReader(input));
                        StringBuilder sb = new StringBuilder();
                        String line;

                        while( (line = in.readLine()) != null ) {
                            sb.append(line);
                            sb.append("\n");
                        }
                        //System.out.println(sb);
                        try {
                            Document doc = parseResponse(sb.toString());

                            if( doc != null ) {
                                NodeList blocks = doc.getElementsByTagName("Error");
                                String code = null, message = null, requestId = null;

                                if( blocks.getLength() > 0 ) {
                                    Node error = blocks.item(0);
                                    NodeList attrs;

                                    attrs = error.getChildNodes();
                                    for( int i=0; i<attrs.getLength(); i++ ) {
                                        Node attr = attrs.item(i);

                                        if( attr.getNodeName().equals("Code") ) {
                                            code = attr.getFirstChild().getNodeValue().trim();
                                        }
                                        else if( attr.getNodeName().equals("Message") ) {
                                            message = attr.getFirstChild().getNodeValue().trim();
                                        }
                                    }

                                }
                                blocks = doc.getElementsByTagName("RequestID");
                                if( blocks.getLength() > 0 ) {
                                    Node id = blocks.item(0);

                                    requestId = id.getFirstChild().getNodeValue().trim();
                                }
                                if( message == null && code == null ) {
                                    throw new CloudException(CloudErrorType.COMMUNICATION, status, null, "Unable to identify error condition: " + status + "/" + requestId + "/null");
                                }
                                else if( message == null ) {
                                    message = code;
                                }
                                throw EC2Exception.create(status, requestId, code, message);
                            }
                        }
                        catch( RuntimeException ignore  ) {
                            // ignore me
                        }
                        catch( Error ignore  ) {
                            // ignore me
                        }
                        msg = msg + ": " + sb.toString().trim().replaceAll("\n", " / ");
                    }
                    finally {
                        input.close();
                    }
                }
                catch( IOException ignore ) {
                    // ignore me
                }
                catch( RuntimeException ignore ) {
                    // ignore me
                }
                catch( Error ignore ) {
                    // ignore me
                }
                throw new CloudException(msg);
            }
            else {
                if( logger.isDebugEnabled() ) {
                    logger.debug("Received " + status + " from " + parameters.get(AWSCloud.P_ACTION));
                }
                if( status == HttpServletResponse.SC_SERVICE_UNAVAILABLE || status == HttpServletResponse.SC_INTERNAL_SERVER_ERROR ) {
                    if( attempts >= 5 ) {
                        String msg;

                        if( status == HttpServletResponse.SC_SERVICE_UNAVAILABLE ) {
                            msg = "Cloud service is currently unavailable.";
                        }
                        else {
                            msg = "The cloud service encountered a server error while processing your request.";
                            try {
                                HttpEntity entity = response.getEntity();

                                if( entity == null ) {
                                    throw EC2Exception.create(status);
                                }
                                msg = msg + "Response from server was:\n" + EntityUtils.toString(entity);
                            }
                            catch( IOException ignore ) {
                                // ignore me
                            }
                            catch( RuntimeException ignore ) {
                                // ignore me
                            }
                            catch( Error ignore ) {
                                // ignore me
                            }
                        }
                        logger.error(msg);
                        throw new CloudException(msg);
                    }
                    else {
                        try { Thread.sleep(5000L); }
                        catch( InterruptedException e ) { /* ignore */ }
                        return invoke();
                    }
                }
                try {
                    HttpEntity entity = response.getEntity();

                    if( entity == null ) {
                        throw EC2Exception.create(status);
                    }
                    InputStream input = entity.getContent();
                    Document doc;

                    try {
                        doc = parseResponse(input);
                    }
                    finally {
                        input.close();
                    }
                    if( doc != null ) {
                        NodeList blocks = doc.getElementsByTagName("Error");
                        String code = null, message = null, requestId = null;

                        if( blocks.getLength() > 0 ) {
                            Node error = blocks.item(0);
                            NodeList attrs;

                            attrs = error.getChildNodes();
                            for( int i=0; i<attrs.getLength(); i++ ) {
                                Node attr = attrs.item(i);

                                if( attr.getNodeName().equals("Code") ) {
                                    code = attr.getFirstChild().getNodeValue().trim();
                                }
                                else if( attr.getNodeName().equals("Message") ) {
                                    message = attr.getFirstChild().getNodeValue().trim();
                                }
                            }

                        }
                        blocks = doc.getElementsByTagName("RequestID");
                        if( blocks.getLength() > 0 ) {
                            Node id = blocks.item(0);

                            requestId = id.getFirstChild().getNodeValue().trim();
                        }
                        if( message == null ) {
                            throw new CloudException(CloudErrorType.COMMUNICATION, status, null, "Unable to identify error condition: " + status + "/" + requestId + "/" + code);
                        }
                        throw EC2Exception.create(status, requestId, code, message);
                    }
                    throw new CloudException("Unable to parse error.");
                }
                catch( IOException e ) {
                    logger.error(e);
                    throw new CloudException(e);
                }
            }
	    }
	    finally {
            if (client != null) {
                client.getConnectionManager().shutdown();
            }
	        if( logger.isTraceEnabled() ) {
	            logger.trace("EXIT - " + EC2Method.class.getName() + ".invoke()");
	        }
            if( wire.isDebugEnabled() ) {
                wire.debug("--------------------------------------------------------------------------------------");
                wire.debug("");
            }

	    }
	}

	private Document parseResponse(String responseBody) throws CloudException, InternalException {
	    try {
            if( wire.isDebugEnabled() ) {
                String[] lines = responseBody.split("\n");

                if( lines.length < 1 ) {
                    lines = new String[] { responseBody };
                }
                for( String l : lines ) {
                    wire.debug(l);
                }
            }
            return XMLParser.parse(new ByteArrayInputStream(responseBody.getBytes()));
	    }
	    catch( IOException e ) {
	        throw new CloudException(e);
	    }
	    catch( ParserConfigurationException e ) {
            throw new CloudException(e);
        }
        catch( SAXException e ) {
            throw new CloudException(e);
        }
	}

	private Document parseResponse(InputStream responseBodyAsStream) throws CloudException, InternalException {
        BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(responseBodyAsStream));
			StringBuilder sb = new StringBuilder();
			String line;

			while( (line = in.readLine()) != null ) {
				sb.append(line);
				sb.append("\n");
			}
			return parseResponse(sb.toString());
		}
		catch( IOException e ) {
			throw new CloudException(e);
		}
        finally {
            if( in != null ) {
                try {
                    in.close();
                } catch( IOException e ) {
                    // Ignore
                }
            }
        }
    }

}
