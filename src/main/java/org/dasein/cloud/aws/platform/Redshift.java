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

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.Tag;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.network.FirewallReference;
import org.dasein.cloud.platform.bigdata.DataCluster;
import org.dasein.cloud.platform.bigdata.DataClusterCreateOptions;
import org.dasein.cloud.platform.bigdata.DataClusterFirewall;
import org.dasein.cloud.platform.bigdata.DataClusterParameterGroup;
import org.dasein.cloud.platform.bigdata.DataClusterProduct;
import org.dasein.cloud.platform.bigdata.DataClusterSnapshot;
import org.dasein.cloud.platform.bigdata.DataClusterSnapshotFilterOptions;
import org.dasein.cloud.platform.bigdata.DataClusterVersion;
import org.dasein.cloud.platform.bigdata.AbstractDataWarehouseSupport;
import org.dasein.cloud.storage.CloudStorageLogging;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

/**
 * Implementation of AWS Redshift support based on the Dasein Cloud Data Warehouse API. Key mappings are:
 * <ul>
 *     <li>Cluster -&gt; Data Cluster</li>
 *     <li>Cluster Security Group -&gt; Data Cluster Firewall</li>
 *     <li>Cluster Snapshot -&gt; Data Cluster Snapshot</li>
 *     <li>Cluster Type -&gt; Data Cluster Product</li>
 * </ul>
 * @since 2014.03
 * @version 2014.03 initial version (issue #83)
 */
public class Redshift extends AbstractDataWarehouseSupport<AWSCloud> {
    public Redshift(@Nonnull AWSCloud provider) { super(provider); }

    @Override
    public void addSnapshotShare(@Nonnull String snapshotId, @Nonnull String accountNumber) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void authorizeComputeFirewalls(@Nonnull String dataClusterFirewallId, @Nonnull FirewallReference... firewalls) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void authorizeIPs(@Nonnull String dataClusterFirewallId, @Nonnull String... cidrs) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull String createCluster(@Nonnull DataClusterCreateOptions options) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull String createClusterFirewall(@Nonnull String name, @Nonnull String description) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull String createClusterParameterGroup(@Nonnull String family, @Nonnull String name, @Nonnull String description, @Nonnull Map<String, Object> initialParameters) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull String createClusterSnapshot(@Nonnull String clusterId, @Nonnull String name, @Nonnull String description) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void disableLogging(@Nonnull String clusterId) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void enableLogging(@Nonnull String clusterId, @Nonnull String bucket, @Nonnull String prefix) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DataCluster getCluster(@Nonnull String clusterId) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DataClusterFirewall getClusterFirewall(@Nonnull String firewallId) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public CloudStorageLogging getClusterLoggingStatus(@Nonnull String clusterId) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DataClusterParameterGroup getClusterParameterGroup(@Nonnull String groupId) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DataClusterProduct getClusterProduct(@Nonnull String productId) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DataClusterSnapshot getClusterSnapshot(@Nonnull String snapshotId) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull Requirement getDataCenterConstraintRequirement() throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull Iterable<DataClusterFirewall> listClusterFirewalls() throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull Iterable<DataClusterParameterGroup> listClusterParameterGroups() throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull Iterable<DataClusterProduct> listClusterProducts() throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull Iterable<DataClusterSnapshot> listClusterSnapshots() throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull Iterable<DataClusterSnapshot> listClusterSnapshots(@Nullable DataClusterSnapshotFilterOptions options) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull Iterable<DataClusterVersion> listClusterVersions() throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull Iterable<DataCluster> listClusters() throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void rebootCluster(@Nonnull String clusterId) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void removeAllSnapshotShares(@Nonnull String snapshotId) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void removeCluster(@Nonnull String clusterId, boolean snapshotFirst) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void removeClusterFirewall(@Nonnull String firewallId) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void removeClusterParameterGroup(@Nonnull String groupId) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void removeClusterSnapshot(@Nonnull String snapshotId) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void removeSnapshotShare(@Nonnull String snapshotId, @Nonnull String accountNumber) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void resizeCluster(@Nonnull String clusterId, @Nonnegative int nodeCount) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void revokeComputeFirewalls(@Nonnull String dataClusterFirewallId, @Nonnull FirewallReference... firewalls) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void revokeIPs(@Nonnull String dataClusterFirewallId, @Nonnull String... cidrs) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void rotateEncryptionKeys(@Nonnull String clusterId) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsAuthorizingComputeFirewalls() throws CloudException, InternalException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsCloudStorageLogging() throws CloudException, InternalException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsClusterFirewalls() throws CloudException, InternalException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsClusterSnapshots(boolean automated) throws CloudException, InternalException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsEncryptionAtRest() throws CloudException, InternalException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateParameters(@Nonnull String parameterGroupId, @Nonnull Map<String, Object> parameters) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsSnapshotSharing() throws InternalException, CloudException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateClusterTags(@Nonnull String clusterId, @Nonnull Tag... tags) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateClusterTags(@Nonnull String[] clusterIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateSnapshotTags(@Nonnull String snapshotId, @Nonnull Tag... tags) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateSnapshotTags(@Nonnull String[] snapshotIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
