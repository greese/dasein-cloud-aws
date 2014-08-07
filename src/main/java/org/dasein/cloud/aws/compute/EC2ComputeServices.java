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

import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.compute.AbstractComputeServices;
import org.dasein.cloud.compute.AffinityGroupSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class EC2ComputeServices extends AbstractComputeServices {
    private AWSCloud cloud;
    
    public EC2ComputeServices(@Nonnull AWSCloud cloud) { this.cloud = cloud; }

    @Nullable @Override public AffinityGroupSupport getAffinityGroupSupport() {
        return null;
    }

    @Override
    public @Nullable AutoScaling getAutoScalingSupport() {
        if( !cloud.getEC2Provider().isAWS() && !cloud.getEC2Provider().isEnStratus() ) {
            return null;
        }
        return new AutoScaling(cloud);
    }
    
    @Override
    public @Nonnull AMI getImageSupport() {
        return new AMI(cloud);
    }
    
    @Override
    public @Nonnull EBSSnapshot getSnapshotSupport() {
        return new EBSSnapshot(cloud);
    }
    
    @Override
    public @Nonnull EC2Instance getVirtualMachineSupport() {
        return new EC2Instance(cloud);
    }
    
    @Override
    public @Nonnull EBSVolume getVolumeSupport() {
        return new EBSVolume(cloud);
    }

    @Override public boolean hasAffinityGroupSupport() {
        return false;
    }
}
