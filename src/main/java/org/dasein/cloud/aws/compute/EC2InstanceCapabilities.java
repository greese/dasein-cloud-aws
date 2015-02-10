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

import org.dasein.cloud.*;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VMScalingCapabilities;
import org.dasein.cloud.compute.VirtualMachineCapabilities;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.util.NamingConstraints;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

/**
 * Describes the capabilities of AWS with respect to Dasein virtual machine operations.
 * <p>Created by George Reese: 2/27/14 4:36 PM</p>
 * @author George Reese
 * @version 2014.03 initial version
 * @since 2014.03
 */
public class EC2InstanceCapabilities extends AbstractCapabilities<AWSCloud> implements VirtualMachineCapabilities {

    public EC2InstanceCapabilities(@Nonnull AWSCloud cloud) { super(cloud); }

    @Override
    public boolean canAlter(@Nonnull VmState fromState) throws CloudException, InternalException {
        return VmState.STOPPED.equals(fromState);
    }

    @Override
    public boolean canClone(@Nonnull VmState fromState) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean canPause(@Nonnull VmState fromState) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean canReboot(@Nonnull VmState fromState) throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean canResume(@Nonnull VmState fromState) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean canStart(@Nonnull VmState fromState) throws CloudException, InternalException {
        return !fromState.equals(VmState.RUNNING);
    }

    @Override
    public boolean canStop(@Nonnull VmState fromState) throws CloudException, InternalException {
        return !fromState.equals(VmState.STOPPED);
    }

    @Override
    public boolean canSuspend(@Nonnull VmState fromState) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean canTerminate(@Nonnull VmState fromState) throws CloudException, InternalException {
        return !VmState.TERMINATED.equals(fromState);
    }

    @Override
    public boolean canUnpause(@Nonnull VmState fromState) throws CloudException, InternalException {
        return false;
    }

    @Override
    public int getMaximumVirtualMachineCount() throws CloudException, InternalException {
        return Capabilities.LIMIT_UNKNOWN;
    }

    @Override
    public int getCostFactor(@Nonnull VmState vmState) throws CloudException, InternalException {
        return (vmState.equals(VmState.STOPPED) ? 0 : 100);
    }

    @Override
    public @Nonnull String getProviderTermForVirtualMachine(@Nonnull Locale locale) throws CloudException, InternalException {
        return "instance";
    }

    @Override
    public @Nonnull NamingConstraints getVirtualMachineNamingConstraints() {
        return NamingConstraints.getAlphaNumeric(1, 100);
    }

    @Nullable
    @Override
    public VisibleScope getVirtualMachineVisibleScope() {
        return null;
    }

    @Nullable
    @Override
    public VisibleScope getVirtualMachineProductVisibleScope() {
        return null;
    }

    @Override
    public @Nullable VMScalingCapabilities getVerticalScalingCapabilities() throws CloudException, InternalException {
        return VMScalingCapabilities.getInstance(false, true, false);
    }

    @Nonnull
    @Override
    public Requirement identifyDataCenterLaunchRequirement() throws CloudException, InternalException {
        return Requirement.OPTIONAL;
    }

    @Override
    public @Nonnull Requirement identifyImageRequirement(@Nonnull ImageClass cls) throws CloudException, InternalException {
        return (ImageClass.MACHINE.equals(cls) ? Requirement.REQUIRED : Requirement.OPTIONAL);
    }

    @Override
    public @Nonnull Requirement identifyPasswordRequirement(Platform platform) throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyRootVolumeRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyShellKeyRequirement(Platform platform) throws CloudException, InternalException {
        return platform.isWindows()? Requirement.REQUIRED : Requirement.OPTIONAL;
    }

    @Override
    public @Nonnull Requirement identifyStaticIPRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Nonnull
    @Override
    public Requirement identifySubnetRequirement() throws CloudException, InternalException {
        // Did not exist in 2013.07
        return (getProvider().getEC2Provider().isEucalyptus() ? Requirement.NONE : Requirement.OPTIONAL);
    }

    @Override
    public @Nonnull Requirement identifyVlanRequirement() throws CloudException, InternalException {
        // Optional as per 2013.07
        return (getProvider().getEC2Provider().isEucalyptus() ? Requirement.NONE : Requirement.OPTIONAL);
    }

    @Override
    public boolean isAPITerminationPreventable() throws CloudException, InternalException {
        return getProvider().getEC2Provider().isAWS();
    }

    @Override
    public boolean isBasicAnalyticsSupported() throws CloudException, InternalException {
        return (getProvider().getEC2Provider().isAWS() || getProvider().getEC2Provider().isEnStratus());
    }

    @Override
    public boolean isExtendedAnalyticsSupported() throws CloudException, InternalException {
        return (getProvider().getEC2Provider().isAWS() || getProvider().getEC2Provider().isEnStratus());
    }

    @Override
    public boolean isUserDataSupported() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean isUserDefinedPrivateIPSupported() throws CloudException, InternalException {
        return false;
    }

    static private volatile Collection<Architecture> architectures;

    @Override
    public @Nonnull Iterable<Architecture> listSupportedArchitectures() throws InternalException, CloudException {
        if (architectures == null) {
            architectures = Arrays.asList(Architecture.I32, Architecture.I64);
        }
        return architectures;
    }

    @Override public boolean supportsSpotVirtualMachines() throws InternalException, CloudException {
        return (getProvider().getEC2Provider().isAWS());
    }

    @Override public boolean supportsAlterVM() {
        return true;
    }

    @Override public boolean supportsClone() {
        return true;
    }

    @Override public boolean supportsPause() {
        return false;
    }

    @Override public boolean supportsReboot() {
        return true;
    }

    @Override public boolean supportsResume() {
        return false;
    }

    @Override public boolean supportsStart() {
        return true;
    }

    @Override public boolean supportsStop() {
        return true;
    }

    @Override public boolean supportsSuspend() {
        return false;
    }

    @Override public boolean supportsTerminate() {
        return true;
    }

    @Override public boolean supportsUnPause() {
        return false;
    }
}
