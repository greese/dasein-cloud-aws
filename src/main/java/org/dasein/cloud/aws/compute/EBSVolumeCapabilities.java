/**
 * Copyright (C) 2009-2013 Dell, Inc.
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

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.compute.VolumeCapabilities;
import org.dasein.cloud.compute.VolumeFormat;
import org.dasein.cloud.util.NamingConstraints;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Storage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

/**
 * Describes the capabilities of AWS with respect to Dasein volume operations.
 * <p>Created by Stas Maksimov: 10/03/2014 00:53</p>
 *
 * @author Stas Maksimov
 * @version 2014.03 initial version
 * @since 2014.03
 */
public class EBSVolumeCapabilities extends AbstractCapabilities<AWSCloud> implements VolumeCapabilities {

    public EBSVolumeCapabilities(AWSCloud provider) {
        super(provider);
    }

    @Override
    public boolean canAttach(VmState vmState) throws InternalException, CloudException {
        return true;
    }

    @Override
    public boolean canDetach(VmState vmState) throws InternalException, CloudException {
        return true;
    }

    @Override
    public int getMaximumVolumeCount() throws InternalException, CloudException {
        return -2;
    }

    @Override
    public int getMaximumVolumeProductIOPS() throws InternalException, CloudException {
        return 0;
    }

    @Override
    public int getMinimumVolumeProductIOPS() throws InternalException, CloudException {
        return 0;
    }

    @Override
    public int getMaximumVolumeSizeIOPS() throws InternalException, CloudException {
        return 0;
    }

    @Override
    public int getMinimumVolumeSizeIOPS() throws InternalException, CloudException {
        return 0;
    }

    static private final Storage<Gigabyte> maxVolSize = new Storage<Gigabyte>(1024, Storage.GIGABYTE);

    @Nullable
    @Override
    public Storage<Gigabyte> getMaximumVolumeSize() throws InternalException, CloudException {
        return maxVolSize;
    }

    static private final Storage<Gigabyte> minVolSize = new Storage<Gigabyte>(10, Storage.GIGABYTE);

    @Nonnull
    @Override
    public Storage<Gigabyte> getMinimumVolumeSize() throws InternalException, CloudException {
        return minVolSize;
    }

    @Nonnull
    @Override
    public NamingConstraints getVolumeNamingConstraints() throws CloudException, InternalException {
        return null;
    }

    @Nonnull
    @Override
    public String getProviderTermForVolume(@Nonnull Locale locale) {
        return "volume";
    }

    @Nonnull
    @Override
    public Requirement getVolumeProductRequirement() throws InternalException, CloudException {
        return ((getProvider().getEC2Provider().isEucalyptus() || getProvider().getEC2Provider().isOpenStack())
                ? Requirement.NONE
                : Requirement.OPTIONAL);
    }

    @Override
    public boolean isVolumeSizeDeterminedByProduct() throws InternalException, CloudException {
        return false;
    }

    static private Collection<String> deviceIdsWindows;
    static private Collection<String> deviceIdsUnix;

    @Nonnull
    @Override
    public Iterable<String> listPossibleDeviceIds(@Nonnull Platform platform) throws InternalException, CloudException {
        if( platform.isWindows() ) {
            if( deviceIdsWindows == null ) {
                deviceIdsWindows = Arrays.asList("xvdf", "xvdg", "xvdh", "xvdi", "xvdj");
            }
            return deviceIdsWindows;
        }
        if( deviceIdsUnix == null ) {
            deviceIdsUnix = Arrays.asList("/dev/sdf", "/dev/sdg", "/dev/sdh", "/dev/sdi", "/dev/sdj");
        }
        return deviceIdsUnix;
    }

    @Nonnull
    @Override
    public Iterable<VolumeFormat> listSupportedFormats() throws InternalException, CloudException {
        return Collections.singletonList(VolumeFormat.BLOCK);
    }

    @Nonnull
    @Override
    public Requirement requiresVMOnCreate() throws InternalException, CloudException {
        return Requirement.NONE;
    }
}
