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
import org.dasein.cloud.compute.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

/**
 * Describes the capabilities of AWS with respect to Dasein image operations.
 * <p>Created by Stas Maksimov: 09/03/2014 18:09</p>
 *
 * @author Stas Maksimov
 * @version 2014.03 initial version
 * @since 2014.03
 */
public class AMICapabilities extends AbstractCapabilities<AWSCloud> implements ImageCapabilities {

    public AMICapabilities(AWSCloud cloud) {
        super(cloud);
    }

    @Override
    public boolean canBundle(@Nonnull VmState vmState) throws CloudException, InternalException {
        return VmState.RUNNING.equals(vmState);
    }

    @Override
    public boolean canImage(@Nonnull VmState vmState) throws CloudException, InternalException {
        return (vmState.RUNNING.equals(vmState) || vmState.STOPPED.equals(vmState));
    }

    @Nonnull
    @Override
    public String getProviderTermForImage(@Nonnull Locale locale, @Nonnull ImageClass imageClass) {
        switch( imageClass ) {
            case MACHINE: return "AMI";
            case KERNEL: return "AKI";
            case RAMDISK: return "ARI";
        }
        return "image";
    }

    @Nonnull
    @Override
    public String getProviderTermForCustomImage(@Nonnull Locale locale, @Nonnull ImageClass imageClass) {
        return getProviderTermForImage(locale, imageClass);
    }

    @Override
    public @Nullable VisibleScope getImageVisibleScope() {
        return null;
    }

    @Nonnull
    @Override
    public Requirement identifyLocalBundlingRequirement() throws CloudException, InternalException {
        return Requirement.REQUIRED;
    }

    @Nonnull
    @Override
    public Iterable<MachineImageFormat> listSupportedFormats() throws CloudException, InternalException {
        return Collections.singletonList(MachineImageFormat.AWS);
    }

    @Nonnull
    @Override
    public Iterable<MachineImageFormat> listSupportedFormatsForBundling() throws CloudException, InternalException {
        return Collections.singletonList(MachineImageFormat.AWS);
    }

    static private Collection<ImageClass> supportedClasses;

    @Nonnull
    @Override
    public Iterable<ImageClass> listSupportedImageClasses() throws CloudException, InternalException {
        if( supportedClasses == null ) {
            supportedClasses = Arrays.asList(ImageClass.KERNEL, ImageClass.MACHINE, ImageClass.RAMDISK);
        }
        return supportedClasses;
    }

    static private Collection<MachineImageType> supportedTypes;

    @Nonnull
    @Override
    public Iterable<MachineImageType> listSupportedImageTypes() throws CloudException, InternalException {
        if( supportedTypes == null ) {
            supportedTypes = Arrays.asList(MachineImageType.STORAGE, MachineImageType.VOLUME);
        }
        return supportedTypes;
    }

    @Override
    public boolean supportsDirectImageUpload() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsImageCapture(@Nonnull MachineImageType machineImageType) {
        return MachineImageType.VOLUME.equals(machineImageType);
    }

    @Override
    public boolean supportsImageSharing() {
        return getProvider().getEC2Provider().isAWS();
    }

    @Override
    public boolean supportsImageSharingWithPublic() {
        return getProvider().getEC2Provider().isAWS();
    }

    @Override
    public boolean supportsPublicLibrary(@Nonnull ImageClass imageClass) {
        return getProvider().getEC2Provider().isAWS();
    }
}
