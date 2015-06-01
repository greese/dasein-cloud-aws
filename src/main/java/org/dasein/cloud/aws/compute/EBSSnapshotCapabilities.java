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

package org.dasein.cloud.aws.compute;

import org.dasein.cloud.*;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.compute.SnapshotCapabilities;
import org.dasein.cloud.util.NamingConstraints;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Locale;

/**
 * Describes the capabilities of AWS with respect to Dasein image operations.
 * <p>Created by Stas Maksimov: 10/03/2014 01:04</p>
 *
 * @author Stas Maksimov
 * @version 2014.03 initial version
 * @since 2014.03
 */
public class EBSSnapshotCapabilities extends AbstractCapabilities<AWSCloud> implements SnapshotCapabilities {
    public EBSSnapshotCapabilities(AWSCloud provider) {
        super(provider);
    }

    @Nonnull
    @Override
    public String getProviderTermForSnapshot(@Nonnull Locale locale) {
        return "snapshot";
    }

    @Nullable
    @Override
    public VisibleScope getSnapshotVisibleScope() {
        return null;
    }

    @Nonnull
    @Override
    public Requirement identifyAttachmentRequirement() throws InternalException, CloudException {
        return Requirement.OPTIONAL;
    }

    @Override
    public boolean supportsSnapshotCopying() throws CloudException, InternalException {
        return getProvider().getEC2Provider().isAWS();
    }

    @Override
    public boolean supportsSnapshotCreation() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean supportsSnapshotSharing() throws InternalException, CloudException {
        return getProvider().getEC2Provider().isAWS();
    }

    @Override
    public boolean supportsSnapshotSharingWithPublic() throws InternalException, CloudException {
        return getProvider().getEC2Provider().isAWS();
    }

    @Override
    public @Nonnull NamingConstraints getSnapshotNamingConstraints() throws CloudException, InternalException {
        return NamingConstraints.getAlphaNumeric(0, 255);
    }
}
