/*
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

package org.dasein.cloud.aws.storage;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.storage.BlobStoreCapabilities;
import org.dasein.cloud.util.NamingConstraints;
import org.dasein.util.uom.storage.*;
import org.dasein.util.uom.storage.Byte;

import javax.annotation.Nonnull;
import java.util.Locale;

/**
 * Created by stas on 02/06/2015.
 */
public class GlacierCapabilities extends AbstractCapabilities<AWSCloud> implements BlobStoreCapabilities {
    static public final int                                       MAX_VAULTS       = 1000;
    static public final int                                       MAX_ARCHIVES     = -1;
    static public final Storage<Megabyte>                         MAX_OBJECT_SIZE  = new Storage<Megabyte>(100L, Storage.MEGABYTE);

    public GlacierCapabilities(AWSCloud provider) {
        super(provider);
    }

    @Override
    public boolean allowsNestedBuckets() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean allowsRootObjects() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean allowsPublicSharing() throws CloudException, InternalException {
        return true;
    }

    @Override
    public int getMaxBuckets() throws CloudException, InternalException {
        return MAX_VAULTS;
    }

    @Nonnull
    @Override
    public Storage<Byte> getMaxObjectSize() throws InternalException, CloudException {
        return (Storage<org.dasein.util.uom.storage.Byte>)MAX_OBJECT_SIZE.convertTo(Storage.BYTE);
    }

    @Override
    public int getMaxObjectsPerBucket() throws CloudException, InternalException {
        return MAX_ARCHIVES;
    }

    @Nonnull
    @Override
    public NamingConstraints getBucketNamingConstraints() throws CloudException, InternalException {
        return NamingConstraints.getAlphaNumeric(1, 255).lowerCaseOnly().limitedToLatin1().constrainedBy(new char[] { '-', '.' });
    }

    @Nonnull
    @Override
    public NamingConstraints getObjectNamingConstraints() throws CloudException, InternalException {
        return NamingConstraints.getAlphaNumeric(1, 255).lowerCaseOnly().limitedToLatin1().constrainedBy(new char[] { '-', '.', ',', '#', '+' });
    }

    @Nonnull
    @Override
    public String getProviderTermForBucket(@Nonnull Locale locale) {
        return "vault";
    }

    @Nonnull
    @Override
    public String getProviderTermForObject(@Nonnull Locale locale) {
        return "archive";
    }
}
