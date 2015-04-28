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

package org.dasein.cloud.aws.storage;

import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.storage.AbstractStorageServices;

import javax.annotation.Nonnull;

/**
 * Provides access to Amazon S3 and Glacier as online and offline storage options, respectively.
 * @author George Reese
 * @version 2013.07 added support for Glacier
 * @since unknown
 */
public class AWSCloudStorageServices extends AbstractStorageServices<AWSCloud> {
    public AWSCloudStorageServices(AWSCloud provider) {
        super(provider);
    }

    @Override
    public @Nonnull Glacier getOfflineStorageSupport() {
        return new Glacier(getProvider());
    }

    @Override
    public @Nonnull S3 getOnlineStorageSupport() {
        return new S3(getProvider());
    }
}
