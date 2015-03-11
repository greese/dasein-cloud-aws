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

package org.dasein.cloud.aws.identity;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.identity.ShellKeyCapabilities;

import javax.annotation.Nonnull;
import java.util.Locale;

/**
 * Created by stas on 14/11/2014.
 */
public class KeypairsCapabilities extends AbstractCapabilities<AWSCloud> implements ShellKeyCapabilities {

    public KeypairsCapabilities( @Nonnull AWSCloud provider ) {
        super(provider);
    }

    @Override
    public @Nonnull Requirement identifyKeyImportRequirement() throws CloudException, InternalException {
        return Requirement.OPTIONAL;
    }

    @Override
    public @Nonnull String getProviderTermForKeypair( @Nonnull Locale locale ) {
        return "keypair";
    }

}
