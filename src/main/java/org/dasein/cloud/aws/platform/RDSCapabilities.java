/*
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
package org.dasein.cloud.aws.platform;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.platform.RelationalDatabaseCapabilities;

import javax.annotation.Nonnull;
import java.util.Locale;

/**
 * Description
 * <p>Created by stas: 05/08/2014 14:43</p>
 *
 * @author Stas Maksimov
 * @version 2014.08 initial version
 * @since 2014.08
 */
public class RDSCapabilities extends AbstractCapabilities<AWSCloud> implements RelationalDatabaseCapabilities {

    public RDSCapabilities( @Nonnull AWSCloud provider ) {
        super(provider);
    }

    @Nonnull
    @Override
    public String getProviderTermForDatabase( Locale locale ) {
        return "database";
    }

    @Nonnull
    @Override
    public String getProviderTermForSnapshot( Locale locale ) {
        return "snapshot";
    }

    @Override
    public boolean isSupportsFirewallRules() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean isSupportsHighAvailability() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean isSupportsLowAvailability() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean isSupportsMaintenanceWindows() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean isSupportsAlterDatabase() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean isSupportsSnapshots() throws CloudException, InternalException {
        return true;
    }
}
