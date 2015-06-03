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

import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.platform.AbstractPlatformServices;
import org.dasein.cloud.platform.MonitoringSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class AWSPlatformServices extends AbstractPlatformServices<AWSCloud> {

    public AWSPlatformServices(AWSCloud provider) {
        super(provider);
    }

    @Override
    public @Nonnull CloudFront getCDNSupport() {
        return new CloudFront(getProvider());
    }
    
    @Override
    public @Nonnull SimpleDB getKeyValueDatabaseSupport() {
        return new SimpleDB(getProvider());
    }
    
    @Override
    public @Nonnull SNS getPushNotificationSupport() {
        return new SNS(getProvider());
    }
    
    @Override
    public @Nonnull RDS getRelationalDatabaseSupport() {
        return new RDS(getProvider());
    }

    @Override
    public @Nonnull MonitoringSupport getMonitoringSupport() {
      return new CloudWatch( getProvider() );
    }

}
