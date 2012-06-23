/**
 * Copyright (C) 2009-2012 enStratus Networks Inc
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

public class AWSPlatformServices extends AbstractPlatformServices {
    private AWSCloud cloud;
    
    public AWSPlatformServices(AWSCloud cloud) { this.cloud = cloud; }
    
    @Override
    public CloudFront getCDNSupport() {
        return new CloudFront(cloud);
    }
    
    @Override
    public SimpleDB getKeyValueDatabaseSupport() {
        return new SimpleDB(cloud);
    }
    
    @Override
    public SNS getPushNotificationSupport() {
        return new SNS(cloud);
    }
    
    @Override
    public RDS getRelationalDatabaseSupport() {
        return new RDS(cloud);
    }
}
