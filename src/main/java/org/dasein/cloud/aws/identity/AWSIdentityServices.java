/**
 * Copyright (C) 2009-2013 Enstratius, Inc.
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

import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.identity.AbstractIdentityServices;

public class AWSIdentityServices extends AbstractIdentityServices {
    private AWSCloud cloud;
    
    public AWSIdentityServices(AWSCloud cloud) { this.cloud = cloud; }
    
    @Override
    public IAM getIdentityAndAccessSupport() {
        if( cloud.getEC2Provider().isAWS() || cloud.getEC2Provider().isEnStratus() ) {
            return new IAM(cloud);
        }
        return null;
    }

    @Override
    public Keypairs getShellKeySupport() {
        return new Keypairs(cloud);
    }
}
