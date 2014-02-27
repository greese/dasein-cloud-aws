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

import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;

@SuppressWarnings("serial")
public class EC2Exception extends CloudException {
    private String requestId = null;

    public EC2Exception(int status, String requestId, String code, String message) {
        super(toCloudErrorType(code), status, code, message);
        this.requestId = requestId;
    }

    private static CloudErrorType toCloudErrorType(String code) {
        if ("Throttling".equals(code)) {
            return CloudErrorType.THROTTLING;
        } else if ("TooManyBuckets".equals(code)) {
            return CloudErrorType.QUOTA;
        } else {
            return CloudErrorType.GENERAL;
        }
    }

    public String getCode() {
        return getProviderCode();
    }

    public String getRequestId() {
        return requestId;
    }

    public int getStatus() {
        return getHttpCode();
    }

    public String getSummary() {
        return (getStatus() + "/" + requestId + "/" + getCode() + ": " + getMessage());
    }
}
