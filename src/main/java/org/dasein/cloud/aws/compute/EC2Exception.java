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

import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;

import java.net.HttpURLConnection;

@SuppressWarnings("serial")
public class EC2Exception extends CloudException {

    private static final String DEFAULT_ERROR_CODE = "NoResponse";
    private static final String DEFAULT_ERROR_MESSAGE = "No response body was specified";

    private String requestId = null;

    private EC2Exception(CloudErrorType errorType, int status, String requestId, String code, String message) {
        super(errorType, status, code, message);
        this.requestId = requestId;
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

    public static EC2Exception create(int status) {
        return new EC2Exception(CloudErrorType.GENERAL, status, null, DEFAULT_ERROR_CODE, DEFAULT_ERROR_MESSAGE);
    }

    public static EC2Exception create(int status, String requestId, String code, String message) {
        CloudErrorType errorType = toCloudErrorType(code);
        if (CloudErrorType.AUTHENTICATION.equals(errorType)) {
            // for authentication exception use 401 error code
            return new EC2Exception(errorType, HttpURLConnection.HTTP_UNAUTHORIZED, requestId, code, message);
        }
        return new EC2Exception(errorType, status, requestId, code, message);
    }

    /**
     * Converts AWS error code to dasein cloud error type
     *
     * @param code AWS error code
     * @return dasein cloud error type
     */
    private static CloudErrorType toCloudErrorType(String code) {
        if ("Throttling".equals(code)) {
            return CloudErrorType.THROTTLING;
        } else if ("TooManyBuckets".equals(code)) {
            return CloudErrorType.QUOTA;
        } else if ("SignatureDoesNotMatch".equals(code)) {
            return CloudErrorType.AUTHENTICATION;
        } else {
            return CloudErrorType.GENERAL;
        }
    }
}
