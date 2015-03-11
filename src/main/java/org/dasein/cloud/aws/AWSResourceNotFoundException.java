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

package org.dasein.cloud.aws;

import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;

import javax.annotation.Nonnull;
import java.net.HttpURLConnection;

/**
 * Exception to be used when some AWS resource should exist, but cannot be found for some reason
 *
 * @author igoonich
 * @since 19.03.2014
 */
public class AWSResourceNotFoundException extends CloudException {
    private static final long serialVersionUID = 950720238875342406L;

    public AWSResourceNotFoundException(@Nonnull String msg) {
        super(CloudErrorType.GENERAL, HttpURLConnection.HTTP_NOT_FOUND, "none", msg);
    }

    public AWSResourceNotFoundException(@Nonnull String msg, @Nonnull Throwable cause) {
        super(CloudErrorType.GENERAL, HttpURLConnection.HTTP_NOT_FOUND, "none", msg, cause);
    }
}
