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

import org.apache.http.client.methods.*;
import org.dasein.cloud.InternalException;

public enum GlacierAction {
    CREATE_VAULT, DELETE_VAULT, DESCRIBE_VAULT, LIST_VAULTS, CREATE_ARCHIVE,
    DELETE_ARCHIVE, CREATE_JOB, DESCRIBE_JOB, LIST_JOBS, GET_JOB_OUTPUT;

    public HttpRequestBase getMethod(String url) throws InternalException {

        switch( GlacierAction.this ) {
            case DELETE_VAULT: case DELETE_ARCHIVE:
                return new HttpDelete(url);
            case LIST_VAULTS: case DESCRIBE_VAULT: case DESCRIBE_JOB: case LIST_JOBS: case GET_JOB_OUTPUT:
                return new HttpGet(url);
            case CREATE_VAULT:
                return new HttpPut(url);
            case CREATE_ARCHIVE: case CREATE_JOB:
                return new HttpPost(url);
        }
        throw new InternalException("failed to build method");
    }
}