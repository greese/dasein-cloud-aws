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

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.aws.AWSCloud;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

/**
 * [Class Documentation]
 * <p>Created by George Reese: 5/22/13 2:46 PM</p>
 *
 * @author George Reese
 */
public class GlacierMethod {
    static private final Logger logger = Logger.getLogger(GlacierMethod.class);

    static public final String GLACIER_PREFIX    = "glacier:";
    static public final String SERVICE_ID        = "glacier";
    static public final String API_VERSION       = "2012-06-01";

    private GlacierAction action           = null;
    private Map<String,String> headers     = null;
    private Map<String,String> queryParameters = null;
    private AWSCloud provider              = null;
    private String vaultId                 = null;
    private String archiveId               = null;
    private String jobId                   = null;
    private String bodyText                = null;
    private File bodyFile                  = null;

    private GlacierMethod(Builder builder) {
        this.action = builder.action;
        this.provider = builder.provider;
        this.vaultId = builder.vaultId;
        this.archiveId = builder.archiveId;
        this.jobId = builder.jobId;
        this.headers = builder.headers == null ? new HashMap<String,String>() : builder.headers;
        this.queryParameters = builder.queryParameters == null ? new HashMap<String, String>() : builder.queryParameters;
        this.bodyText = builder.bodyText;
        this.bodyFile = builder.bodyFile;
    }

    private static byte[] computePayloadSHA256Hash(byte[] payload) throws NoSuchAlgorithmException, IOException {
        BufferedInputStream inputStream =
                new BufferedInputStream(new ByteArrayInputStream(payload));
        MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
        byte[] buffer = new byte[4096];
        int bytesRead;
        while ( (bytesRead = inputStream.read(buffer, 0, buffer.length)) != -1 ) {
            messageDigest.update(buffer, 0, bytesRead);
        }
        return messageDigest.digest();
    }

    static private final Logger wire = AWSCloud.getWireLogger(Glacier.class);

    /**
     * Invokes the method and returns the response body as a JSON object
     * @return JSON object
     * @throws InternalException
     * @throws CloudException
     * @throws GlacierException
     */
    public JSONObject invokeJson() throws InternalException, CloudException {
        ClientAndResponse clientAndResponse = null;
        String content;
        try {
            clientAndResponse = invokeInternal();
            Header contentType = clientAndResponse.response.getFirstHeader("content-type");
            if (!"application/json".equalsIgnoreCase(contentType.getValue())) {
                throw new CloudException("Invalid Glacier response: expected JSON");
            }
            final HttpEntity entity = clientAndResponse.response.getEntity();
            content = EntityUtils.toString(entity);
            if (content == null) {
                return null;
            }
            return new JSONObject(content);

        } catch (IOException e) {
            throw new CloudException(e);
        } catch (JSONException e) {
            throw new CloudException(e);
        } finally {
            if (clientAndResponse != null) {
                clientAndResponse.client.getConnectionManager().shutdown();
            }
        }
    }

    /**
     * Invokes the method and returns the response headers
     * @return map of response headers; duplicate header keys are ignored
     * @throws InternalException
     * @throws CloudException
     * @throws GlacierException
     */
    public Map<String, String> invokeHeaders() throws InternalException, CloudException {
        ClientAndResponse clientAndResponse = invokeInternal();
        try {
            Map<String, String> headers = new HashMap<String, String>();
            // doesn't support duplicate header keys, but they are unused by glacier
            for (Header header : clientAndResponse.response.getAllHeaders()) {
                headers.put(header.getName().toLowerCase(), header.getValue());
            }
            return headers;
        }finally {
            clientAndResponse.client.getConnectionManager().shutdown();
        }
    }

    /**
     * Invokes the method and returns nothing
     * @throws InternalException
     * @throws CloudException
     */
    public void invoke() throws InternalException, CloudException {

        final ClientAndResponse clientAndResponse = invokeInternal();
        clientAndResponse.client.getConnectionManager().shutdown();
    }

    private ClientAndResponse invokeInternal() throws InternalException, CloudException {

        if( wire.isDebugEnabled() ) {
            wire.debug("");
            wire.debug("----------------------------------------------------------------------------------");
        }
        try {
            final String url = getUrlWithParameters();
            final String host;
            try {
                host = new URI(url).getHost();
            } catch (URISyntaxException e) {
                throw new InternalException(e);
            }
            final HttpRequestBase method = action.getMethod(url);

            final HttpClient client = provider.getClient();

            final String accessId;
            final String secret;
            try {
                accessId = new String(provider.getContext().getAccessPublic(), "utf-8");
                secret = new String(provider.getContext().getAccessPrivate(), "utf-8");
            } catch (UnsupportedEncodingException e) {
                throw new InternalException(e);
            }
            headers.put(AWSCloud.P_AWS_DATE, provider.getV4HeaderDate(null));
            headers.put("x-amz-glacier-version", API_VERSION);
            headers.put("host", host);
            final String v4Authorization = provider.getV4Authorization(accessId, secret,
                    method.getMethod(), url, SERVICE_ID, headers, getRequestBodyHash());
            for( Map.Entry<String, String> entry : headers.entrySet() ) {
                method.addHeader(entry.getKey(), entry.getValue());
            }
            method.addHeader(AWSCloud.P_CFAUTH, v4Authorization);

            if (bodyText != null) {
                try {
                    ((HttpEntityEnclosingRequestBase)method).setEntity(new StringEntity(bodyText));
                } catch (UnsupportedEncodingException e) {
                    throw new InternalException(e);
                }
            }

            if( wire.isDebugEnabled() ) {
                wire.debug("[" + url + "]");
                wire.debug(method.getRequestLine().toString());
                for( Header header : method.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
                if( bodyText != null ) {
                    try { wire.debug(EntityUtils.toString(((HttpEntityEnclosingRequestBase)method).getEntity())); }
                    catch( IOException ignore ) { }

                    wire.debug("");
                }
            }

            HttpResponse httpResponse;
            try {
                httpResponse = client.execute(method);
            } catch (IOException e) {
                throw new CloudException(e);
            }
            if( wire.isDebugEnabled() ) {
                wire.debug(httpResponse.getStatusLine().toString());
                for( Header header : httpResponse.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }

            int status = httpResponse.getStatusLine().getStatusCode();
            if( status >= 400) {
                throw getGlacierException(httpResponse);
            } else {
                return new ClientAndResponse(client, httpResponse);
            }
        }
        finally {
            if( wire.isDebugEnabled() ) {
                wire.debug("----------------------------------------------------------------------------------");
                wire.debug("");
            }
        }
    }

    private GlacierException getGlacierException(HttpResponse httpResponse) {
        String errorCode;
        String errorBody = "";
        String errorMessage;

        try {
            errorBody = EntityUtils.toString(httpResponse.getEntity());
            JSONObject errorObject = new JSONObject(errorBody);
            errorCode = errorObject.getString("code");
            errorMessage = errorObject.getString("message");

        } catch (IOException e) {
            errorCode = errorMessage = "failed to read error";
        } catch (JSONException e) {
            errorMessage = errorBody;
            errorCode = "GeneralError";
        }

        int statusCode = httpResponse.getStatusLine().getStatusCode();
        CloudErrorType errorType = CloudErrorType.GENERAL;
        if (statusCode == 403) {
            errorType = CloudErrorType.AUTHENTICATION;
        }
        else if (statusCode == 400) {
            errorType = CloudErrorType.COMMUNICATION;
        }
        return new GlacierException(errorType, statusCode, errorCode, errorMessage);
    }

    private String getRequestBodyHash() throws InternalException {
        if (bodyText == null && bodyFile == null) {
            // use hash of the empty string
            return AWSCloud.computeSHA256Hash("");
        } else if (bodyText != null) {
            return AWSCloud.computeSHA256Hash(bodyText);
        } else {
            throw new OperationNotSupportedException("glacier file handling not implemented");
        }
    }

    private String getUrlWithParameters() throws InternalException, CloudException {
        if (queryParameters == null || queryParameters.size() == 0) {
            return getUrl();
        }

        final StringBuilder sb = new StringBuilder();
        sb.append(getUrl()).append("?");
        boolean first = true;
        for (Map.Entry<String, String> entry : queryParameters.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key == null || value == null) {
                throw new InternalException("Invalid query parameter: " + key);
            }
            key = AWSCloud.encode(key.trim(), false);
            value = AWSCloud.encode(value.trim(), false);
            if (key.length() == 0) {
                throw new InternalException("Empty query parameter key");
            }

            if (!first) {
                sb.append("&");
            } else {
                first = false;
            }
            sb.append(key).append("=").append(value);
        }
        return sb.toString();
    }

    public String getUrl() throws InternalException, CloudException {
        String baseUrl = provider.getGlacierUrl();
        StringBuilder url = new StringBuilder(baseUrl).append("/vaults");

        if (action == GlacierAction.LIST_VAULTS) {
            return url.toString();
        }

        if (vaultId == null) {
            throw new InternalException("vaultId required");
        }
        url.append("/").append(vaultId);

        switch (action) {
            case CREATE_VAULT: case DELETE_VAULT: case DESCRIBE_VAULT:
                break;
            case CREATE_ARCHIVE:
                url.append("/archives");
                break;
            case DELETE_ARCHIVE:
                if (archiveId == null) {
                    throw new InternalException("archiveId required");
                }
                url.append("/archives/").append(archiveId);
                break;
            case LIST_JOBS: case CREATE_JOB:
                url.append("/jobs");
                break;
            case DESCRIBE_JOB: case GET_JOB_OUTPUT:
                if (jobId == null) {
                    throw new InternalException("jobId required");
                }
                url.append("/jobs/").append(jobId);
                if (action == GlacierAction.GET_JOB_OUTPUT) {
                    url.append("/output");
                }
                break;
        }
        return url.toString();
    }


    private static class ClientAndResponse {
        public final HttpClient client;
        public final HttpResponse response;

        private ClientAndResponse(HttpClient client, HttpResponse response) {
            this.client = client;
            this.response = response;
        }
    }

    public static Builder build(@Nonnull AWSCloud provider, @Nonnull GlacierAction action) {
        return new Builder(provider, action);
    }

    public static class Builder {
        private final AWSCloud provider;
        private final GlacierAction action;
        private String vaultId;
        private String archiveId;
        private String jobId;
        public Map<String, String> headers;
        public Map<String, String> queryParameters;
        public String bodyText;
        public File bodyFile;

        public Builder(@Nonnull AWSCloud provider, @Nonnull GlacierAction action) {
            this.provider = provider;
            this.action = action;
        }

        public Builder vaultId(@Nonnull String value) {
            vaultId = value;
            return this;
        }

        public Builder archiveId(@Nonnull String value) {
            archiveId = value;
            return this;
        }

        public Builder jobId(@Nonnull String value) {
            jobId = value;
            return this;
        }

        public Builder headers(@Nonnull Map<String, String> value) {
            headers = value;
            return this;
        }

        public Builder queryParameters(@Nonnull Map<String, String> value) {
            queryParameters = value;
            return this;
        }

        public Builder bodyText(@Nonnull String value) {
            bodyText = value;
            return this;
        }

        public Builder bodyFile(@Nonnull File value) {
            bodyFile = value;
            return this;
        }

        public GlacierMethod toMethod() {
            return new GlacierMethod(this);
        }
    }

}
