package org.dasein.cloud.aws.storage;

import org.apache.http.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.*;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
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
import java.util.Properties;

/**
 * [Class Documentation]
 * <p>Created by George Reese: 5/22/13 2:46 PM</p>
 *
 * @author George Reese
 */
public class GlacierMethod {
    static private final Logger logger = Logger.getLogger(S3Method.class);

    static public final String GLACIER_PREFIX    = "glacier:";
    static public final String SERVICE_ID        = "glacier";
    static public final String API_VERSION       = "2012-06-01";

    private GlacierAction action           = null;
    private Map<String,String> headers     = null;
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
        String content;
        try {
            HttpResponse response = invokeInternal();
            Header contentType = response.getFirstHeader("content-type");
            if (!"application/json".equalsIgnoreCase(contentType.getValue())) {
                throw new CloudException("Invalid Glacier response: expected JSON");
            }
            content = EntityUtils.toString(response.getEntity());
            if (content == null) {
                return null;
            }
            return new JSONObject(content);

        } catch (IOException e) {
            throw new CloudException(e);
        } catch (JSONException e) {
            throw new CloudException(e);
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
        HttpResponse response = invokeInternal();
        Map<String, String> headers = new HashMap<String, String>();
        // doesn't support duplicate header keys, but they are unused by glacier
        for (Header header : response.getAllHeaders()) {
            headers.put(header.getName().toLowerCase(), header.getValue());
        }
        return headers;
    }

    /**
     * Invokes the method and returns nothing
     * @throws InternalException
     * @throws CloudException
     */
    public void invoke() throws InternalException, CloudException {
        invokeInternal();
    }

    private HttpResponse invokeInternal() throws InternalException, CloudException {

        if( wire.isDebugEnabled() ) {
            wire.debug("");
            wire.debug("----------------------------------------------------------------------------------");
        }
        try {
            final String url = getUrl();
            final String host;
            try {
                host = new URI(url).getHost();
            } catch (URISyntaxException e) {
                throw new InternalException(e);
            }
            final HttpRequestBase method = action.getMethod(url);

            final HttpClient client = getClient(url);

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
                logger.error(url + ": " + e.getMessage());
                throw new InternalException(e);
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
                return httpResponse;
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


    protected @Nonnull
    HttpClient getClient(String url) throws InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new InternalException("No context was specified for this request");
        }
        boolean ssl = url.startsWith("https");
        HttpParams params = new BasicHttpParams();

        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        //noinspection deprecation
        HttpProtocolParams.setContentCharset(params, HTTP.UTF_8);
        HttpProtocolParams.setUserAgent(params, "Dasein Cloud");

        Properties p = ctx.getCustomProperties();

        if( p != null ) {
            String proxyHost = p.getProperty("proxyHost");
            String proxyPort = p.getProperty("proxyPort");

            if( proxyHost != null ) {
                int port = 0;

                if( proxyPort != null && proxyPort.length() > 0 ) {
                    port = Integer.parseInt(proxyPort);
                }
                params.setParameter(ConnRoutePNames.DEFAULT_PROXY, new HttpHost(proxyHost, port, ssl ? "https" : "http"));
            }
        }
        return new DefaultHttpClient(params);
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
