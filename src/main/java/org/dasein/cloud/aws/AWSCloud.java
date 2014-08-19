/**
 * Copyright (C) 2009-2013 Dell, Inc.
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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.http.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.cloud.aws.admin.AWSAdminServices;
import org.dasein.cloud.aws.compute.EC2ComputeServices;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.aws.compute.EC2Method;
import org.dasein.cloud.aws.identity.AWSIdentityServices;
import org.dasein.cloud.aws.network.EC2NetworkServices;
import org.dasein.cloud.aws.platform.AWSPlatformServices;
import org.dasein.cloud.aws.storage.AWSCloudStorageServices;
import org.dasein.cloud.compute.ComputeServices;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.platform.KeyValuePair;
import org.dasein.cloud.storage.BlobStoreSupport;
import org.dasein.cloud.storage.StorageServices;
import org.dasein.cloud.util.APITrace;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class AWSCloud extends AbstractCloud {
    static private String getLastItem( String name ) {
        int idx = name.lastIndexOf('.');

        if( idx < 0 ) {
            return name;
        }
        else if( idx == ( name.length() - 1 ) ) {
            return "";
        }
        return name.substring(idx + 1);
    }

    static public Logger getLogger( Class<?> cls ) {
        String pkg = getLastItem(cls.getPackage().getName());

        if( pkg.equals("aws") ) {
            pkg = "";
        }
        else {
            pkg = pkg + ".";
        }
        return Logger.getLogger("dasein.cloud.aws.std." + pkg + getLastItem(cls.getName()));
    }

    static public Logger getWireLogger( Class<?> cls ) {
        return Logger.getLogger("dasein.cloud.aws.wire." + getLastItem(cls.getPackage().getName()) + "." + getLastItem(cls.getName()));
    }

    static private final Logger logger = getLogger(AWSCloud.class);

    static public final String P_ACCESS = "AWSAccessKeyId";
    static public final String P_ACTION = "Action";
    static public final String P_CFAUTH = "Authorization";
    static public final String P_AWS_DATE = "x-amz-date";
    static public final String P_GOOG_DATE = "x-goog-date";
    static public final String P_SIGNATURE = "Signature";
    static public final String P_SIGNATURE_METHOD = "SignatureMethod";
    static public final String P_SIGNATURE_VERSION = "SignatureVersion";
    static public final String P_TIMESTAMP = "Timestamp";
    static public final String P_VERSION = "Version";

    static public final String CLOUD_FRONT_ALGORITHM = "HmacSHA1";
    static public final String EC2_ALGORITHM = "HmacSHA256";
    static public final String S3_ALGORITHM = "HmacSHA1";
    static public final String SIGNATURE = "2";
    static public final String V4_ALGORITHM = "AWS4-HMAC-SHA256";
    static public final String V4_TERMINATION = "aws4_request";

    static public final String PLATFORM_EC2                     = "EC2";
    static public final String PLATFORM_VPC                     = "VPC";


    static public @Nonnull String encode( @Nonnull String value, boolean encodePath ) throws InternalException {
        String encoded;

        try {
            encoded = URLEncoder.encode(value, "utf-8").replace("+", "%20").replace("*", "%2A").replace("%7E", "~");
            if( encodePath ) {
                encoded = encoded.replace("%2F", "/");
            }
        } catch( UnsupportedEncodingException e ) {
            logger.error(e);
            e.printStackTrace();
            throw new InternalException(e);
        }
        return encoded;
    }

    static public String escapeXml( String nonxml ) {
        StringBuilder str = new StringBuilder();

        for( int i = 0; i < nonxml.length(); i++ ) {
            char c = nonxml.charAt(i);

            switch( c ) {
                case '&':
                    str.append("&amp;");
                    break;
                case '>':
                    str.append("&gt;");
                    break;
                case '<':
                    str.append("&lt;");
                    break;
                case '"':
                    str.append("&quot;");
                    break;
                case '[':
                    str.append("&#091;");
                    break;
                case ']':
                    str.append("&#093;");
                    break;
                case '!':
                    str.append("&#033;");
                    break;
                default:
                    str.append(c);
            }
        }
        return str.toString();
    }

    static public byte[] HmacSHA256( String data, byte[] key ) throws InternalException {

        final String algorithm = "HmacSHA256";
        Mac mac;
        try {
            mac = Mac.getInstance(algorithm);
            mac.init(new SecretKeySpec(key, algorithm));
            return mac.doFinal(data.getBytes("UTF-8"));
        } catch( NoSuchAlgorithmException e ) {
            throw new InternalException(e);
        } catch( InvalidKeyException e ) {
            throw new InternalException(e);
        } catch( UnsupportedEncodingException e ) {
            throw new InternalException(e);
        }
    }

    static public String computeSHA256Hash( String value ) throws InternalException {
        try {
            byte[] valueBytes = value.getBytes("utf-8");
            BufferedInputStream inputStream = new BufferedInputStream(new ByteArrayInputStream(valueBytes));
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[4096];
            int read;
            while( ( read = inputStream.read(buffer, 0, buffer.length) ) != -1 ) {
                digest.update(buffer, 0, read);
            }
            return new String(Hex.encodeHex(digest.digest(), true));
        } catch( NoSuchAlgorithmException e ) {
            throw new InternalException(e);
        } catch( IOException e ) {
            throw new InternalException(e);
        }
    }

    public AWSCloud() {
    }

    private String buildEc2AuthString( String method, String serviceUrl, Map<String, String> parameters ) throws InternalException {
        StringBuilder authString = new StringBuilder();
        TreeSet<String> sortedKeys;
        URI endpoint;
        String tmp;

        authString.append(method);
        authString.append("\n");
        try {
            endpoint = new URI(serviceUrl);
        } catch( URISyntaxException e ) {
            logger.error(e);
            e.printStackTrace();
            throw new InternalException(e);
        }
        authString.append(endpoint.getHost().toLowerCase());
        authString.append("\n");
        tmp = endpoint.getPath();
        if( tmp == null || tmp.length() == 0 ) {
            tmp = "/";
        }
        authString.append(encode(tmp, true));
        authString.append("\n");
        sortedKeys = new TreeSet<String>();
        sortedKeys.addAll(parameters.keySet());
        boolean first = true;
        for( String key : sortedKeys ) {
            String value = parameters.get(key);

            if( !first ) {
                authString.append("&");
            }
            else {
                first = false;
            }
            authString.append(encode(key, false));
            authString.append("=");
            if( value == null ) {
                value = "";
            }
            authString.append(encode(value, false));
        }
        return authString.toString();
    }


    public boolean createTags( String resourceId, Tag... keyValuePairs ) {
        return createTags(new String[]{resourceId}, keyValuePairs);
    }

    public boolean createTags( final String[] resourceIds, final Tag... keyValuePairs ) {
        hold();

        Thread t = new Thread() {
            public void run() {
                try {
                    createTags(1, resourceIds, keyValuePairs);
                } finally {
                    release();
                }
            }
        };

        t.setName("Tag Setter");
        t.setDaemon(true);
        t.start();
        return true;
    }

    private void createTags( int attempt, String[] resourceIds, Tag... keyValuePairs ) {
        APITrace.begin(this, "Cloud.createTags");
        try {
            try {
                Map<String, String> parameters = getStandardParameters(getContext(), "CreateTags");
                EC2Method method;

                for( int i = 0; i < resourceIds.length; i++ ) {
                    parameters.put("ResourceId." + ( i + 1 ), resourceIds[i]);
                }

                Map<String, String> tagParameters = new HashMap<String, String>();
                for( int i = 0; i < keyValuePairs.length; i++ ) {
                    String key = keyValuePairs[i].getKey();
                    String value = keyValuePairs[i].getValue();

                    if (value == null) {
                        value = "";
                    }
                    tagParameters.put("Tag." + (i + 1) + ".Key", key);
                    tagParameters.put("Tag." + (i + 1) + ".Value", value);
                }
                if( tagParameters.size() == 0 ) {
                    return;
                }
                addExtraParameters(parameters, tagParameters);
                method = new EC2Method(this, getEc2Url(), parameters);
                try {
                    method.invoke();
                } catch( EC2Exception e ) {
                    if( attempt > 20 ) {
                        logger.error("EC2 error settings tags for " + Arrays.toString(resourceIds) + ": " + e.getSummary());
                        return;
                    }
                    try {
                        Thread.sleep(5000L);
                    } catch( InterruptedException ignore ) {
                    }
                    createTags(attempt + 1, resourceIds, keyValuePairs);
                }
            } catch( Throwable ignore ) {
                logger.error("Error while creating tags for " + Arrays.toString(resourceIds) + ".", ignore);
            }
        } finally {
            APITrace.end();
        }
    }

    public boolean removeTags( String resourceId, Tag... keyValuePairs ) {
        return removeTags(new String[]{resourceId}, keyValuePairs);
    }

    public boolean removeTags( String[] resourceIds, Tag... keyValuePairs ) {
        APITrace.begin(this, "Cloud.removeTags");
        try {
            try {
                Map<String, String> parameters = getStandardParameters(getContext(), "DeleteTags");
                EC2Method method;

                for( int i = 0; i < resourceIds.length; i++ ) {
                    parameters.put("ResourceId." + ( i + 1 ), resourceIds[i]);
                }

                for( int i = 0; i < keyValuePairs.length; i++ ) {
                    String key = keyValuePairs[i].getKey();
                    String value = keyValuePairs[i].getValue();

                    parameters.put("Tag." + (i + 1) + ".Key", key);
                    if (value != null && value.length() > 0) {
                        parameters.put("Tag." + (i + 1) + ".Value", value);
                    }
                }
                method = new EC2Method(this, getEc2Url(), parameters);
                method.invoke();
                return true;
            } catch( Throwable ignore ) {
                logger.error("Error while removing tags for " + Arrays.toString(resourceIds) + ".", ignore);
                return false;
            }
        } finally {
            APITrace.end();
        }
    }

    public Map<String, String> getTagsFromTagSet( Node attr ) {
        if( attr == null || !attr.hasChildNodes() ) {
            return null;
        }
        Map<String, String> tags = new HashMap<String, String>();
        NodeList tagNodes = attr.getChildNodes();
        for( int j = 0; j < tagNodes.getLength(); j++ ) {
            Node tag = tagNodes.item(j);

            if( tag.getNodeName().equals("item") && tag.hasChildNodes() ) {
                NodeList parts = tag.getChildNodes();
                String key = null, value = null;

                for( int k = 0; k < parts.getLength(); k++ ) {
                    Node part = parts.item(k);

                    if( part.getNodeName().equalsIgnoreCase("key") ) {
                        if( part.hasChildNodes() ) {
                            key = part.getFirstChild().getNodeValue().trim();
                        }
                    }
                    else if( part.getNodeName().equalsIgnoreCase("value") ) {
                        if( part.hasChildNodes() ) {
                            value = part.getFirstChild().getNodeValue().trim();
                        }
                    }
                    if( key != null && value != null ) {
                        tags.put(key, value);
                    }
                }
            }
        }
        return tags;
    }

    @Override
    public AWSAdminServices getAdminServices() {
        EC2Provider p = getEC2Provider();

        if( p.isAWS() || p.isEnStratus() || p.isOpenStack() || p.isEucalyptus() ) {
            return new AWSAdminServices(this);
        }
        return null;
    }

    private @Nonnull String[] getBootstrapUrls( @Nullable ProviderContext ctx ) {
        String endpoint = ( ctx == null ? null : ctx.getCloud().getEndpoint() );

        if( endpoint == null ) {
            return new String[0];
        }
        if( !endpoint.contains(",") ) {
            return new String[]{endpoint};
        }
        String[] endpoints = endpoint.split(",");

        if( endpoints == null ) {
            endpoints = new String[0];
        }
        if( endpoints.length > 1 ) {
            String second = endpoints[1];

            if( !second.startsWith("http") ) {
                if( endpoints[0].startsWith("http") ) {
                    // likely a URL with a , in it
                    return new String[]{endpoint + ( getEC2Provider().isEucalyptus() ? "/Eucalyptus" : "" )};
                }
            }
        }
        for( int i = 0; i < endpoints.length; i++ ) {
            if( !endpoints[i].startsWith("http") ) {
                endpoints[i] = "https://" + endpoints[i] + ( getEC2Provider().isEucalyptus() ? "/Eucalyptus" : "" );
            }
        }
        return endpoints;
    }

    @Override
    public @Nonnull String getCloudName() {
        ProviderContext ctx = getContext();
        String name = ( ctx == null ? null : ctx.getCloud().getCloudName() );

        return ( ( name == null ) ? "AWS" : name );
    }

    @Override
    public EC2ComputeServices getComputeServices() {
        if( getEC2Provider().isStorage() ) {
            return null;
        }
        return new EC2ComputeServices(this);
    }

    static public final String DSN_ACCESS_KEY = "accessKey";

    @Override
    public @Nonnull ContextRequirements getContextRequirements() {
        return new ContextRequirements(
                new ContextRequirements.Field(DSN_ACCESS_KEY, "AWS API access keys", ContextRequirements.FieldType.KEYPAIR, ContextRequirements.Field.ACCESS_KEYS, true),
                new ContextRequirements.Field("proxyHost", "Proxy host", ContextRequirements.FieldType.TEXT, false),
                new ContextRequirements.Field("proxyPort", "Proxy port", ContextRequirements.FieldType.TEXT, false));
    }

    public byte[][] getAccessKey( ProviderContext ctx ) {
        return ( byte[][] ) ctx.getConfigurationValue(DSN_ACCESS_KEY);
    }

    @Override
    public @Nonnull RegionsAndZones getDataCenterServices() {
        return new RegionsAndZones(this);
    }

    private transient volatile EC2Provider provider;

    public @Nonnull EC2Provider getEC2Provider() {
        if( provider == null ) {
            provider = EC2Provider.valueOf(getProviderName());
        }
        return provider;
    }

    public @Nullable String getEc2Url() {
        ProviderContext ctx = getContext();
        String url = getEc2Url(ctx == null ? null : ctx.getRegionId());

        if( getEC2Provider().isEucalyptus() ) {
            return url + "/Eucalyptus";
        }
        else {
            return url;
        }
    }

    public @Nullable String getEc2Url( @Nullable String regionId ) {
        ProviderContext ctx = getContext();
        String url;

        if( regionId == null ) {
            return getBootstrapUrls(ctx)[0];
        }
        if( getEC2Provider().isAWS() ) {

            url = ( ctx == null ? null : ctx.getCloud().getEndpoint() );
            if( url != null && url.endsWith("amazonaws.com") ) {
                return "https://ec2." + regionId + ".amazonaws.com";
            }
            return "https://ec2." + regionId + ".amazonaws.com";
        }
        else if( !getEC2Provider().isEucalyptus() ) {
            url = ( ctx == null ? null : ctx.getCloud().getEndpoint() );
            if( url == null ) {
                return null;
            }
            if( !url.startsWith("http") ) {
                String cloudUrl = ctx.getCloud().getEndpoint();

                if( cloudUrl != null && cloudUrl.startsWith("http:") ) {
                    return "http://" + url + "/" + regionId;
                }
                return "https://" + url + "/" + regionId;
            }
            else {
                return url + "/" + regionId;
            }
        }
        url = ( ctx == null ? null : ctx.getCloud().getEndpoint() );
        if( url == null ) {
            return null;
        }
        if( !url.startsWith("http") ) {
            String cloudUrl = ctx.getCloud().getEndpoint();

            if( cloudUrl != null && cloudUrl.startsWith("http:") ) {
                return "http://" + url;
            }
            return "https://" + url;
        }
        else {
            return url;
        }
    }

    public String getGlacierUrl() throws InternalException, CloudException {
        ProviderContext ctx = getContext();
        String regionId = ctx.getRegionId();
        return "https://glacier." + regionId + ".amazonaws.com/-/";
    }

    public String getAutoScaleVersion() {
        return "2011-01-01";
    }

    public String getCloudWatchVersion() {
        return "2010-08-01";
    }

    public String getEc2Version() {
        if (getEC2Provider().isAWS()) {
            return "2014-05-01";
        }
        else if (getEC2Provider().isEucalyptus()) {
            return "2010-11-15";
        }
        else if( getEC2Provider().isOpenStack() ) {
            return "2009-11-30";
        }
        return "2012-07-20";
    }

    public String getElbVersion() {
        return "2012-06-01";
    }

    public String getRdsVersion() {
        return "2012-09-17";
    }

    public String getRoute53Version() {
        return "2012-12-12";
    }

    public String getSdbVersion() {
        return "2009-04-15";
    }

    public String getSnsVersion() {
        return "2010-03-31";
    }

    public String getSqsVersion() {
        return "2009-02-01";
    }

    @Override
    public AWSIdentityServices getIdentityServices() {
        if( getEC2Provider().isStorage() ) {
            return null;
        }
        return new AWSIdentityServices(this);
    }

    @Override
    public EC2NetworkServices getNetworkServices() {
        if( getEC2Provider().isStorage() ) {
            return null;
        }
        return new EC2NetworkServices(this);
    }

    @Override
    public @Nullable AWSPlatformServices getPlatformServices() {
        EC2Provider p = getEC2Provider();

        if( p.isAWS() || p.isEnStratus() ) {
            return new AWSPlatformServices(this);
        }
        return null;
    }

    @Override
    public @Nonnull String getProviderName() {
        ProviderContext ctx = getContext();
        String name = ( ctx == null ? null : ctx.getCloud().getProviderName() );

        return ( ( name == null ) ? EC2Provider.AWS.getName() : name );
    }

    public @Nullable String getProxyHost() {
        ProviderContext ctx = getContext();

        if( ctx == null ) {
            return null;
        }
        Properties props = ctx.getCustomProperties();

        return ( props == null ? null : props.getProperty("proxyHost") );
    }

    public int getProxyPort() {
        ProviderContext ctx = getContext();

        if( ctx == null ) {
            return -1;
        }
        Properties props = ctx.getCustomProperties();

        if( props == null ) {
            return -1;
        }
        String port = props.getProperty("proxyPort");

        if( port != null ) {
            return Integer.parseInt(port);
        }
        return -1;
    }

    @Override
    public @Nonnull AWSCloudStorageServices getStorageServices() {
        return new AWSCloudStorageServices(this);
    }

    public Map<String, String> getStandardParameters( ProviderContext ctx, String action ) throws InternalException {
        return getStandardParameters(ctx, action, getEc2Version());
    }

    public Map<String, String> getStandardParameters( ProviderContext ctx, String action, String version ) throws InternalException {
        HashMap<String, String> parameters = new HashMap<String, String>();

        parameters.put(P_ACTION, action);
        parameters.put(P_SIGNATURE_VERSION, SIGNATURE);
        try {
            byte[][] keys = getAccessKey(ctx);

            parameters.put(P_ACCESS, new String(keys[0], "utf-8"));
        } catch( UnsupportedEncodingException e ) {
            logger.error(e);
            e.printStackTrace();
            throw new InternalException(e);
        }
        parameters.put(P_SIGNATURE_METHOD, EC2_ALGORITHM);
        parameters.put(P_TIMESTAMP, getTimestamp(System.currentTimeMillis(), true));
        parameters.put(P_VERSION, version);
        return parameters;
    }

    public Map<String, String> getStandardCloudWatchParameters( ProviderContext ctx, String action ) throws InternalException {
        Map<String, String> parameters = getStandardParameters(ctx, action);

        parameters.put(P_VERSION, getCloudWatchVersion());
        return parameters;
    }

    public Map<String, String> getStandardRdsParameters( ProviderContext ctx, String action ) throws InternalException {
        Map<String, String> parameters = getStandardParameters(ctx, action);

        parameters.put(P_VERSION, getRdsVersion());
        return parameters;
    }

    public Map<String, String> getStandardSimpleDBParameters( ProviderContext ctx, String action ) throws InternalException {
        Map<String, String> parameters = getStandardParameters(ctx, action);

        parameters.put(P_VERSION, getSdbVersion());
        return parameters;
    }

    public Map<String, String> getStandardSnsParameters( ProviderContext ctx, String action ) throws InternalException {
        Map<String, String> parameters = getStandardParameters(ctx, action);

        parameters.put(P_VERSION, getSnsVersion());
        return parameters;
    }

    public Map<String, String> getStandardSqsParameters( ProviderContext ctx, String action ) throws InternalException {
        Map<String, String> parameters = getStandardParameters(ctx, action);

        parameters.put(P_VERSION, getSqsVersion());
        return parameters;
    }

    public static void addExtraParameters( Map<String, String> parameters, Map<String, String> extraParameters ) {
        if( extraParameters == null || extraParameters.size() == 0 ) {
            return;
        }
        if( parameters == null ) {
            parameters = new HashMap<String, String>();
        }
        parameters.putAll(extraParameters);
    }

    public static @Nullable Map<String, String> getTagFilterParams( @Nullable Map<String, String> tags ) {
        return getTagFilterParams(tags, 1);
    }

    public static @Nullable Map<String, String> getTagFilterParams( @Nullable Map<String, String> tags, int startingFilterIndex ) {
        if( tags == null || tags.size() == 0 ) {
            return null;
        }

        Map<String, String> filterParameters = new HashMap<String, String>();
        int i = startingFilterIndex;

        for (Map.Entry<String, String> parameter : tags.entrySet()) {
            addFilterParameters(filterParameters, i, "tag:" + parameter.getKey(), Collections.singletonList(parameter.getValue()));
            i++;
        }
        return filterParameters;
    }

    public static void addFilterParameters(Map<String, String> filterParameters, int index, String filterName, Collection<?> filterValues) {
        if (filterValues == null || filterValues.isEmpty()) {
            return;
        }

        filterParameters.put("Filter." + index + ".Name", filterName);
        int valueIndex = 0;
        for (Object filterValue : filterValues) {
            // filter values must be in lower case
            filterParameters.put("Filter." + index + ".Value." + valueIndex++, filterValue.toString().toLowerCase());
        }
    }

    public static void addFilterParameters(Map<String, String> filterParameters, int index, String filterName, Object ... filterValues) {
        if (filterValues == null || filterValues.length == 0) {
            return;
        }

        filterParameters.put("Filter." + index + ".Name", filterName);
        int valueIndex = 0;
        for (Object filterValue : filterValues) {
            // filter values must be in lower case
            filterParameters.put("Filter." + index + ".Value." + valueIndex++, filterValue.toString().toLowerCase());
        }
    }

    public @Nonnull String getTimestamp(long timestamp, boolean withMillis) {
        SimpleDateFormat fmt;

        if( withMillis ) {
            fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        }
        else {
            fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        }
        fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        return fmt.format(new Date(timestamp));
    }

    public long parseTime( @Nullable String time ) throws CloudException {
        if( time == null ) {
            return 0L;
        }
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

        if( time.length() > 0 ) {
            try {
                return fmt.parse(time).getTime();
            } catch( ParseException e ) {
                fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                try {
                    return fmt.parse(time).getTime();
                } catch( ParseException encore ) {
                    throw new CloudException("Could not parse date: " + time);
                }
            }
        }
        return 0L;
    }

    private String sign( byte[] key, String authString, String algorithm ) throws InternalException {
        try {
            Mac mac = Mac.getInstance(algorithm);

            mac.init(new SecretKeySpec(key, algorithm));
            return new String(Base64.encodeBase64(mac.doFinal(authString.getBytes("utf-8"))));
        } catch( NoSuchAlgorithmException e ) {
            logger.error(e);
            e.printStackTrace();
            throw new InternalException(e);
        } catch( InvalidKeyException e ) {
            logger.error(e);
            e.printStackTrace();
            throw new InternalException(e);
        } catch( IllegalStateException e ) {
            logger.error(e);
            e.printStackTrace();
            throw new InternalException(e);
        } catch( UnsupportedEncodingException e ) {
            logger.error(e);
            e.printStackTrace();
            throw new InternalException(e);
        }
    }

    public String signUploadPolicy( String base64Policy ) throws InternalException {
        ProviderContext ctx = getContext();

        if( ctx == null ) {
            throw new InternalException("No context for signing the request");
        }
        return sign(getAccessKey(ctx)[1], base64Policy, S3_ALGORITHM);
    }

    public String signCloudFront( String accessKey, byte[] secretKey, String dateString ) throws InternalException {
        String signature = sign(secretKey, dateString, CLOUD_FRONT_ALGORITHM);

        if( getEC2Provider().isStorage() && "google".equalsIgnoreCase(getProviderName()) ) {
            return ( "GOOG1 " + accessKey + ":" + signature );
        }
        else {
            return ( "AWS " + accessKey + ":" + signature );
        }
    }

    public String signEc2( byte[] key, String serviceUrl, Map<String, String> parameters ) throws InternalException {
        return sign(key, buildEc2AuthString("POST", serviceUrl, parameters), EC2_ALGORITHM);
    }

    public String signAWS3( String keyId, byte[] key, String dateString ) throws InternalException {
        return ( "AWS3-HTTPS AWSAccessKeyId=" + keyId + ",Algorithm=" + EC2_ALGORITHM + ",Signature=" + sign(key, dateString, EC2_ALGORITHM) );
    }

    public String signS3( String accessKey, byte[] secretKey, String action, String hash, String contentType, Map<String, String> headers, String bucket, String object ) throws InternalException {
        StringBuilder toSign = new StringBuilder();

        toSign.append(action);
        toSign.append("\n");
        if( hash != null ) {
            toSign.append(hash);
        }
        toSign.append("\n");
        if( contentType != null ) {
            toSign.append(contentType);
        }
        toSign.append("\n\n");
        ArrayList<String> keys = new ArrayList<String>();
        keys.addAll(headers.keySet());
        Collections.sort(keys);
        for( String hkey : keys ) {
            if( hkey.startsWith("x-amz") || ( getEC2Provider().isStorage() && hkey.startsWith("x-goog") ) ) {
                String val = headers.get(hkey);

                if( val != null ) {
                    toSign.append(hkey);
                    toSign.append(":");
                    toSign.append(headers.get(hkey).trim());
                    toSign.append("\n");
                }
            }
        }
        toSign.append("/");
        if( getEC2Provider().isEucalyptus() ) {
            toSign.append("services/Walrus/");
        }
        if( bucket != null ) {
            toSign.append(bucket);
            toSign.append("/");
        }
        if( object != null ) {
            toSign.append(object.toLowerCase());
        }
        String signature = sign(secretKey, toSign.toString(), S3_ALGORITHM);

        if( getEC2Provider().isStorage() && "google".equalsIgnoreCase(getProviderName()) ) {
            return ( "GOOG1 " + accessKey + ":" + signature );
        }
        else {
            return ( "AWS " + accessKey + ":" + signature );
        }
    }

    /**
     * Generates an AWS v4 signature authorization string
     *
     * @param accessKey Amazon credential
     * @param secretKey Amazon credential
     * @param action    the HTTP method (GET, POST, etc)
     * @param url       the full URL for the request, including any query parameters
     * @param serviceId the canonical name of the service targeted in the request (e.g. "glacier")
     * @param headers   map of headers of request. MUST include x-amz-date or date header.
     * @param bodyHash  a hex-encoded sha256 hash of the body of the request
     * @return a string suitable for including as the HTTP Authorization header
     * @throws InternalException
     */
    public String getV4Authorization( String accessKey, String secretKey, String action, String url, String serviceId, Map<String, String> headers, String bodyHash ) throws InternalException {

        ProviderContext ctx = getContext();
        if( ctx == null || ctx.getRegionId() == null ) {
            throw new InternalException("no region is configured");
        }
        String regionId = ctx.getRegionId();

        serviceId = serviceId.toLowerCase();

        String amzDate = extractV4Date(headers);
        String credentialScope = getV4CredentialScope(amzDate, regionId, serviceId);
        String signedHeaders = getV4SignedHeaders(headers);
        String signature = signV4(secretKey, action, url, regionId, serviceId, headers, bodyHash);

        return V4_ALGORITHM + " " + "Credential=" + accessKey + "/" + credentialScope + ", " + "SignedHeaders=" + signedHeaders + ", " + "Signature=" + signature;
    }

    private String signV4( String secretKey, String action, String serviceUrl, String regionId, String serviceId, Map<String, String> headers, String bodyHash ) throws InternalException {
        final String canonicalRequest = getV4CanonicalRequest(action, serviceUrl, headers, bodyHash);

        String amzDate = extractV4Date(headers);
        final String stringToSign = getV4StringToSign(amzDate, regionId, serviceId, canonicalRequest);

        // signature uses YYYYMMDD
        String dateStamp = amzDate.substring(0, 8);
        final byte[] signingKey = getV4SigningKey(secretKey, dateStamp, regionId, serviceId);

        return new String(Hex.encodeHex(HmacSHA256(stringToSign, signingKey), true));
    }

    private String extractV4Date( Map<String, String> headers ) throws InternalException {

        Map<String, String> lower = new HashMap<String, String>();
        for( Map.Entry<String, String> entry : headers.entrySet() ) {
            lower.put(entry.getKey().toLowerCase(), entry.getValue());
        }
        String amzDate = headers.get(P_AWS_DATE);
        // expecting YYYYMMDDTHHMMSSZ
        if( amzDate != null ) {
            if( amzDate.length() != 16 ) {
                throw new InternalException("request has invalid " + P_AWS_DATE);
            }
            return amzDate;
        }

        String date = lower.get("date");
        if( date == null ) {
            throw new InternalException("request is missing date header");
        }
        SimpleDateFormat parser = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");
        try {
            return getV4HeaderDate(parser.parse(date));

        } catch( ParseException e ) {
            throw new InternalException("request has invalid date header format");
        }
    }

    private byte[] getV4SigningKey( String secretKey, String dateStamp, String regionId, String serviceId ) throws InternalException {
        byte[] withSecret = ( "AWS4" + secretKey ).getBytes();
        byte[] withDate = HmacSHA256(dateStamp, withSecret);
        byte[] withRegion = HmacSHA256(regionId, withDate);
        byte[] withService = HmacSHA256(serviceId, withRegion);
        return HmacSHA256("aws4_request", withService);
    }

    private String getV4StringToSign( String dateStamp, String regionId, String serviceId, String canonicalRequest ) throws InternalException {
        return V4_ALGORITHM + "\n" + dateStamp + "\n" + getV4CredentialScope(dateStamp, regionId, serviceId) + "\n" + computeSHA256Hash(canonicalRequest);
    }

    private String getV4CredentialScope( String dateStamp, String regionId, String serviceId ) {
        return dateStamp.substring(0, 8) + "/" + regionId + "/" + serviceId + "/" + V4_TERMINATION;
    }

    private String getV4CanonicalRequest( String action, String serviceUrl, Map<String, String> headers, String bodyHash ) throws InternalException {
    /*
      CanonicalRequest =
      HTTPRequestMethod + '\n' +
      CanonicalURI + '\n' +
      CanonicalQueryString + '\n' +
      CanonicalHeaders + '\n' +
      SignedHeaders + '\n' +
      HexEncode(Hash(Payload))
     */

        final URI endpoint;
        try {
            endpoint = new URI(serviceUrl.replace(" ", "%20")).normalize();
        } catch( URISyntaxException e ) {
            throw new InternalException(e);
        }

        final StringBuilder s = new StringBuilder();
        s.append(action.toUpperCase()).append("\n");


        String path = endpoint.getPath();
        if( path == null || path.length() == 0 ) {
            path = "/";
        }
        s.append(encode(path, true)).append("\n");
        s.append(getV4CanonicalQueryString(endpoint)).append("\n");

        List<String> sortedHeaders = new ArrayList<String>();
        sortedHeaders.addAll(headers.keySet());
        Collections.sort(sortedHeaders, String.CASE_INSENSITIVE_ORDER);

        for( String header : sortedHeaders ) {
            String value = headers.get(header).trim().replaceAll("\\s+", " ");
            header = header.toLowerCase().replaceAll("\\s+", " ");
            s.append(header).append(":").append(value).append("\n");
        }
        s.append("\n").append(getV4SignedHeaders(headers)).append("\n").append(bodyHash);

        return s.toString();
    }

    private String getV4CanonicalQueryString( URI endpoint ) throws InternalException {
        // parse query params and translate to another form of tuple that is comparable on both key and value

        List<NameValuePair> parsedParams = URLEncodedUtils.parse(endpoint, "UTF-8");
        List<KeyValuePair> queryParams = new ArrayList<KeyValuePair>(parsedParams.size());
        for( NameValuePair param : parsedParams ) {
            String key = encode(param.getName(), false);
            String value = param.getValue() != null ? encode(param.getValue(), false) : "";
            queryParams.add(new KeyValuePair(key, value));
        }

        // sort query parameters by key, then value
        Collections.sort(queryParams);

        StringBuilder sb = new StringBuilder();
        for( KeyValuePair pair : queryParams ) {
            if( sb.length() > 0 ) {
                sb.append("&");
            }
            sb.append(pair.getKey()).append("=").append(pair.getValue());
        }
        return sb.toString();
    }

    private String getV4SignedHeaders( Map<String, String> headers ) {
        // move to set to lower case and remove dupes
        Set<String> sorted = new TreeSet<String>();
        for( String header : headers.keySet() ) {
            sorted.add(header.toLowerCase());
        }

        StringBuilder sb = new StringBuilder();
        for( String header : sorted ) {
            if( sb.length() > 0 ) {
                sb.append(";");
            }
            sb.append(header.toLowerCase());
        }

        return sb.toString();
    }

    public String getV4HeaderDate( Date date ) {
        SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
        Calendar cal = Calendar.getInstance(new SimpleTimeZone(0, "GMT"));
        fmt.setCalendar(cal);
        if( date == null ) {
            return fmt.format(new Date());
        }
        else {
            return fmt.format(date);
        }
    }

    @Override
    public String testContext() {
        APITrace.begin(this, "Cloud.testContext");
        try {
            ProviderContext ctx = getContext();

            if( ctx == null ) {
                logger.warn("No context exists for testing");
                return null;
            }
            try {
                ComputeServices compute = getComputeServices();

                if( compute != null ) {
                    VirtualMachineSupport support = compute.getVirtualMachineSupport();

                    if( support == null || !support.isSubscribed() ) {
                        logger.warn("Not subscribed to virtual machine support");
                        return null;
                    }
                    String actualAccountNumber = getOwnerId();
                    // Return actual account number as the number provided in configuration
                    // may have been incorrect
                    if( actualAccountNumber != null ) {
                        return actualAccountNumber;
                    }
                }
                else {
                    StorageServices storage = getStorageServices();
                    BlobStoreSupport support = storage.getOnlineStorageSupport();

                    if( support == null || !support.isSubscribed() ) {
                        logger.warn("No subscribed to storage services");
                        return null;
                    }
                }
            } catch( Throwable t ) {
                logger.warn("Unable to connect to AWS for " + ctx.getAccountNumber() + ": " + t.getMessage());
                return null;
            }
            return ctx.getAccountNumber();
        } finally {
            APITrace.end();
        }
    }

    /**
     * Retrieve current account number using DescribeSecurityGroups. May not always be reliable but is better than
     * nothing.
     *
     * @return current account number or null if not found
     */
    private String getOwnerId() {
        APITrace.begin(this, "AWSCloud.getOwnerId");
        try {
            ProviderContext ctx = getContext();

            if( ctx == null ) {
                return null;
            }
            Map<String, String> parameters = getStandardParameters(getContext(), EC2Method.DESCRIBE_SECURITY_GROUPS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            method = new EC2Method(this, getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("securityGroupInfo");
            for( int i = 0; i < blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j = 0; j < items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("item") ) {
                        NodeList attrs = item.getChildNodes();
                        for( int k = 0; k < attrs.getLength(); k++ ) {
                            Node attr = attrs.item(k);
                            if( attr.getNodeName().equals("ownerId") ) {
                                return attr.getFirstChild().getNodeValue().trim();
                            }
                        }
                    }
                }
            }
            return null;
        } catch( InternalException e ) {
        } catch( CloudException e ) {
        } finally {
            APITrace.end();
        }
        // Couldn't get the number for some reason
        return null;
    }

    public void setTags( @Nonnull Node attr, @Nonnull Taggable item ) {
        if( attr.hasChildNodes() ) {
            NodeList tags = attr.getChildNodes();

            for( int j = 0; j < tags.getLength(); j++ ) {
                Node tag = tags.item(j);

                if( tag.getNodeName().equals("item") && tag.hasChildNodes() ) {
                    NodeList parts = tag.getChildNodes();
                    String key = null, value = null;

                    for( int k = 0; k < parts.getLength(); k++ ) {
                        Node part = parts.item(k);

                        if( part.getNodeName().equalsIgnoreCase("key") ) {
                            if( part.hasChildNodes() ) {
                                key = part.getFirstChild().getNodeValue().trim();
                            }
                        }
                        else if( part.getNodeName().equalsIgnoreCase("value") ) {
                            if( part.hasChildNodes() ) {
                                value = part.getFirstChild().getNodeValue().trim();
                            }
                        }
                    }
                    if( key != null && value != null ) {
                        item.setTag(key, value);
                    }
                }
            }
        }
    }

    /**
     * Gets the epoch form of the text value of the provided node.
     *
     * @param node the node to extact the value from
     * @return the epoch time
     * @throws CloudException
     */
    public static long getTimestampValue( Node node ) throws CloudException {
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        String value = getTextValue(node);

        try {
            return fmt.parse(value).getTime();
        } catch( ParseException e ) {
            logger.error(e);
            e.printStackTrace();
            throw new CloudException(e);
        }
    }

    /**
     * Returns the text from the given node.
     *
     * @param node the node to extract the value from
     * @return the text from the node
     */
    static public String getTextValue( Node node ) {
        if( node.getChildNodes().getLength() == 0 ) {
            return null;
        }
        return node.getFirstChild().getNodeValue();
    }

    /**
     * Returns the boolean value of the given node.
     *
     * @param node the node to extract the value from
     * @return the boolean value of the node
     */
    static public boolean getBooleanValue( Node node ) {
        return Boolean.valueOf(getTextValue(node));
    }

    /**
     * Returns the int value of the given node.
     *
     * @param node the node to extract the value from
     * @return the int value of the given node
     */
    public static int getIntValue( Node node ) {
        return Integer.valueOf(getTextValue(node));
    }

    /**
     * Returns the double value of the given node.
     *
     * @param node the node to extract the value from
     * @return the double value of the given node
     */
    public static double getDoubleValue( Node node ) {
        return Double.valueOf(getTextValue(node));
    }

    /**
     * Returns the float value of the given node.
     *
     * @param node the node to extract the value from
     * @return the float value of the given node
     */
    public static float getFloatValue( Node node ) {
        return Float.valueOf(getTextValue(node));
    }

    /**
     * Helper method for adding indexed member parameters, e.g. <i>AlarmNames.member.N</i>. Will overwrite existing
     * parameters if present. Assumes indexing starts at 1.
     *
     * @param parameters the existing parameters map to add to
     * @param prefix     the prefix value for each parameter key
     * @param values     the values to add
     */
    public static void addIndexedParameters( @Nonnull Map<String, String> parameters, @Nonnull String prefix, String ... values ) {
        if( values == null || values.length == 0 ) {
            return;
        }
        int i = 1;
        if( !prefix.endsWith(".") ) {
            prefix += ".";
        }
        for( String value : values ) {
            parameters.put(String.format("%s%d", prefix, i), value);
            i++;
        }
    }

    /**
     * Helper method for adding indexed member parameters, e.g. <i>AlarmNames.member.N</i>. Will overwrite existing
     * parameters if present. Assumes indexing starts at 1.
     *
     * @param parameters      the existing parameters map to add to
     * @param prefix          the prefix value for each parameter key
     * @param extraParameters the values to add
     */
    public static void addIndexedParameters( @Nonnull Map<String, String> parameters, @Nonnull String prefix, Map<String, String> extraParameters ) {
        if( extraParameters == null || extraParameters.size() == 0 ) {
            return;
        }
        int i = 1;
        for( Map.Entry<String, String> entry : extraParameters.entrySet() ) {
            parameters.put(prefix + i + ".Name", entry.getKey());
            if( entry.getValue() != null ) {
                parameters.put(prefix + i + ".Value", entry.getValue());
            }
            i++;
        }
    }

    /**
     * Puts the given key/value into the given map only if the value is not null.
     *
     * @param parameters the map to add to
     * @param key        the key of the value
     * @param value      the value to add if not null
     */
    public static void addValueIfNotNull( @Nonnull Map<String, String> parameters, @Nonnull String key, String value ) {
        if( value == null ) {
            return;
        }
        parameters.put(key, value);
    }

    private static volatile Boolean supportsEC2 = null;
    private static volatile Boolean supportsVPC = null;

    /**
     * Retrieve current account number using DescribeSecurityGroups. May not always be reliable but is better than
     * nothing.
     *
     * @return current account number or null if not found
     */
    private void fetchSupportedPlatforms() {
        if( supportsEC2 != null ) {
            // We've already done this before, don't continue;
            return;
        }
        APITrace.begin(this, "AWSCloud.getSupportedPlatforms");
        try {
            ProviderContext ctx = getContext();
            if( ctx == null ) {
                return;
            }
            Map<String, String> parameters = getStandardParameters(getContext(), EC2Method.DESCRIBE_ACCOUNT_ATTRIBUTES);
            EC2Method method;
            NodeList blocks;
            Document doc;
            parameters.put("AttributeName.1", "supported-platforms");
            method = new EC2Method(this, getEc2Url(), parameters);
            try {
                doc = method.invoke();
            } catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }

            blocks = doc.getElementsByTagName("attributeValueSet");
            for (int i = 0; i < blocks.getLength(); i++) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j = 0; j < items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("item") ) {
                        NodeList attrs = item.getChildNodes();
                        for( int k = 0; k < attrs.getLength(); k++ ) {
                            Node attr = attrs.item(k);
                            if (attr.getNodeName().equals("attributeValue")) {
                                String value = attr.getFirstChild().getNodeValue().trim();
                                if( PLATFORM_EC2.equalsIgnoreCase(value) ) {
                                    supportsEC2 = Boolean.TRUE;
                                }
                                else if( PLATFORM_VPC.equalsIgnoreCase(value) ) {
                                    supportsVPC = Boolean.TRUE;
                                }
                            }
                        }
                    }
                }
            }
            if( supportsEC2 == null ) {
                supportsEC2 = Boolean.FALSE;
            }
            if( supportsVPC == null ) {
                supportsVPC = Boolean.FALSE;
            }
        } catch ( InternalException e ) {
        } catch ( CloudException e ) {
        } finally {
            APITrace.end();
        }
    }

    /**
     * @return
     */
    public boolean isEC2Supported() {
        fetchSupportedPlatforms();
        return supportsEC2 != null && supportsEC2;
    }

    /**
     *
     * @return
     */
    public boolean isVPCSupported() {
        fetchSupportedPlatforms();
        return supportsVPC != null && supportsVPC;
    }

    public @Nonnull HttpClient getClient() throws InternalException {
        return getClient(false);
    }

    public @Nonnull HttpClient getClient(boolean multipart) throws InternalException {
        ProviderContext ctx = getContext();
        if( ctx == null ) {
            throw new InternalException("No context was specified for this request");
        }

        final HttpParams params = new BasicHttpParams();

        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        if( !multipart ) {
            HttpProtocolParams.setContentCharset(params, Consts.UTF_8.toString());
        }
        HttpProtocolParams.setUserAgent(params, "Dasein Cloud");

        Properties p = ctx.getCustomProperties();
        if( p != null ) {
            String proxyHost = p.getProperty("proxyHost");
            String proxyPortStr = p.getProperty("proxyPort");
            int proxyPort = 0;
            if( proxyPortStr != null ) {
                proxyPort = Integer.parseInt(proxyPortStr);
            }
            if( proxyHost != null && proxyHost.length() > 0 && proxyPort > 0 ) {
                params.setParameter(ConnRoutePNames.DEFAULT_PROXY,
                        new HttpHost(proxyHost, proxyPort)
                );
            }
        }
        DefaultHttpClient client = new DefaultHttpClient(params);
        client.addRequestInterceptor(new HttpRequestInterceptor() {
            public void process(
                    final HttpRequest request,
                    final HttpContext context) throws HttpException, IOException {
                if( !request.containsHeader("Accept-Encoding") ) {
                    request.addHeader("Accept-Encoding", "gzip");
                }
                request.setParams(params);
            }
        });
        client.addResponseInterceptor(new HttpResponseInterceptor() {
            public void process(
                    final HttpResponse response,
                    final HttpContext context) throws HttpException, IOException {
                HttpEntity entity = response.getEntity();
                if( entity != null ) {
                    Header header = entity.getContentEncoding();
                    if( header != null ) {
                        for( HeaderElement codec : header.getElements() ) {
                            if( codec.getName().equalsIgnoreCase("gzip") ) {
                                response.setEntity(
                                        new GzipDecompressingEntity(response.getEntity()));
                                break;
                            }
                        }
                    }
                }
            }
        });
        return client;
    }

}
