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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nonnull;

import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.aws.compute.EC2Method;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.platform.KeyValueDatabase;
import org.dasein.cloud.platform.KeyValueDatabaseCapabilities;
import org.dasein.cloud.platform.KeyValueDatabaseSupport;
import org.dasein.cloud.platform.KeyValuePair;
import org.dasein.cloud.util.APITrace;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class SimpleDB implements KeyValueDatabaseSupport {
    static private final Logger logger = AWSCloud.getLogger(SimpleDB.class);

    static public final String SERVICE_ID        = "sdb";
    
    static public final String CREATE_DOMAIN     = "CreateDomain";
    static public final String DELETE_ATTRIBUTES = "DeleteAttributes";
    static public final String DELETE_DOMAIN     = "DeleteDomain";
    static public final String DOMAIN_META_DATA  = "DomainMetadata";
    static public final String GET_ATTRIBUTES    = "GetAttributes";
    static public final String LIST_DOMAINS      = "ListDomains";
    static public final String PUT_ATTRIBUTES    = "PutAttributes";
    static public final String SELECT            = "Select";
    private volatile transient SimpleDBCapabilities capabilities;

    static public @Nonnull ServiceAction[] asSimpleDBServiceAction( @Nonnull String action ) {
        if( action.equals(CREATE_DOMAIN) ) {
            return new ServiceAction[]{KeyValueDatabaseSupport.CREATE_KVDB};
        }
        else if( action.equals(DELETE_DOMAIN) ) {
            return new ServiceAction[]{KeyValueDatabaseSupport.REMOVE_KVDB};
        }
        else if( action.equals(LIST_DOMAINS) ) {
            return new ServiceAction[]{KeyValueDatabaseSupport.LIST_KVDB, KeyValueDatabaseSupport.GET_KVDB};
        }
        else if( action.equals(SELECT) ) {
            return new ServiceAction[]{KeyValueDatabaseSupport.SELECT};
        }
        return new ServiceAction[0];
    }

    private AWSCloud provider;

    SimpleDB( AWSCloud cloud ) {
        provider = cloud;
    }

    @Override
    public void addKeyValuePairs( String inDomainId, String itemId, KeyValuePair... pairs ) throws CloudException, InternalException {
        APITrace.begin(provider, "KVDB.addKeyValuePairs");
        try {
            if( pairs != null && pairs.length > 0 ) {
                Map<String, String> parameters = provider.getStandardSimpleDBParameters(provider.getContext(), PUT_ATTRIBUTES);
                EC2Method method;
                int i = 0;

                parameters.put("DomainName", inDomainId);
                parameters.put("ItemName", itemId);
                for( KeyValuePair pair : pairs ) {
                    parameters.put("Attribute." + i + ".Name", pair.getKey());
                    parameters.put("Attribute." + i + ".Value", pair.getValue());
                    i++;
                }
                method = new EC2Method(SERVICE_ID, provider, parameters);
                try {
                    method.invoke();
                } catch( EC2Exception e ) {
                    throw new CloudException(e);
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public String createDatabase(String name, String description) throws CloudException, InternalException {
        APITrace.begin(provider, "KVDB.createDatabase");
        try {
            Map<String,String> parameters = provider.getStandardSimpleDBParameters(provider.getContext(), CREATE_DOMAIN);
            EC2Method method;

            name = validateName(name);
            parameters.put("DomainName", name);
            method = new EC2Method(SERVICE_ID, provider, parameters);
            try {
                method.invoke();
            }
            catch( EC2Exception e ) {
                throw new CloudException(e);
            }
            return name;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull KeyValueDatabaseCapabilities getCapabilities() throws InternalException, CloudException {
        if(capabilities == null) {
            capabilities = new SimpleDBCapabilities(provider);
        }
        return capabilities;
    }

    @Override
    public KeyValueDatabase getDatabase(String domainId) throws CloudException, InternalException {
        APITrace.begin(provider, "KVDB.getDatabase");
        try {
            Map<String,String> parameters = provider.getStandardSimpleDBParameters(provider.getContext(), DOMAIN_META_DATA);
            EC2Method method;
            Document doc;

            parameters.put("DomainName", domainId);
            method = new EC2Method(SERVICE_ID, provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && code.equals("NoSuchDomain") ) {
                    return null;
                }
                throw new CloudException(e);
            }
            KeyValueDatabase database = new KeyValueDatabase();

            database.setProviderOwnerId(provider.getContext().getAccountNumber());
            database.setProviderRegionId(provider.getContext().getRegionId());
            database.setProviderDatabaseId(domainId);
            database.setName(domainId);
            database.setDescription(domainId);
            NodeList blocks = doc.getElementsByTagName("DomainMetadataResult");
            if( blocks.getLength() > 0 ) {
                for( int i=0; i<blocks.getLength(); i++ ) {
                    NodeList items = blocks.item(i).getChildNodes();

                    for( int j=0; j<items.getLength(); j++ ) {
                        Node item = items.item(j);
                        String name = item.getNodeName();

                        if( name.equals("ItemCount") ) {
                            if( item.hasChildNodes() ) {
                                database.setItemCount(Integer.parseInt(item.getFirstChild().getNodeValue()));
                            }
                        }
                        else if( name.equals("AttributeValueCount") ) {
                            if( item.hasChildNodes() ) {
                                database.setKeyValueCount(Integer.parseInt(item.getFirstChild().getNodeValue()));
                            }
                        }
                        else if( name.equals("AttributeNameCount") ) {
                            if( item.hasChildNodes() ) {
                                database.setKeyCount(Integer.parseInt(item.getFirstChild().getNodeValue()));
                            }
                        }
                        else if( name.equals("ItemNamesSizeBytes") ) {
                            if( item.hasChildNodes() ) {
                                database.setItemSize(Integer.parseInt(item.getFirstChild().getNodeValue()));
                            }
                        }
                        else if( name.equals("AttributeValuesSizeBytes") ) {
                            if( item.hasChildNodes() ) {
                                database.setKeyValueSize(Integer.parseInt(item.getFirstChild().getNodeValue()));
                            }
                        }
                        else if( name.equals("AttributeNamesSizeBytes") ) {
                            if( item.hasChildNodes() ) {
                                database.setKeySize(Integer.parseInt(item.getFirstChild().getNodeValue()));
                            }
                        }
                    }
                }
            }
            return database;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Iterable<KeyValuePair> getKeyValuePairs(String inDomainId, String itemId, boolean consistentRead) throws CloudException, InternalException {
        APITrace.begin(provider, "KVDB.getKeyValuePairs");
        try {
            Map<String,String> parameters = provider.getStandardSimpleDBParameters(provider.getContext(), GET_ATTRIBUTES);
            EC2Method method;
            Document doc;

            parameters.put("DomainName", inDomainId);
            parameters.put("ItemName", itemId);
            parameters.put("ConsistentRead", String.valueOf(consistentRead));
            method = new EC2Method(SERVICE_ID, provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && code.equals("NoSuchDomain") ) {
                    return null;
                }
                throw new CloudException(e);
            };
            ArrayList<KeyValuePair> pairs = new ArrayList<KeyValuePair>();

            NodeList blocks = doc.getElementsByTagName("Attribute");
            for( int i=0; i<blocks.getLength(); i++ ) {
                Node node = blocks.item(i);

                if( node.hasChildNodes() ) {
                    NodeList children = node.getChildNodes();
                    String key = null, value = null;

                    for( int j=0; j<children.getLength(); j++ ) {
                        Node item = children.item(j);

                        if( item.hasChildNodes() ) {
                            String nv = item.getFirstChild().getNodeValue();

                            if( item.getNodeName().equals("Name") ) {
                                key = nv;
                            }
                            else if( item.getNodeName().equals("Value") ) {
                                value = nv;
                            }
                        }
                    }
                    if( key != null ) {
                        pairs.add(new KeyValuePair(key, value));
                    }
                }
            }
            return pairs;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    @Deprecated
    public String getProviderTermForDatabase(Locale locale) {
        try {
            return getCapabilities().getProviderTermForDatabase(locale);
        } catch( InternalException e ) {
        } catch( CloudException e ) {
        }
        return "domain"; // legacy
    }

    private String getSimpleDBUrl() throws InternalException, CloudException {
        if( provider.getContext().getRegionId() == null || provider.getContext().getRegionId().equals("us-east-1") ) {
            return ("https://sdb.amazonaws.com");            
        }
        return ("https://sdb." + provider.getContext().getRegionId() + ".amazonaws.com");
    }
    
    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(provider, "KVDB.isSubscribed");
        try {
            Map<String,String> parameters = provider.getStandardSimpleDBParameters(provider.getContext(), LIST_DOMAINS);
            EC2Method method;

            method = new EC2Method(SERVICE_ID, provider, parameters);
            try {
                method.invoke();
                return true;
            }
            catch( EC2Exception e ) {
                if( e.getStatus() == HttpStatus.SC_UNAUTHORIZED || e.getStatus() == HttpStatus.SC_FORBIDDEN ) {
                    return false;
                }
                String code = e.getCode();

                if( code != null && (code.equals("SubscriptionCheckFailed") || code.equals("AuthFailure") || code.equals("SignatureDoesNotMatch") || code.equals("InvalidClientTokenId") || code.equals("OptInRequired")) ) {
                    return false;
                }
                logger.warn(e.getSummary());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }
    
    @Override
    @Deprecated
    public boolean isSupportsKeyValueDatabases() throws CloudException, InternalException {
        return getCapabilities().isSupportsKeyValueDatabases();
    }
    
    @Override
    public Iterable<String> list() throws CloudException, InternalException {
        APITrace.begin(provider, "KVDB.list");
        try {
            ArrayList<String> list = new ArrayList<String>();
            String marker = null;

            do {
                Map<String,String> parameters = provider.getStandardSimpleDBParameters(provider.getContext(), LIST_DOMAINS);
                EC2Method method;
                NodeList blocks;
                Document doc;

                if( marker != null ) {
                    parameters.put("NextToken", marker);
                }
                method = new EC2Method(SERVICE_ID, provider, parameters);
                try {
                    doc = method.invoke();
                }
                catch( EC2Exception e ) {
                    throw new CloudException(e);
                }
                marker = null;
                blocks = doc.getElementsByTagName("NextToken");
                if( blocks.getLength() > 0 ) {
                    for( int i=0; i<blocks.getLength(); i++ ) {
                        Node item = blocks.item(i);

                        if( item.hasChildNodes() ) {
                            marker = item.getFirstChild().getNodeValue().trim();
                        }
                    }
                    if( marker != null ) {
                        break;
                    }
                }
                blocks = doc.getElementsByTagName("DomainName");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    Node name = blocks.item(i);

                    if( name.hasChildNodes() ) {
                        String domain = name.getFirstChild().getNodeValue();

                        if( domain != null ) {
                            list.add(domain);
                        }
                    }
                }
            }
            while( marker != null );
            return list;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Iterable<ResourceStatus> listKeyValueDatabaseStatus() throws CloudException, InternalException {
        APITrace.begin(provider, "KVDB.listKeyValueDatabaseStatus");
        try {
            ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();
            String marker = null;

            do {
                Map<String,String> parameters = provider.getStandardSimpleDBParameters(provider.getContext(), LIST_DOMAINS);
                EC2Method method;
                NodeList blocks;
                Document doc;

                if( marker != null ) {
                    parameters.put("NextToken", marker);
                }
                method = new EC2Method(SERVICE_ID, provider, parameters);
                try {
                    doc = method.invoke();
                }
                catch( EC2Exception e ) {
                    throw new CloudException(e);
                }
                marker = null;
                blocks = doc.getElementsByTagName("NextToken");
                if( blocks.getLength() > 0 ) {
                    for( int i=0; i<blocks.getLength(); i++ ) {
                        Node item = blocks.item(i);

                        if( item.hasChildNodes() ) {
                            marker = item.getFirstChild().getNodeValue().trim();
                        }
                    }
                    if( marker != null ) {
                        break;
                    }
                }
                blocks = doc.getElementsByTagName("DomainName");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    Node name = blocks.item(i);

                    if( name.hasChildNodes() ) {
                        list.add(new ResourceStatus(name.getFirstChild().getNodeValue(), true));
                    }
                }
            }
            while( marker != null );
            return list;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        if( action.equals(KeyValueDatabaseSupport.ANY) ) {
            return new String[] { EC2Method.SDB_PREFIX + "*" };
        }
        else if( action.equals(KeyValueDatabaseSupport.CREATE_KVDB) ) {
            return new String[] { EC2Method.SDB_PREFIX + CREATE_DOMAIN };
        }
        else if( action.equals(KeyValueDatabaseSupport.DELETE) ) {
            return new String[] { EC2Method.SDB_PREFIX + DELETE_DOMAIN };            
        }
        else if( action.equals(KeyValueDatabaseSupport.GET_KVDB) ) {
            return new String[] { EC2Method.SDB_PREFIX + LIST_DOMAINS };
        }
        else if( action.equals(KeyValueDatabaseSupport.LIST_KVDB) ) {
            return new String[] { EC2Method.SDB_PREFIX + LIST_DOMAINS };
        }
        else if( action.equals(KeyValueDatabaseSupport.PUT) ) {
            return new String[] { EC2Method.SDB_PREFIX + PUT_ATTRIBUTES };
        }
        else if( action.equals(KeyValueDatabaseSupport.REMOVE_KVDB) ) {
            return new String[] { EC2Method.SDB_PREFIX + DELETE_DOMAIN };
        }
        else if( action.equals(KeyValueDatabaseSupport.SELECT) ) {
            return new String[] { EC2Method.SDB_PREFIX + SELECT };
        }
        return new String[0];
    }

    @Override
    public Map<String,Set<KeyValuePair>> query(String queryString, boolean consistentRead) throws CloudException, InternalException {
        APITrace.begin(provider, "KVDB.query");
        try {
            Map<String,Set<KeyValuePair>> pairs = new HashMap<String,Set<KeyValuePair>>();
            String marker = null;

            do {
                Map<String,String> parameters = provider.getStandardSimpleDBParameters(provider.getContext(), SELECT);
                NodeList blocks;
                EC2Method method;
                Document doc;

                if( marker != null ) {
                    parameters.put("NextToken", marker);
                }
                parameters.put("SelectExpression", queryString);
                parameters.put("ConsistentRead", String.valueOf(consistentRead));
                method = new EC2Method(SERVICE_ID, provider, parameters);
                try {
                    doc = method.invoke();
                }
                catch( EC2Exception e ) {
                    String code = e.getCode();

                    if( code != null && code.equals("NoSuchDomain") ) {
                        return null;
                    }
                    throw new CloudException(e);
                }
                marker = null;
                blocks = doc.getElementsByTagName("NextToken");
                if( blocks.getLength() > 0 ) {
                    for( int i=0; i<blocks.getLength(); i++ ) {
                        Node item = blocks.item(i);

                        if( item.hasChildNodes() ) {
                            marker = item.getFirstChild().getNodeValue().trim();
                        }
                    }
                    if( marker != null ) {
                        break;
                    }
                }
                blocks = doc.getElementsByTagName("Item");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    Node item = blocks.item(i);

                    if( item.hasChildNodes() ) {
                        TreeSet<KeyValuePair> itemPairs = new TreeSet<KeyValuePair>();
                        NodeList children = item.getChildNodes();
                        String itemId = null;

                        for( int j=0; j<children.getLength(); j++ ) {
                            Node child = children.item(j);

                            if( child.hasChildNodes() ) {
                                String nn = child.getNodeName();

                                if( nn.equals("Name") ) {
                                    itemId = child.getFirstChild().getNodeValue();
                                }
                                else if( nn.equals("Attribute") ) {
                                    NodeList parts = child.getChildNodes();
                                    String key = null, value = null;

                                    for( int k=0; k<parts.getLength(); k++ ) {
                                        Node part = parts.item(k);

                                        if( part.hasChildNodes() ) {
                                            String nv = part.getFirstChild().getNodeValue();

                                            if( part.getNodeName().equals("Name") ) {
                                                key = nv;
                                            }
                                            else if( part.getNodeName().equals("Value") ) {
                                                value = nv;
                                            }
                                        }
                                    }
                                    if( key != null ) {
                                        itemPairs.add(new KeyValuePair(key, value));
                                    }
                                }
                            }
                        }
                        if( itemId != null ) {
                            pairs.put(itemId, itemPairs);
                        }
                    }
                }
            } while( marker != null );
            return pairs;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeDatabase(String domainId) throws CloudException, InternalException {
        APITrace.begin(provider, "KVDB.removeDatabase");
        try {
            Map<String,String> parameters = provider.getStandardSimpleDBParameters(provider.getContext(), DELETE_DOMAIN);
            EC2Method method;

            parameters.put("DomainName", domainId);
            method = new EC2Method(SERVICE_ID, provider, parameters);
            try {
                method.invoke();
            }
            catch( EC2Exception e ) {
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeKeyValuePairs(String inDomainId, String itemId, KeyValuePair ... pairs) throws CloudException, InternalException {
        APITrace.begin(provider, "KVDB.removeKeyValuePairs");
        try {
            if( pairs != null && pairs.length > 0 ) {
                Map<String,String> parameters = provider.getStandardSimpleDBParameters(provider.getContext(), DELETE_ATTRIBUTES);
                EC2Method method;
                int i = 0;

                parameters.put("DomainName", inDomainId);
                parameters.put("ItemName", itemId);
                for( KeyValuePair pair : pairs ) {
                    parameters.put("Attribute." + i + ".Name", pair.getKey());
                    if( pair.getValue() != null ) {
                        parameters.put("Attribute." + i + ".Value", pair.getValue());
                    }
                    i++;
                }
                method = new EC2Method(SERVICE_ID, provider, parameters);
                try {
                    method.invoke();
                }
                catch( EC2Exception e ) {
                    throw new CloudException(e);
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeKeyValuePairs(String inDomainId, String itemId, String ... pairs) throws CloudException, InternalException {
        APITrace.begin(provider, "KVDB.removeKeyValuePairStrings");
        try {
            if( pairs != null && pairs.length > 0 ) {
                Map<String,String> parameters = provider.getStandardSimpleDBParameters(provider.getContext(), DELETE_ATTRIBUTES);
                EC2Method method;
                int i = 0;

                parameters.put("DomainName", inDomainId);
                parameters.put("ItemName", itemId);
                for( String pair : pairs ) {
                    parameters.put("Attribute." + i + ".Name", pair);
                    i++;
                }
                method = new EC2Method(SERVICE_ID, provider, parameters);
                try {
                    method.invoke();
                }
                catch( EC2Exception e ) {
                    throw new CloudException(e);
                }
            }
        }
        finally {
            APITrace.end();
        }
    }
    
    @Override
    public void replaceKeyValuePairs(String inDomainId, String itemId, KeyValuePair ... pairs) throws CloudException, InternalException {
        APITrace.begin(provider, "KVDB.replaceKeyValuePairs");
        try {
            if( pairs != null && pairs.length > 0 ) {
                Map<String,String> parameters = provider.getStandardSimpleDBParameters(provider.getContext(), PUT_ATTRIBUTES);
                EC2Method method;
                int i = 0;

                parameters.put("DomainName", inDomainId);
                parameters.put("ItemName", itemId);
                for( KeyValuePair pair : pairs ) {
                    parameters.put("Attribute." + i + ".Name", pair.getKey());
                    parameters.put("Attribute." + i + ".Value", pair.getValue());
                    parameters.put("Attribute." + i + ".Replace", "true");
                    i++;
                }
                method = new EC2Method(SERVICE_ID, provider, parameters);
                try {
                    method.invoke();
                }
                catch( EC2Exception e ) {
                    throw new CloudException(e);
                }
            }
        }
        finally {
            APITrace.end();
        }
    }
    
    private String validateName(String name) {
        StringBuilder str = new StringBuilder();
        
        for( int i=0; i<name.length(); i++ ) {
            char c = name.charAt(i);
            
            if( Character.isLetterOrDigit(c) ) {
                str.append(c);
            }
            else if( c == '-' || c == '_' || c == '.' ) {
                str.append(c);
            }
        }
        if( str.length() < 3 ) {
            if( str.length() < 2 ) {
                if( str.length() < 1 ) {
                    return "aaa";
                }
                return str.toString() + "aa";
            }
            return str.toString() + "a";
        }
        else if( str.length() > 255 ) {
            return str.toString().substring(0,255);
        }
        return str.toString();
    }
}
