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

package org.dasein.cloud.aws.identity;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.aws.compute.EC2Method;
import org.dasein.cloud.identity.SSHKeypair;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.identity.ShellKeyCapabilities;
import org.dasein.cloud.identity.ShellKeySupport;
import org.dasein.cloud.util.APITrace;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class Keypairs implements ShellKeySupport {
	static private final Logger logger = AWSCloud.getLogger(Keypairs.class);
	
	private AWSCloud provider = null;
    private volatile transient KeypairsCapabilities capabilities;

	public Keypairs(@Nonnull AWSCloud provider) {
		this.provider =  provider;
	}
	
	@Override
	public @Nonnull SSHKeypair createKeypair(@Nonnull String name) throws InternalException, CloudException {
        APITrace.begin(provider, "Keypair.createKeypair");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was established for this call.");
            }
            String regionId = ctx.getRegionId();

            if( regionId == null ) {
                throw new CloudException("No region was set for this request.");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.CREATE_KEY_PAIR);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("KeyName", name);
            method = new EC2Method(provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            String material = null, fingerprint = null;
            blocks = doc.getElementsByTagName("CreateKeyPairResponse");
            for( int i=0; i<blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                NodeList attrs = item.getChildNodes();

                for( int j=0; j<attrs.getLength(); j++ ) {
                    Node attr = attrs.item(j);

                    if( attr.getNodeName().equalsIgnoreCase("keyMaterial")) {
                        material = attr.getFirstChild().getNodeValue();

                    }
                    else if( attr.getNodeName().equalsIgnoreCase("keyFingerPrint")) {
                        fingerprint = attr.getFirstChild().getNodeValue();

                    }
                }
            }
            if( fingerprint == null || material == null ) {
                throw new CloudException("Invalid response to attempt to create the keypair");
            }
            SSHKeypair key = new SSHKeypair();

            try {
                key.setPrivateKey(material.getBytes("utf-8"));
            }
            catch( UnsupportedEncodingException e ) {
                throw new InternalException(e);
            }
            key.setFingerprint(fingerprint);
            key.setName(name);
            key.setProviderKeypairId(name);
            key.setProviderOwnerId(ctx.getAccountNumber());
            key.setProviderRegionId(regionId);
            return key;
        }
        finally {
            APITrace.end();
        }
	}

	@Override
	public void deleteKeypair(@Nonnull String name) throws InternalException, CloudException {
        APITrace.begin(provider, "Keypair.deleteKeypair");
        try {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DELETE_KEY_PAIR);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("KeyName", name);
            method = new EC2Method(provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && code.startsWith("InvalidKeyPair") ) {
                    return;
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("return");
            if( blocks.getLength() > 0 ) {
                if( !blocks.item(0).getFirstChild().getNodeValue().equalsIgnoreCase("true") ) {
                    throw new CloudException("Deletion of keypair denied.");
                }
            }
        }
        finally {
            APITrace.end();
        }
	}

	@Override
	public @Nullable String getFingerprint(@Nonnull String name) throws InternalException, CloudException {
        APITrace.begin(provider, "Keypair.getFingerprint");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new InternalException("No context was established for this call.");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_KEY_PAIRS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("KeyName.1", name);
            method = new EC2Method(provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && code.startsWith("InvalidKeyPair") ) {
                    return null;
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("keyFingerprint");
            if( blocks.getLength() > 0 ) {
                return blocks.item(0).getFirstChild().getNodeValue().trim();
            }
            throw new CloudException("Unable to identify key fingerprint.");
        }
        finally {
            APITrace.end();
        }
	}

    @Override
    public Requirement getKeyImportSupport() throws CloudException, InternalException {
        return Requirement.OPTIONAL;
    }

    @Override
    public @Nullable SSHKeypair getKeypair(@Nonnull String name) throws InternalException, CloudException {
        APITrace.begin(provider, "Keypair.getKeypair");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was established for this call.");
            }
            String regionId = ctx.getRegionId();

            if( regionId == null ) {
                throw new CloudException("No region was set for this request.");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_KEY_PAIRS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("KeyName.1", name);
            method = new EC2Method(provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && code.startsWith("InvalidKeyPair") ) {
                    return null;
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("item");
            for( int i=0; i<blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                NodeList attrs = item.getChildNodes();
                String fingerprint = null;
                String keyName = null;

                for( int j=0; j<attrs.getLength(); j++ ) {
                    Node attr = attrs.item(j);

                    if( attr.getNodeName().equalsIgnoreCase("keyFingerprint") && attr.hasChildNodes() ) {
                        fingerprint = attr.getFirstChild().getNodeValue().trim();
                    }
                    else if( attr.getNodeName().equalsIgnoreCase("keyName") && attr.hasChildNodes() ) {
                        keyName = attr.getFirstChild().getNodeValue().trim();
                    }
                }
                if( keyName != null && keyName.equals(name) && fingerprint != null ) {
                    SSHKeypair kp = new SSHKeypair();

                    kp.setFingerprint(fingerprint);
                    kp.setName(keyName);
                    kp.setPrivateKey(null);
                    kp.setPublicKey(null);
                    kp.setProviderKeypairId(keyName);
                    kp.setProviderOwnerId(ctx.getAccountNumber());
                    kp.setProviderRegionId(regionId);
                    return kp;
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

	@Override
	public @Nonnull String getProviderTermForKeypair(@Nonnull Locale locale) {
		return "keypair";
	}

    @Override
    public @Nonnull ShellKeyCapabilities getCapabilities() throws CloudException, InternalException {
        if( capabilities == null ) {
            capabilities = new KeypairsCapabilities(provider);
        }
        return capabilities;
    }

    @Override
    public @Nonnull SSHKeypair importKeypair(@Nonnull String name, @Nonnull String material) throws InternalException, CloudException {
        APITrace.begin(provider, "Keypair.importKeypair");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was established for this call.");
            }
            String regionId = ctx.getRegionId();

            if( regionId == null ) {
                throw new CloudException("No region was set for this request.");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.IMPORT_KEY_PAIR);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("KeyName", name);
            parameters.put("PublicKeyMaterial", material);
            method = new EC2Method(provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            String fingerprint = null;

            blocks = doc.getElementsByTagName("ImportKeyPairResponse");
            for( int i=0; i<blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                NodeList attrs = item.getChildNodes();

                for( int j=0; j<attrs.getLength(); j++ ) {
                    Node attr = attrs.item(j);

                    if( attr.getNodeName().equalsIgnoreCase("keyFingerPrint")) {
                        fingerprint = attr.getFirstChild().getNodeValue();

                    }
                }
            }
            if( fingerprint == null ) {
                throw new CloudException("Invalid response to attempt to create the keypair");
            }
            SSHKeypair key = new SSHKeypair();

            try {
                key.setPrivateKey(material.getBytes("utf-8"));
            }
            catch( UnsupportedEncodingException e ) {
                throw new InternalException(e);
            }
            key.setFingerprint(fingerprint);
            key.setName(name);
            key.setProviderKeypairId(name);
            key.setProviderOwnerId(ctx.getAccountNumber());
            key.setProviderRegionId(regionId);
            return key;
        }
        finally {
            APITrace.end();
        }
    }
    
    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(provider, "Keypair.isSubscribed");
        try {
            provider.testContext();
            return true;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
	public @Nonnull Collection<SSHKeypair> list() throws InternalException, CloudException {
        APITrace.begin(provider, "Keypair.list");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was established for this call.");
            }
            String regionId = ctx.getRegionId();

            if( regionId == null ) {
                throw new CloudException("No region was set for this request.");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_KEY_PAIRS);
            ArrayList<SSHKeypair> keypairs = new ArrayList<SSHKeypair>();
            EC2Method method;
            NodeList blocks;
            Document doc;

            method = new EC2Method(provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && code.startsWith("InvalidKeyPair") ) {
                    return Collections.emptyList();
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("item");
            for( int i=0; i<blocks.getLength(); i++ ) {
                Node item = blocks.item(i);

                if( item.hasChildNodes() ) {
                    NodeList attrs = item.getChildNodes();
                    String fingerprint = null;

                    String keyName = null;
                    for( int j=0; j<attrs.getLength(); j++ ) {
                        Node attr = attrs.item(j);

                        if( attr.getNodeName().equalsIgnoreCase("keyName") && attr.hasChildNodes() ) {
                            keyName = attr.getFirstChild().getNodeValue().trim();
                        }
                        else if( attr.getNodeName().equalsIgnoreCase("keyFingerprint") && attr.hasChildNodes() ) {
                            fingerprint = attr.getFirstChild().getNodeValue().trim();
                        }
                    }
                    if( keyName != null && fingerprint != null ) {
                        SSHKeypair keypair = new SSHKeypair();

                        keypair.setName(keyName);
                        keypair.setProviderKeypairId(keyName);
                        keypair.setFingerprint(fingerprint);
                        keypair.setProviderOwnerId(ctx.getAccountNumber());
                        keypair.setProviderRegionId(regionId);
                        keypairs.add(keypair);
                    }
                }
            }
            return keypairs;
        }
        finally {
            APITrace.end();
        }
	}
    
    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        if( action.equals(ShellKeySupport.ANY) ) {
            return new String[] { EC2Method.EC2_PREFIX + "*" };
        }
        if( action.equals(ShellKeySupport.CREATE_KEYPAIR) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.CREATE_KEY_PAIR, EC2Method.EC2_PREFIX + EC2Method.IMPORT_KEY_PAIR };
        }
        else if( action.equals(ShellKeySupport.GET_KEYPAIR) || action.equals(ShellKeySupport.LIST_KEYPAIR) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_KEY_PAIRS };
        }
        else if( action.equals(ShellKeySupport.REMOVE_KEYPAIR) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DELETE_KEY_PAIR };
        }
        return new String[0];
    }
}
