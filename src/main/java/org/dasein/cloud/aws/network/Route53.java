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

package org.dasein.cloud.aws.network;

import org.apache.http.HttpStatus;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.DNSRecord;
import org.dasein.cloud.network.DNSRecordType;
import org.dasein.cloud.network.DNSSupport;
import org.dasein.cloud.network.DNSZone;
import org.dasein.cloud.util.APITrace;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorPopulator;
import org.dasein.util.PopulatorThread;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Locale;
import java.util.TreeSet;
import java.util.UUID;

public class Route53 implements DNSSupport {
    
    private AWSCloud provider;
    
    Route53(AWSCloud provider) { this.provider = provider; }
    
    private @Nonnull String generateCallerReference() {
        return UUID.randomUUID().toString();
    }

    @Override
    public @Nonnull DNSRecord addDnsRecord(@Nonnull String providerDnsZoneId, @Nonnull DNSRecordType recordType, @Nonnull String name, @Nonnegative int ttl, @Nonnull String... values) throws CloudException, InternalException {
        APITrace.begin(provider, "DNS.addDnsRecord");
        try {
            DNSZone zone = getDnsZone(providerDnsZoneId);

            if( zone == null ) {
                throw new CloudException("Invalid DNS zone: " + providerDnsZoneId);
            }
            Route53Method method;

            if( !name.endsWith(".") && (recordType.equals(DNSRecordType.A) || recordType.equals(DNSRecordType.AAAA)) ) {
                name = name + zone.getDomainName();
            }
            for( DNSRecord record : listDnsRecords(providerDnsZoneId, recordType, null) ) {
                if( record != null && record.getType().equals(recordType) && record.getName().equals(name) ) {
                    deleteDnsRecords(record);
                }
            }
            method = new Route53Method(Route53Method.CHANGE_RESOURCE_RECORD_SETS, provider, getResourceUrl(providerDnsZoneId));
            StringBuilder xml = new StringBuilder();

            xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n");
            xml.append("<ChangeResourceRecordSetsRequest xmlns=\"https://route53.amazonaws.com/doc/2012-12-12/\">");
            xml.append("<ChangeBatch>");
            xml.append("<Changes>");
            xml.append("<Change>");
            xml.append("<Action>CREATE</Action>");
            xml.append("<ResourceRecordSet>");
            xml.append("<Name>");
            xml.append(name);
            xml.append("</Name>");
            xml.append("<Type>");
            xml.append(recordType.toString());
            xml.append("</Type>");
            xml.append("<TTL>");
            xml.append(String.valueOf(ttl));
            xml.append("</TTL>");
            xml.append("<ResourceRecords>");
            if( values.length > 0 ) {
                for( String value : values ) {
                    xml.append("<ResourceRecord>");
                    xml.append("<Value>");
                    xml.append(AWSCloud.escapeXml(value));
                    xml.append("</Value>");
                    xml.append("</ResourceRecord>");
                }
            }
            xml.append("</ResourceRecords>");
            xml.append("</ResourceRecordSet>");
            xml.append("</Change>");
            xml.append("</Changes>");
            xml.append("</ChangeBatch>");
            xml.append("</ChangeResourceRecordSetsRequest>");
            try {
                method.invoke(xml.toString());
            }
            catch( EC2Exception e ) {
                throw new CloudException(e);
            }
            for( DNSRecord record : listDnsRecords(providerDnsZoneId, recordType, null) ) {
                if( record != null && record.getType().equals(recordType) && record.getName().equals(name) ) {
                    return record;
                }
            }
            throw new CloudException("Unable to identified newly added record");
        }
        finally {
            APITrace.end();
        }
    }
    
    @Override
    public @Nonnull String createDnsZone(@Nonnull String domainName, @Nonnull String name, @Nonnull String description) throws CloudException, InternalException {
        APITrace.begin(provider, "DNS.createDnsZone");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured for this request");
            }
            Route53Method method;
            NodeList blocks;
            Document doc;

            method = new Route53Method(Route53Method.CREATE_HOSTED_ZONE, provider, getHostedZoneUrl(null));
            StringBuilder xml = new StringBuilder();

            xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n");
            xml.append("<CreateHostedZoneRequest xmlns=\"https://route53.amazonaws.com/doc/2012-12-12/\">");
            xml.append("<Name>");
            xml.append(domainName);
            xml.append("</Name>");
            xml.append("<CallerReference>");
            xml.append(generateCallerReference());
            xml.append("</CallerReference>");
            xml.append("<HostedZoneConfig><Comment>");
            xml.append(AWSCloud.escapeXml(description));
            xml.append("</Comment></HostedZoneConfig>");
            xml.append("</CreateHostedZoneRequest>");
            try {
                doc = method.invoke(xml.toString());
            }
            catch( EC2Exception e ) {
                throw new CloudException(e);
            }
            ArrayList<String> ns = new ArrayList<String>();
            blocks = doc.getElementsByTagName("NameServer");
            for( int i=0; i<blocks.getLength(); i++ ) {
                Node item = blocks.item(i);

                ns.add(item.getFirstChild().getNodeValue().trim());
            }
            String[] nameservers = new String[ns.size()];

            ns.toArray(nameservers);
            blocks = doc.getElementsByTagName("HostedZone");
            for( int i=0; i<blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                DNSZone zone = toDnsZone(ctx, item, nameservers);

                if( zone != null ) {
                    return zone.getProviderDnsZoneId();
                }
            }
            throw new CloudException("Unable to identify newly created zone");
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void deleteDnsRecords(@Nonnull DNSRecord... dnsRecords) throws CloudException, InternalException {
        APITrace.begin(provider, "DNS.deleteDnsRecords");
        try {
            if( dnsRecords.length < 1 ) {
                return;
            }
            TreeSet<String> zones = new TreeSet<String>();
            for( DNSRecord record : dnsRecords ) {
                zones.add(record.getProviderZoneId());
            }
            for( String zoneId : zones ) {
                Route53Method method = new Route53Method(Route53Method.CHANGE_RESOURCE_RECORD_SETS, provider, getResourceUrl(zoneId));
                StringBuilder xml = new StringBuilder();

                xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n");
                xml.append("<ChangeResourceRecordSetsRequest xmlns=\"https://route53.amazonaws.com/doc/2012-12-12/\">");
                xml.append("<ChangeBatch>");
                xml.append("<Changes>");
                for( DNSRecord record : dnsRecords ) {
                    if( record.getProviderZoneId().equals(zoneId) ) {
                        xml.append("<Change>");
                        xml.append("<Action>DELETE</Action>");
                        xml.append("<ResourceRecordSet>");
                        xml.append("<Name>");
                        xml.append(record.getName());
                        xml.append("</Name>");
                        xml.append("<Type>");
                        xml.append(record.getType().toString());
                        xml.append("</Type>");
                        xml.append("<TTL>");
                        xml.append(String.valueOf(record.getTtl()));
                        xml.append("</TTL>");
                        xml.append("<ResourceRecords>");
                        String[] values = record.getValues();
                        if( values != null && values.length > 0 ) {
                            for( String value : values ) {
                                xml.append("<ResourceRecord>");
                                xml.append("<Value>");
                                xml.append(AWSCloud.escapeXml(value));
                                xml.append("</Value>");
                                xml.append("</ResourceRecord>");
                            }
                        }
                        xml.append("</ResourceRecords>");
                        xml.append("</ResourceRecordSet>");
                        xml.append("</Change>");
                    }
                }
                xml.append("</Changes>");
                xml.append("</ChangeBatch>");
                xml.append("</ChangeResourceRecordSetsRequest>");
                try {
                    method.invoke(xml.toString());
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
    public void deleteDnsZone(@Nonnull String providerDnsZoneId) throws CloudException, InternalException {
        APITrace.begin(provider, "DNS.deleteDnsZone");
        try {
            Route53Method method;

            method = new Route53Method(Route53Method.DELETE_HOSTED_ZONE, provider, getHostedZoneUrl(providerDnsZoneId));
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
    public @Nullable DNSZone getDnsZone(@Nonnull String providerDnsZoneId) throws CloudException, InternalException {
        APITrace.begin(provider, "DNS.getDnsZone");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was configured for this request");
            }
            Route53Method method;
            NodeList blocks;
            Document doc;

            method = new Route53Method(Route53Method.GET_HOSTED_ZONE, provider, getHostedZoneUrl(providerDnsZoneId));
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && ( code.equals("AccessDenied") || code.equals("InvalidInput") ) ) {
                    for( DNSZone zone : listDnsZones() ) {
                        if( zone.getProviderDnsZoneId().equals(providerDnsZoneId) ) {
                            return zone;
                        }
                    }
                    return null;
                }
                else if( code != null && code.equals("NoSuchHostedZone") ) {
                    return null;
                }
                throw new CloudException(e);
            }
            ArrayList<String> ns = new ArrayList<String>();
            blocks = doc.getElementsByTagName("NameServer");
            for( int i=0; i<blocks.getLength(); i++ ) {
                Node item = blocks.item(i);

                ns.add(item.getFirstChild().getNodeValue().trim());
            }
            String[] nameservers = new String[ns.size()];

            ns.toArray(nameservers);
            blocks = doc.getElementsByTagName("HostedZone");
            for( int i=0; i<blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                DNSZone zone = toDnsZone(ctx, item, nameservers);

                if( zone != null ) {
                    return zone;
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String getProviderTermForRecord(@Nonnull Locale locale) {
        return "resource";
    }

    @Override
    public @Nonnull String getProviderTermForZone(@Nonnull Locale locale) {
        return "hosted zone";
    }

    private @Nonnull String getHostedZoneUrl(@Nullable String zoneId) {
        if( zoneId == null ) {
            return "https://route53.amazonaws.com/" + provider.getRoute53Version() + "/hostedzone";
        }
        else {
            return "https://route53.amazonaws.com/" + provider.getRoute53Version() + "/hostedzone/" + zoneId;
        }
    }
    
    private @Nonnull String getResourceUrl(@Nonnull String zoneId) {
        return "https://route53.amazonaws.com/" + provider.getRoute53Version() + "/hostedzone/" + zoneId + "/rrset";
    }
    
    @Override
    public @Nonnull Iterable<DNSRecord> listDnsRecords(@Nonnull String providerDnsZoneId, @Nullable DNSRecordType forType, @Nullable String name) throws CloudException, InternalException {
        final ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was configured for this request");
        }
        PopulatorThread<DNSRecord> populator;
        final String zoneId = providerDnsZoneId;
        final DNSRecordType type = forType;
        final String nom = name;

        provider.hold();
        populator = new PopulatorThread<DNSRecord>(new JiteratorPopulator<DNSRecord>() {
            public void populate(@Nonnull Jiterator<DNSRecord> iterator) throws CloudException, InternalException {
                try {
                    populateRecords(iterator, zoneId, type, nom);
                }
                finally {
                    provider.release();
                }
            }
        });
        populator.populate();
        return populator.getResult();
    }
    
    private void populateRecords(@Nonnull Jiterator<DNSRecord> iterator, @Nonnull String providerDnsZoneId, @Nullable DNSRecordType forType, @Nullable String name) throws CloudException, InternalException {
        APITrace.begin(provider, "DNS.listDnsRecords");
        try {
            DNSZone zone = getDnsZone(providerDnsZoneId);

            if( zone == null ) {
                return;
            }
            String url = getResourceUrl(providerDnsZoneId);
            Route53Method method;
            NodeList blocks;
            Document doc;

            if( name == null ) {
                name = zone.getDomainName();
            }
            if( forType == null ) {
                url += "?name=" + AWSCloud.encode(name, false);
            }
            else {
                url += "?type=" + AWSCloud.encode(forType.toString(), false) + "&name=" + AWSCloud.encode(name, false);
            }
            method = new Route53Method(Route53Method.LIST_RESOURCE_RECORD_SETS, provider, url);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("ResourceRecordSet");
            for( int i=0; i<blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                DNSRecord record = toDnsRecord(providerDnsZoneId, item);

                if( record != null ) {
                    iterator.push(record);
                }
            }
            blocks = doc.getElementsByTagName("IsTruncated");
            if( blocks != null && blocks.getLength() == 1 && blocks.item(0).hasChildNodes() && blocks.item(0).getFirstChild().getNodeValue().trim().equalsIgnoreCase("true") ) {
                DNSRecordType nextType = null;
                String nextName = null;

                blocks = doc.getElementsByTagName("NextRecordName");
                if( blocks != null && blocks.getLength() == 1 && blocks.item(0).hasChildNodes() ) {
                    nextName = blocks.item(0).getFirstChild().getNodeValue().trim();
                }
                blocks = doc.getElementsByTagName("NextRecordType");
                if( blocks != null && blocks.getLength() == 1 && blocks.item(0).hasChildNodes() ) {
                    nextType = DNSRecordType.valueOf(blocks.item(0).getFirstChild().getNodeValue().trim());
                }
                if( nextName != null && nextType != null ) {
                    populateRecords(iterator, providerDnsZoneId, nextType, nextName);
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listDnsZoneStatus() throws CloudException, InternalException {
        final ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was configured for this request");
        }
        PopulatorThread<ResourceStatus> populator;

        provider.hold();
        populator = new PopulatorThread<ResourceStatus>(new JiteratorPopulator<ResourceStatus>() {
            public void populate(@Nonnull Jiterator<ResourceStatus> iterator) throws CloudException, InternalException {
                try {
                    populateZoneStatus(iterator, null);
                }
                finally {
                    provider.release();
                }
            }
        });
        populator.populate();
        return populator.getResult();
    }

    @Override
    public @Nonnull Iterable<DNSZone> listDnsZones() throws CloudException, InternalException {
        final ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was configured for this request");
        }
        PopulatorThread<DNSZone> populator;
        
        provider.hold();
        populator = new PopulatorThread<DNSZone>(new JiteratorPopulator<DNSZone>() {
            public void populate(@Nonnull Jiterator<DNSZone> iterator) throws CloudException, InternalException {
                populateZones(ctx, iterator, null);
                provider.release();
            }
        });        
        populator.populate();
        return populator.getResult();
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        if( action.equals(DNSSupport.ANY) ) {
            return new String[] { Route53Method.R53_PREFIX + "*" };
        }
        else if( action.equals(DNSSupport.ADD_RECORD) ) {
            return new String[] { Route53Method.R53_PREFIX + Route53Method.CHANGE_RESOURCE_RECORD_SETS };
        }
        else if( action.equals(DNSSupport.CREATE_ZONE) ) {
            return new String[] { Route53Method.R53_PREFIX + Route53Method.CREATE_HOSTED_ZONE };
        }
        else if( action.equals(DNSSupport.GET_ZONE) ) {
            return new String[] { Route53Method.R53_PREFIX + Route53Method.GET_HOSTED_ZONE};
        }
        else if( action.equals(DNSSupport.LIST_ZONE) ) {
            return new String[] { Route53Method.R53_PREFIX + Route53Method.LIST_HOSTED_ZONES };
        }
        else if( action.equals(DNSSupport.LIST_RECORD) ) {
            return new String[] { Route53Method.R53_PREFIX + Route53Method.LIST_RESOURCE_RECORD_SETS };
        }
        else if( action.equals(DNSSupport.REMOVE_RECORD) ) {
            return new String[] { Route53Method.R53_PREFIX + Route53Method.CHANGE_RESOURCE_RECORD_SETS };
        }
        else if( action.equals(DNSSupport.REMOVE_ZONE) ) {
            return new String[] { Route53Method.R53_PREFIX + Route53Method.DELETE_HOSTED_ZONE };
        }
        return new String[0];
    }

    private void populateZoneStatus(@Nonnull Jiterator<ResourceStatus> iterator, @Nullable String marker) throws CloudException, InternalException {
        APITrace.begin(provider, "DNS.listDnsZoneStatus");
        try {
            String url = getHostedZoneUrl(null);
            Route53Method method;
            NodeList blocks;
            Document doc;

            if( marker != null ) {
                url = url + "?marker=" + marker;
            }
            method = new Route53Method(Route53Method.LIST_HOSTED_ZONES, provider, url);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("HostedZone");
            for( int i=0; i<blocks.getLength(); i++ ) {
                ResourceStatus status = toStatus(blocks.item(i));

                if( status != null ) {
                    iterator.push(status);
                }
            }
            blocks = doc.getElementsByTagName("IsTruncated");
            if( blocks != null && blocks.getLength() == 1 && blocks.item(0).hasChildNodes() && blocks.item(0).getFirstChild().getNodeValue().trim().equalsIgnoreCase("true") ) {
                blocks = doc.getElementsByTagName("NextMarker");
                if( blocks != null && blocks.getLength() == 1 && blocks.item(0).hasChildNodes() ) {
                    populateZoneStatus(iterator, blocks.item(0).getFirstChild().getNodeValue().trim());
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    private void populateZones(@Nonnull ProviderContext ctx, @Nonnull Jiterator<DNSZone> iterator, @Nullable String marker) throws CloudException, InternalException {
        APITrace.begin(provider, "DNS.listDnsZones");
        try {
            String url = getHostedZoneUrl(null);
            Route53Method method;
            NodeList blocks;
            Document doc;

            if( marker != null ) {
                url = url + "?marker=" + marker;
            }
            method = new Route53Method(Route53Method.LIST_HOSTED_ZONES, provider, url);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("HostedZone");
            for( int i=0; i<blocks.getLength(); i++ ) {
                Node item = blocks.item(i);
                DNSZone zone = toDnsZone(ctx, item, new String[0]);

                if( zone != null ) {
                    iterator.push(zone);
                }
            }
            blocks = doc.getElementsByTagName("IsTruncated");
            if( blocks != null && blocks.getLength() == 1 && blocks.item(0).hasChildNodes() && blocks.item(0).getFirstChild().getNodeValue().trim().equalsIgnoreCase("true") ) {
                blocks = doc.getElementsByTagName("NextMarker");
                if( blocks != null && blocks.getLength() == 1 && blocks.item(0).hasChildNodes() ) {
                    populateZones(ctx, iterator, blocks.item(0).getFirstChild().getNodeValue().trim());
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(provider, "DNS.isSubscribed");
        try {
            Route53Method method;

            method = new Route53Method(Route53Method.LIST_HOSTED_ZONES, provider, getHostedZoneUrl(null));
            try {
                method.invoke();
            }
            catch( EC2Exception e ) {
                if( e.getStatus() == HttpStatus.SC_UNAUTHORIZED || e.getStatus() == HttpStatus.SC_FORBIDDEN ) {
                    return false;
                }
                String code = e.getCode();

                if( code != null && (code.equals("SubscriptionCheckFailed") || code.equals("AuthFailure") || code.equals("SignatureDoesNotMatch") || code.equals("InvalidClientTokenId") || code.equals("OptInRequired")) ) {
                    return false;
                }
                throw new CloudException(e);
            }
            return true;
        }
        finally {
            APITrace.end();
        }
    }

    private @Nullable DNSRecord toDnsRecord(@Nonnull String providerDnsZoneId, @Nullable Node xmlRecord) {
        if( xmlRecord == null ) {
            return null;
        }
        NodeList attrs = xmlRecord.getChildNodes();
        DNSRecord record = new DNSRecord();
        
        record.setProviderZoneId(providerDnsZoneId);
        for( int i=0; i<attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;
            
            name = attr.getNodeName();
            if( name.equalsIgnoreCase("name") ) {
                String value = attr.getFirstChild().getNodeValue();
                
                if( value == null ) {
                    return null;
                }
                record.setName(value.trim());
            }
            else if( name.equalsIgnoreCase("type") ) {
                String value = attr.getFirstChild().getNodeValue();
                
                if( value != null ) {
                    record.setType(DNSRecordType.valueOf(value.trim()));
                }
            }
            else if( name.equalsIgnoreCase("ttl") ) {
                String value = attr.getFirstChild().getNodeValue();
                
                if( value != null ) {
                    record.setTtl(Integer.parseInt(value.trim()));
                }
            }            
            else if( name.equalsIgnoreCase("resourcerecords") ) {
                ArrayList<String> data = new ArrayList<String>();
                NodeList configs = attr.getChildNodes();
                
                for( int j=0; j<configs.getLength(); j++ ) {
                    Node item = configs.item(j);
                    
                    if( item.getNodeName().equalsIgnoreCase("resourcerecord") ) {
                        NodeList values = item.getChildNodes();
                        
                        for( int k=0; k<values.getLength(); k++ ) {
                            Node r = values.item(k);
                            
                            if( r.getNodeName().equalsIgnoreCase("value") ) {
                                String value = (r.hasChildNodes() ? r.getFirstChild().getNodeValue() : null);
                                
                                if( value != null ) {
                                    data.add(value.trim());
                                }
                            }
                        }
                    }
                }
                record.setValues(data.toArray(new String[data.size()]));
            }
        }
        return record;
    }
    
    private @Nullable DNSZone toDnsZone(@Nonnull ProviderContext ctx, @Nullable Node xmlZone, @Nullable String[] nameservers) {
        if( xmlZone == null ) {
            return null;
        }
        NodeList attrs = xmlZone.getChildNodes();
        DNSZone zone = new DNSZone();
        
        for( int i=0; i<attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;
            
            name = attr.getNodeName();
            if( name.equalsIgnoreCase("id") ) {
                if (attr.getFirstChild() == null || attr.getFirstChild().getNodeValue() == null) {
                    return null;
                }
                String value = attr.getFirstChild().getNodeValue().trim();
                int idx = value.lastIndexOf('/');
                
                value = value.substring(idx+1);
                zone.setProviderDnsZoneId(value);
            }
            else if( name.equalsIgnoreCase("name") ) {
                String value = attr.getFirstChild().getNodeValue().trim();
                
                zone.setDomainName(value);
            }
            else if( name.equalsIgnoreCase("config") ) {
                NodeList configs = attr.getChildNodes();
                
                for( int j=0; j<configs.getLength(); j++ ) {
                    Node item = configs.item(j);
                    
                    if( item.getNodeName().equalsIgnoreCase("comment") ) {
                        zone.setDescription(item.getFirstChild().getNodeValue().trim());
                    }
                }
            }
        }
        if( zone.getName() == null ) {
            zone.setName(zone.getDomainName());
        }
        if( zone.getDescription() == null ) {
            zone.setDescription(zone.getName());
        }
        zone.setProviderOwnerId(ctx.getAccountNumber());
        zone.setNameservers(nameservers);
        return zone;
    }

    private @Nullable ResourceStatus toStatus(@Nullable Node xmlZone) {
        if( xmlZone == null ) {
            return null;
        }
        NodeList attrs = xmlZone.getChildNodes();
        String zoneId = null;

        for( int i=0; i<attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            String name;

            name = attr.getNodeName();
            if( name.equalsIgnoreCase("id") ) {
                if (attr.getFirstChild() == null || attr.getFirstChild().getNodeValue() == null) {
                    return null;
                }
                String value = attr.getFirstChild().getNodeValue().trim();
                int idx = value.lastIndexOf('/');

                value = value.substring(idx+1);
                zoneId = value;
                break;
            }
        }
        if( zoneId == null ) {
            return null;
        }
        return new ResourceStatus(zoneId, true);
    }
}
