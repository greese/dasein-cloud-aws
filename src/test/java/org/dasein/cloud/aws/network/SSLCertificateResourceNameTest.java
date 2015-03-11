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

import org.dasein.cloud.aws.identity.InvalidAmazonResourceNameException;
import org.junit.Test;

import org.dasein.cloud.aws.identity.SSLCertificateResourceName;
import static org.junit.Assert.assertEquals;

/**
 * @author igoonich
 * @since 13.05.2014
 */
public class SSLCertificateResourceNameTest {

    @Test
    public void testSSLCertificateNameMatches() throws InvalidAmazonResourceNameException {
        SSLCertificateResourceName sslCertificateResourceName = SSLCertificateResourceName
                .parseArn("arn:aws:iam::123456789012:server-certificate/SomeCertName");
        assertEquals("Certificate name doesn't match", sslCertificateResourceName.getCertificateName(), "SomeCertName");
        assertEquals("Account ID doesn't match", sslCertificateResourceName.getAccountId(), "123456789012");
        assertEquals("Path doesn't match", sslCertificateResourceName.getPath(), "/");
    }

    @Test
    public void testSSLCertificateNameWithPathsMatches() throws InvalidAmazonResourceNameException {
        SSLCertificateResourceName sslCertificateResourceName = SSLCertificateResourceName
                .parseArn("arn:aws:iam::123456789012:server-certificate/division_abc/subdivision_xyz/SomeCertName");
        assertEquals("Certificate name doesn't match", sslCertificateResourceName.getCertificateName(), "SomeCertName");
        assertEquals( "Account ID doesn't match", sslCertificateResourceName.getAccountId(), "123456789012");
        assertEquals("Path doesn't match", sslCertificateResourceName.getPath(), "/division_abc/subdivision_xyz/");
    }

    @Test(expected = InvalidAmazonResourceNameException.class)
    public void negativeTestSSLCertificateNameWrongType() throws InvalidAmazonResourceNameException {
        SSLCertificateResourceName.parseArn("arn:aws:abc::123456789012:SomeCertName");
    }

    @Test(expected = InvalidAmazonResourceNameException.class)
    public void negativeTestSSLInvalidCertificateName() throws InvalidAmazonResourceNameException {
        SSLCertificateResourceName.parseArn("Invalid-Certificate");
    }
}
