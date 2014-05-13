package org.dasein.cloud.aws.network;

import org.dasein.cloud.aws.identity.InvalidAmazonResourceName;
import org.junit.Test;

import org.dasein.cloud.aws.identity.SSLCertificateResourceName;
import static org.junit.Assert.assertEquals;

/**
 * @author igoonich
 * @since 13.05.2014
 */
public class SSLCertificateResourceNameTest {

    @Test
    public void testSSLCertificateNameMatches() throws InvalidAmazonResourceName {
        SSLCertificateResourceName sslCertificateResourceName = SSLCertificateResourceName
                .parseArn("arn:aws:iam::123456789012:server-certificate/SomeCertName");
        assertEquals("Certificate name doesn't match", sslCertificateResourceName.getCertificateName(), "SomeCertName");
        assertEquals("Account ID doesn't match", sslCertificateResourceName.getAccountId(), "123456789012");
        assertEquals("Path doesn't match", sslCertificateResourceName.getPath(), "/");
    }

    @Test
    public void testSSLCertificateNameWithPathsMatches() throws InvalidAmazonResourceName {
        SSLCertificateResourceName sslCertificateResourceName = SSLCertificateResourceName
                .parseArn("arn:aws:iam::123456789012:server-certificate/division_abc/subdivision_xyz/SomeCertName");
        assertEquals("Certificate name doesn't match", sslCertificateResourceName.getCertificateName(), "SomeCertName");
        assertEquals( "Account ID doesn't match", sslCertificateResourceName.getAccountId(), "123456789012");
        assertEquals("Path doesn't match", sslCertificateResourceName.getPath(), "/division_abc/subdivision_xyz");
    }

    @Test(expected = InvalidAmazonResourceName.class)
    public void negativeTestSSLCertificateNameWrongType() throws InvalidAmazonResourceName {
        SSLCertificateResourceName.parseArn("arn:aws:abc::123456789012:SomeCertName");
    }

    @Test(expected = InvalidAmazonResourceName.class)
    public void negativeTestSSLInvalidCertificateName() throws InvalidAmazonResourceName {
        SSLCertificateResourceName.parseArn("Invalid-Certificate");
    }
}
