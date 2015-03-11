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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SSL Certificates resource name
 *
 * <p> According to <a href="http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#arn-syntax-iam">documentation</a>
 * IAM ARN for SSL certificates should have the following format: <b>arn:aws:iam::account:server-certificate/certificatename</b>
 *
 * <p> For example certificates can have the following values:
 * <ul>
 * <li>arn:aws:iam::123456789012:server-certificate/ProdServerCert</li>
 * <li>arn:aws:iam::123456789012:server-certificate/division_abc/subdivision_xyz/ProdServerCert</li>
 * </ul>
 *
 * @author igoonich
 * @since 13.05.2014
 */
public class SSLCertificateResourceName {

    private static final Pattern SSL_CERTIFICATE_RESOURCE_PATTERN
            = Pattern.compile("^arn:aws:iam::([^:]+):server-certificate/([^:]+)$");

    private static final Pattern SSL_CERTIFICATE_RESOURCE_WITH_PATH_PATTERN
            = Pattern.compile("^arn:aws:iam::([^:]+):server-certificate/([^:]+)/([^:]+)$");

    private String accountId;
    private String path = "/";
    private String certificateName;

    public SSLCertificateResourceName(String accountId, String certificateName) {
        this.accountId = accountId;
        this.certificateName = certificateName;
    }

    public SSLCertificateResourceName(String accountId, String path, String certificateName) {
        this.accountId = accountId;
        this.path = path;
        this.certificateName = certificateName;
    }

    public String getAccountId() {
        return accountId;
    }

    public String getCertificateName() {
        return certificateName;
    }

    public String getPath() {
        return path;
    }

    public static SSLCertificateResourceName parseArn(String sslCertificateArn) throws InvalidAmazonResourceNameException {
        if (sslCertificateArn == null) {
            throw new IllegalArgumentException("Provided SSL certificate resource name is null");
        }

        Matcher matcher = SSL_CERTIFICATE_RESOURCE_WITH_PATH_PATTERN.matcher(sslCertificateArn);
        if (matcher.matches()) {
            return new SSLCertificateResourceName(matcher.group(1), "/" + matcher.group(2) + "/", matcher.group(3));
        }

        matcher = SSL_CERTIFICATE_RESOURCE_PATTERN.matcher(sslCertificateArn);
        if (!matcher.matches()) {
            throw new InvalidAmazonResourceNameException("Provided server certificate ARN [" + sslCertificateArn
                    + "] doesn't match pattern [" + SSL_CERTIFICATE_RESOURCE_PATTERN + "]", sslCertificateArn);
        }
        return new SSLCertificateResourceName(matcher.group(1), matcher.group(2));
    }

    @Override
    public String toString() {
        return "arn:aws:iam::" + accountId + ":server-certificate" + path + certificateName;
    }

}
