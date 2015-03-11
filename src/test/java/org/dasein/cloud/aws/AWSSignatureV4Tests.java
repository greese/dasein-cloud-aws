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

package org.dasein.cloud.aws;

import org.dasein.cloud.ProviderContext;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class AWSSignatureV4Tests {

    private AWSCloud awsCloud;

    public AWSSignatureV4Tests() {
        this.awsCloud = new AWSCloud();
        awsCloud.connect(new ProviderContext("2123454", "us-east-1"));
    }

    /* ========================= GENERATED TESTS ==========================

    The below tests are generated from the AWS Signature V4 Test Suite:
        http://docs.aws.amazon.com/general/latest/gr/signature-v4-test-suite.html

    We currently skip four of these test cases which cover functionality we do
    not care about. Namely, duplicate HTTP headers and Unicode query strings.

    Tests were generated using this Python script, run from the extracted
    aws5_testsuite directory:

#!/usr/bin/python
import os
from hashlib import sha256 as sha256

ACCESS_KEY = "AKIDEXAMPLE"
SECRET = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"

# ignore some tests for functionality we don't care about:
# unicode query strings and duplicate headers
IGNORED = ["get-vanilla-ut8-query", "get-header-key-duplicate",
           "get-header-value-order", "post-vanilla-query-nonunreserved"]

files = set(os.listdir("."))
basenames = set(os.path.splitext(f)[0] for f in files)
basenames = set(f for f in basenames if f + ".req" in files and f + ".authz" in files)

def out(s):
    print "    " + s

for f in sorted(basenames):
    req = open(f + ".req").read()
    authz = open(f + ".authz").read().strip()

    header, body = req.split("\r\n\r\n", 1)

    req_lines = header.split("\r\n")
    request = req_lines[0].strip()

    headers = [(line.split(":", 1)) for line in req_lines[1:]]
    body_hash = sha256(body).hexdigest()

    method, _, request = request.partition(" ")
    request = request[:request.find(" ")].replace("\\", "\\\\").replace('"', '\\"')

    out("@Test")
    if f in IGNORED:
        out('@Ignore("AWS4 signature feature we don\'t care about")')
    out("public void testV4Signature__" + f.replace('-', '_') + "() throws Exception {")
    out("    Map<String, String> headers = new HashMap<String, String>();")
    for key, val in headers:
        out('    headers.put("' + key + '", "' + val + '");')
    out('    String authz = awsCloud.getV4Authorization("' + ACCESS_KEY + '", "' + SECRET + '",')
    out('"' + method + '", "https://host.foo.com' + request +
            '", "host", headers, "' + body_hash + '");')
    out('    assertEquals("'+ authz + '", authz);')
    out("}\n")
     */


    @Test
    @Ignore("AWS4 signature feature we don't care about")
    public void testV4Signature__get_header_key_duplicate() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("DATE", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("host", "host.foo.com");
        headers.put("ZOO", "zoobar");
        headers.put("zoo", "foobar");
        headers.put("zoo", "zoobar");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "POST", "https://host.foo.com/", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host;zoo, Signature=54afcaaf45b331f81cd2edb974f7b824ff4dd594cbbaa945ed636b48477368ed", authz);
    }

    @Test
    @Ignore("AWS4 signature feature we don't care about")
    public void testV4Signature__get_header_value_order() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("DATE", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("host", "host.foo.com");
        headers.put("p", "z");
        headers.put("p", "a");
        headers.put("p", "p");
        headers.put("p", "a");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "POST", "https://host.foo.com/", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host;p, Signature=d2973954263943b11624a11d1c963ca81fb274169c7868b2858c04f083199e3d", authz);
    }

    @Test
    public void testV4Signature__get_header_value_trim() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("DATE", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("host", "host.foo.com");
        headers.put("p", " phfft ");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "POST", "https://host.foo.com/", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host;p, Signature=debf546796015d6f6ded8626f5ce98597c33b47b9164cf6b17b4642036fcb592", authz);
    }

    @Test
    public void testV4Signature__get_relative() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "GET", "https://host.foo.com/foo/..", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=b27ccfbfa7df52a200ff74193ca6e32d4b48b8856fab7ebf1c595d0670a7e470", authz);
    }

    @Test
    public void testV4Signature__get_relative_relative() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "GET", "https://host.foo.com/foo/bar/../..", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=b27ccfbfa7df52a200ff74193ca6e32d4b48b8856fab7ebf1c595d0670a7e470", authz);
    }

    @Test
    public void testV4Signature__get_slash() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "GET", "https://host.foo.com//", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=b27ccfbfa7df52a200ff74193ca6e32d4b48b8856fab7ebf1c595d0670a7e470", authz);
    }

    @Test
    public void testV4Signature__get_slash_dot_slash() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "GET", "https://host.foo.com/./", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=b27ccfbfa7df52a200ff74193ca6e32d4b48b8856fab7ebf1c595d0670a7e470", authz);
    }

    @Test
    public void testV4Signature__get_slash_pointless_dot() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "GET", "https://host.foo.com/./foo", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=910e4d6c9abafaf87898e1eb4c929135782ea25bb0279703146455745391e63a", authz);
    }

    @Test
    public void testV4Signature__get_slashes() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "GET", "https://host.foo.com//foo//", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=b00392262853cfe3201e47ccf945601079e9b8a7f51ee4c3d9ee4f187aa9bf19", authz);
    }

    @Test
    public void testV4Signature__get_space() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "GET", "https://host.foo.com/%20/foo", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=f309cfbd10197a230c42dd17dbf5cca8a0722564cb40a872d25623cfa758e374", authz);
    }

    @Test
    public void testV4Signature__get_unreserved() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "GET", "https://host.foo.com/-._~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=830cc36d03f0f84e6ee4953fbe701c1c8b71a0372c63af9255aa364dd183281e", authz);
    }

    @Test
    public void testV4Signature__get_utf8() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "GET", "https://host.foo.com/%E1%88%B4", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=8d6634c189aa8c75c2e51e106b6b5121bed103fdb351f7d7d4381c738823af74", authz);
    }

    @Test
    public void testV4Signature__get_vanilla() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "GET", "https://host.foo.com/", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=b27ccfbfa7df52a200ff74193ca6e32d4b48b8856fab7ebf1c595d0670a7e470", authz);
    }

    @Test
    public void testV4Signature__get_vanilla_empty_query_key() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "GET", "https://host.foo.com/?foo=bar", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=56c054473fd260c13e4e7393eb203662195f5d4a1fada5314b8b52b23f985e9f", authz);
    }

    @Test
    public void testV4Signature__get_vanilla_query() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "GET", "https://host.foo.com/", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=b27ccfbfa7df52a200ff74193ca6e32d4b48b8856fab7ebf1c595d0670a7e470", authz);
    }

    @Test
    public void testV4Signature__get_vanilla_query_order_key() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "GET", "https://host.foo.com/?a=foo&b=foo", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=0dc122f3b28b831ab48ba65cb47300de53fbe91b577fe113edac383730254a3b", authz);
    }

    @Test
    public void testV4Signature__get_vanilla_query_order_key_case() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "GET", "https://host.foo.com/?foo=Zoo&foo=aha", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=be7148d34ebccdc6423b19085378aa0bee970bdc61d144bd1a8c48c33079ab09", authz);
    }

    @Test
    public void testV4Signature__get_vanilla_query_order_value() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "GET", "https://host.foo.com/?foo=b&foo=a", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=feb926e49e382bec75c9d7dcb2a1b6dc8aa50ca43c25d2bc51143768c0875acc", authz);
    }

    @Test
    public void testV4Signature__get_vanilla_query_unreserved() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "GET", "https://host.foo.com/?-._~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz=-._~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=f1498ddb4d6dae767d97c466fb92f1b59a2c71ca29ac954692663f9db03426fb", authz);
    }

    @Test
    @Ignore("AWS4 signature feature we don't care about")
    public void testV4Signature__get_vanilla_ut8_query() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "GET", "https://host.foo.com/?foo=bar", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=6fb359e9a05394cc7074e0feb42573a2601abc0c869a953e8c5c12e4e01f1a8c", authz);
    }

    @Test
    public void testV4Signature__post_header_key_case() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("DATE", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "POST", "https://host.foo.com/", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=22902d79e148b64e7571c3565769328423fe276eae4b26f83afceda9e767f726", authz);
    }

    @Test
    public void testV4Signature__post_header_key_sort() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("DATE", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("host", "host.foo.com");
        headers.put("ZOO", "zoobar");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "POST", "https://host.foo.com/", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host;zoo, Signature=b7a95a52518abbca0964a999a880429ab734f35ebbf1235bd79a5de87756dc4a", authz);
    }

    @Test
    public void testV4Signature__post_header_value_case() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("DATE", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("host", "host.foo.com");
        headers.put("zoo", "ZOOBAR");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "POST", "https://host.foo.com/", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host;zoo, Signature=273313af9d0c265c531e11db70bbd653f3ba074c1009239e8559d3987039cad7", authz);
    }

    @Test
    public void testV4Signature__post_vanilla() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "POST", "https://host.foo.com/", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=22902d79e148b64e7571c3565769328423fe276eae4b26f83afceda9e767f726", authz);
    }

    @Test
    public void testV4Signature__post_vanilla_empty_query_value() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "POST", "https://host.foo.com/?foo=bar", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=b6e3b79003ce0743a491606ba1035a804593b0efb1e20a11cba83f8c25a57a92", authz);
    }

    @Test
    public void testV4Signature__post_vanilla_query() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "POST", "https://host.foo.com/?foo=bar", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=b6e3b79003ce0743a491606ba1035a804593b0efb1e20a11cba83f8c25a57a92", authz);
    }

    @Test
    @Ignore("AWS4 signature feature we don't care about")
    public void testV4Signature__post_vanilla_query_nonunreserved() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "POST", "https://host.foo.com/?@#$%^&+=/,?><`\";:\\|][{}", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=28675d93ac1d686ab9988d6617661da4dffe7ba848a2285cb75eac6512e861f9", authz);
    }

    @Test
    public void testV4Signature__post_vanilla_query_space() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "POST", "https://host.foo.com/?f", "host", headers, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=b7eb653abe5f846e7eee4d1dba33b15419dc424aaf215d49b1240732b10cc4ca", authz);
    }

    @Test
    public void testV4Signature__post_x_www_form_urlencoded() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Content-Type", "application/x-www-form-urlencoded");
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "POST", "https://host.foo.com/", "host", headers, "3ba8907e7a252327488df390ed517c45b96dead033600219bdca7107d1d3f88a");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=content-type;date;host, Signature=5a15b22cf462f047318703b92e6f4f38884e4a7ab7b1d6426ca46a8bd1c26cbc", authz);
    }

    @Test
    public void testV4Signature__post_x_www_form_urlencoded_parameters() throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Content-Type", "application/x-www-form-urlencoded; charset=utf8");
        headers.put("Date", "Mon, 09 Sep 2011 23:36:00 GMT");
        headers.put("Host", "host.foo.com");
        String authz = awsCloud.getV4Authorization("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "POST", "https://host.foo.com/", "host", headers, "3ba8907e7a252327488df390ed517c45b96dead033600219bdca7107d1d3f88a");
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=content-type;date;host, Signature=b105eb10c6d318d2294de9d49dd8b031b55e3c3fe139f2e637da70511e9e7b71", authz);
    }



}
