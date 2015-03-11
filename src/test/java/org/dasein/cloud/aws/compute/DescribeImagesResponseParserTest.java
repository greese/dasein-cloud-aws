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

package org.dasein.cloud.aws.compute;

import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.ImageFilterOptions;
import org.dasein.cloud.compute.MachineImage;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

/**
 * Unit tests for streaming parser used to parse DescribeImages response.
 * <p>Created by Stas Maksimov: 11/04/2014 21:22</p>
 *
 * @author Stas Maksimov
 * @version 2014.03 initial version
 * @since 2014.03
 * @see org.dasein.cloud.aws.compute.DescribeImagesResponseParser
 */
public class DescribeImagesResponseParserTest {
    final static String OWNER_ID = "979382823631";
    final static String STATE_REASON = "Client.SomeAction: Some action has occurred";
    final static String IMAGE_ID = "ami-image2";
    final static String XML_IMAGE_1 = "        <item>\n" +
                    "            <imageId>ami-image1</imageId>\n" +
                    "            <imageLocation>bitnami-cloud-eu/concrete5/bitnami-concrete5-5.6.1.1-0-linux-ubuntu-12.04.2-x86_64-s3.manifest.xml</imageLocation>\n" +
                    "            <imageState>available</imageState>\n" +
                    "            <imageOwnerId>"+OWNER_ID+"</imageOwnerId>\n" +
                    "            <isPublic>true</isPublic>\n" +
                    "            <architecture>x86_64</architecture>\n" +
                    "            <imageType>machine</imageType>\n" +
                    "            <kernelId>aki-71665e05</kernelId>\n" +
                    "            <rootDeviceType>instance-store</rootDeviceType>\n" +
                    "            <blockDeviceMapping/>\n" +
                    "            <virtualizationType>paravirtual</virtualizationType>\n" +
                    "            <hypervisor>xen</hypervisor>\n" +
                    "        </item>\n";

    final static String XML_IMAGE_2 = "        <item>\n" +
                    "            <imageId>" + IMAGE_ID + "</imageId>\n" +
                    "            <imageLocation>bitnami-cloud-eu/concrete5/bitnami-concrete5-5.6.1.1-0-linux-ubuntu-12.04.2-x86_64-s3.manifest.xml</imageLocation>\n" +
                    "            <imageState>available</imageState>\n" +
                    "            <imageOwnerId>979382823631</imageOwnerId>\n" +
                    "            <isPublic>true</isPublic>\n" +
                    "            <architecture>x86_64</architecture>\n" +
                    "            <imageType>machine</imageType>\n" +
                    "            <kernelId>aki-71665e05</kernelId>\n" +
                    "            <rootDeviceType>instance-store</rootDeviceType>\n" +
                    "            <blockDeviceMapping>\n" +
                    "               <item>\n" +
                    "                   <deviceName>sdb</deviceName>\n" +
                    "               </item>\n" +
                    "               <item>\n" +
                    "                   <deviceName>sdc</deviceName>\n" +
                    "               </item>\n" +
                    "            </blockDeviceMapping>\n" +
                    "            <virtualizationType>paravirtual</virtualizationType>\n" +
                    "            <hypervisor>xen</hypervisor>\n" +
                    "            <stateReason>\n" +
                    "                <code>Client.SomeCode</code>\n" +
                    "                <message>" + STATE_REASON + "</message>\n" +
                    "            </stateReason>\n" +
                    "            <tagSet>\n" +
                    "                <item>\n" +
                    "                    <key>Name</key>\n" +
                    "                    <value>image-name</value>\n" +
                    "                </item>\n" +
                    "                <item>\n" +
                    "                    <key>Description</key>\n" +
                    "                    <value>image-description</value>\n" +
                    "                </item>\n" +
                    "                <item>\n" +
                    "                    <key>Empty</key>\n" +
                    "                    <value></value>\n" +
                    "                </item>\n" +
                    "            </tagSet>\n" +
                    "        </item>\n";

    final static String TEST_XML_TWO_IMAGES = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<DescribeImagesResponse xmlns=\"http://ec2.amazonaws.com/doc/2013-06-15/\">\n" +
            "    <requestId>f048defe-4a1f-4c77-9001-5c74fc8b61d7</requestId>\n" +
            "    <imagesSet>\n" +
            XML_IMAGE_1 +
            XML_IMAGE_2 +
            "    </imagesSet>\n" +
            "</DescribeImagesResponse>\n";

    final static String TEST_XML_ONE_IMAGE = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<DescribeImagesResponse xmlns=\"http://ec2.amazonaws.com/doc/2013-06-15/\">\n" +
            "    <requestId>f048defe-4a1f-4c77-9001-5c74fc8b61d7</requestId>\n" +
            "    <imagesSet>\n" +
            XML_IMAGE_2 +
            "    </imagesSet>\n" +
            "</DescribeImagesResponse>\n";

    private static List<MachineImage> singleImageResults = new ArrayList<MachineImage>();

    @BeforeClass
    public static void beforeClass() throws Exception {
        ImageFilterOptions filterOptions = ImageFilterOptions.getInstance().withImageClass(ImageClass.MACHINE);

        new DescribeImagesResponseParser("test-region", null, filterOptions, singleImageResults).parse(new ByteArrayInputStream(TEST_XML_ONE_IMAGE.getBytes()));
    }

    @Test
    public void testParseMultiImages() throws Exception {
        ImageFilterOptions filterOptions = ImageFilterOptions.getInstance().withImageClass(ImageClass.MACHINE);
        List<MachineImage> list = new ArrayList<MachineImage>();
        new DescribeImagesResponseParser("test-region", null, filterOptions, list).parse(new ByteArrayInputStream(TEST_XML_TWO_IMAGES.getBytes()));
        assertEquals("Incorrect number of images parsed", 2, list.size());
        assertNotSame("Images should be different", list.get(0), list.get(1));
    }

    @Test
    public void testParseStateReason() throws Exception {
        assertEquals("Incorrect number of images parsed", 1, singleImageResults.size());
        MachineImage image1 = singleImageResults.get(0);
        assertEquals("State reason didn't parse correctly.", STATE_REASON, image1.getTag("stateReason"));
    }

    @Test
    public void testParseOwnerId() throws Exception {
        assertEquals("Incorrect number of images parsed", 1, singleImageResults.size());
        MachineImage image1 = singleImageResults.get(0);
        assertEquals(OWNER_ID, image1.getProviderOwnerId());
    }

    @Test
    public void testParseImageId() throws Exception {
        assertEquals("Incorrect number of images parsed", 1, singleImageResults.size());
        MachineImage image1 = singleImageResults.get(0);
        assertEquals(IMAGE_ID, image1.getProviderMachineImageId());
    }

    @Test
    public void testParseTags() throws Exception {
        assertEquals("Incorrect number of images parsed", 1, singleImageResults.size());
        MachineImage image1 = singleImageResults.get(0);
        assertEquals("Tag 'image-name' is not found or is not parsed correctly", "image-name", image1.getTag("Name"));
        assertEquals("Tag 'image-description' is not found or is not parsed correctly", "image-description", image1.getTag("Description"));
    }

    @Test
    public void testParseTagWithEmptyValue() throws Exception {
        assertEquals("Incorrect number of images parsed", 1, singleImageResults.size());
        MachineImage image1 = singleImageResults.get(0);
        assertEquals("Tag 'Empty' is not found or is not parsed correctly", "", image1.getTag("Empty"));
    }

}