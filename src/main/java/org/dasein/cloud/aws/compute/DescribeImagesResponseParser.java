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

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.compute.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static javax.xml.stream.XMLStreamConstants.END_ELEMENT;

/**
 * Description
 * <p>Created by Stas Maksimov: 17/04/2014 19:18</p>
 *
 * @author Stas Maksimov
 * @version 2014.03 initial version
 * @since 2014.03
 */
public class DescribeImagesResponseParser implements XmlStreamParser<MachineImage> {

    private final List<MachineImage> list;
    private final String providerOwnerId;
    private final String regionId;
    private final ImageFilterOptions filterOptions;
    private int itemDepth;

    public DescribeImagesResponseParser(@Nonnull String regionId,
                                        @Nullable String providerOwnerId,
                                        @Nullable ImageFilterOptions filterOptions,
                                        @Nonnull List<MachineImage> list) {
        this.providerOwnerId = providerOwnerId;
        this.regionId = regionId;
        this.filterOptions = filterOptions;
        this.list = list;
    }

    @Override
    public List<MachineImage> parse( InputStream stream ) throws IOException, CloudException, InternalException {
        XMLStreamReader reader = null;
        try {
            XMLInputFactory factory = XMLInputFactory.newInstance();
            reader = factory.createXMLStreamReader(stream);

            // skip DescribeImageResponse, skip ImagesSet, iterate over all items
            skipToNext("imagesSet", reader);

            while ( reader.hasNext() ) {
                int event = reader.next();
                switch( event ) {
                    case XMLStreamConstants.START_ELEMENT:
                        String name = reader.getLocalName();
                        if( "item".equalsIgnoreCase(name) ) {
                            itemDepth++;
                            MachineImage image = readItem(reader);
                            if( image != null && ( filterOptions != null && filterOptions.matches(image) ) ) {
                                list.add(image);
                            }
                        }
                        break;
                    case END_ELEMENT:
                        name = reader.getLocalName();
                        if( "imagesSet".equalsIgnoreCase(name) ) {
                            return list; // done with the images
                        }
                        break;
                }
            }
        }
        catch( XMLStreamException e ) {
            throw new CloudException(e);
        } finally {
            if( reader != null ) {
                try {
                    reader.close();
                } catch( XMLStreamException e ) {
                    // Ignore
                }
            }
        }
        return list;
    }

    private int skipToNext( String imagesSet, XMLStreamReader reader ) throws XMLStreamException {
        while ( reader.hasNext() ) {
            int event = reader.next();
            switch( event ) {
                case XMLStreamConstants.START_ELEMENT:
                    String name = reader.getLocalName();
                    if( "imagesSet".equalsIgnoreCase(name) ) {
                        return event;
                    }
            }
        }
        return 0;
    }

    private @Nullable MachineImage readItem(@Nullable XMLStreamReader parser) throws CloudException, InternalException, XMLStreamException {
        if( parser == null ) {
            return null;
        }
        String location = null;
        ImageClass imageClass = ImageClass.MACHINE;
        MachineImageState imageState = MachineImageState.ACTIVE;
        MachineImageType imageType = MachineImageType.STORAGE;
        MachineImageFormat imageFormat = null;
        String providerMachineImageId = null;
        String providerOwnerId = null;
        Map<String, String> tags = new HashMap<String, String>();
        Architecture architecture = Architecture.I64;
        Platform platform = Platform.UNKNOWN;
        String imageName = null;
        String description = null;
        boolean isPublic = false;
        boolean itemEnd = false;
        String virtualizationType = null;
        String hypervisor = null;
        String value = null;

        for( int event = parser.next(); event != XMLStreamConstants.END_DOCUMENT && !itemEnd; event = parser.next() ) {
            switch( event ) {
                case XMLStreamConstants.START_ELEMENT:
                    String name = parser.getLocalName();
                    if( "stateReason".equalsIgnoreCase(name) ) {
                        String stateReason = readStateReason(parser);
                        if( stateReason != null ) {
                            tags.put("stateReason", stateReason);
                        }
                    }
                    else if( "tagSet".equalsIgnoreCase(name) ) {
                        readTags(parser, tags);
                    }
                    else if( "item".equalsIgnoreCase(name) ) {
                        itemDepth++;
                    }
                    break;

                case XMLStreamConstants.CHARACTERS:
                    value = parser.getText().trim();
                    break;

                case XMLStreamConstants.END_ELEMENT: {
                    name = parser.getLocalName();
                    if( "imageType".equals(name) ) {
                        if( value.equals("machine") ) {
                            imageClass = ImageClass.MACHINE;
                        }
                        else if( value.equals("kernel") ) {
                            imageClass = ImageClass.KERNEL;
                        }
                        else if( value.equals("ramdisk") ) {
                            imageClass = ImageClass.RAMDISK;
                        }
                    }
                    else if( "imageId".equals(name) ) {
                        providerMachineImageId = value;
                        if( providerMachineImageId.startsWith("ami") ) {
                            imageClass = ImageClass.MACHINE;
                        }
                        else if( providerMachineImageId.startsWith("aki") ) {
                            imageClass = ImageClass.KERNEL;
                        }
                        else if( providerMachineImageId.startsWith("ari") ) {
                            imageClass = ImageClass.RAMDISK;
                        }
                    }
                    else if( "imageLocation".equals(name) ) {
                        location = value;
                    }
                    else if( "imageState".equals(name) ) {
                        if( "available".equalsIgnoreCase(value) ) {
                            imageState = MachineImageState.ACTIVE;
                        }
                        else if( "failed".equalsIgnoreCase(value) ) {
                            imageState = MachineImageState.DELETED;
                        }
                        else {
                            imageState = MachineImageState.PENDING;
                        }
                    }
                    else if( "imageOwnerId".equals(name) ) {
                        providerOwnerId = value;
                    }
                    else if( "isPublic".equals(name) ) {
                        if( value != null && value.trim().equalsIgnoreCase("true")) {
                            isPublic = true;
                        }
                        tags.put("public", value);
                    }
                    else if( "architecture".equals(name) ) {
                        if( "i386".equals(value) ) {
                            architecture = Architecture.I32;
                        }
                    }
                    else if( "platform".equals(name) ) {
                        platform = Platform.guess(value);
                    }
                    else if( "name".equals(name) ) {
                        imageName = value;
                    }
                    else if( "description".equals(name) ) {
                        description = value;
                    }
                    else if( "rootDeviceType".equals(name) ) {
                        if( "ebs".equalsIgnoreCase(value) ) {
                            imageType = MachineImageType.VOLUME;
                        }
                        else {
                            imageType = MachineImageType.STORAGE;
                            imageFormat = MachineImageFormat.AWS;
                        }
                    }
                    else if( "virtualizationType".equals(name) ) {
                        tags.put("virtualizationType", value);
                    }
                    else if( "hypervisor".equals(name) ) {
                        tags.put("hypervisor", value);
                    }
                    else if( "tagSet".equals(name) ) {
                        readTags(parser, tags);
                    }
                    else if( "item".equals(name) ) {
                        itemDepth--;
                        if( itemDepth == 0 ) {
                            itemEnd = true;
                        }
                    }
                    break;
                }
            } // switch
        } // for

        if( platform == null ) {
            platform = Platform.guess(location);
        }

        if( location != null ) {
            String[] parts = location.split("/");

            if( parts != null && parts.length > 1 ) {
                location = parts[parts.length - 1];
            }
            int i = location.indexOf(".manifest.xml");

            if( i > -1 ) {
                location = location.substring(0, i);
            }
            if( imageName == null || imageName.isEmpty() ) {
                imageName = location;
            }
            if( platform == null || platform.equals(Platform.UNKNOWN) ) {
                platform = Platform.guess(imageName);
            }
            if( description == null || description.isEmpty() ) {
                description = AMI.createDescription(location, architecture, platform);
            }
        }

        if( imageName == null || imageName.isEmpty() ) {
            imageName = providerMachineImageId;
        }
        if( platform == null || platform.equals(Platform.UNKNOWN) ) {
            platform = Platform.guess(imageName);
        }
        if( description == null || description.isEmpty() ) {
            description = AMI.createDescription(imageName, architecture, platform);
        }
        if( platform == null || platform.equals(Platform.UNKNOWN) ) {
            platform = Platform.guess(description);
        }

        if( this.providerOwnerId != null) {
            providerOwnerId = this.providerOwnerId;
        }

        MachineImage image = MachineImage.getInstance(providerOwnerId, regionId, providerMachineImageId, imageClass, imageState, imageName, description, architecture, platform);

        if( imageType != null ) {
            image.withType(imageType);
        }
        if( imageFormat != null ) {
            image.withStorageFormat(imageFormat);
        }
        if( isPublic ) {
            image.sharedWithPublic();
        }
        image.setTags(tags);
        return image;
    }

    private void readTags( XMLStreamReader parser, Map<String, String> tags ) throws XMLStreamException {
        String tagKey = null;
        String tagValue = null;
        String value = null;
        while( parser.hasNext() ) {
            int event = parser.next();
            switch( event ) {
                case XMLStreamConstants.CHARACTERS:
                    value = parser.getText().trim();
                    break;
                case XMLStreamConstants.START_ELEMENT:
                    if( "item".equalsIgnoreCase(parser.getLocalName()) ) {
                        itemDepth++;
                    }
                    break;
                case XMLStreamConstants.END_ELEMENT:
                    String name = parser.getLocalName();
                    if( "key".equalsIgnoreCase(name) ) {
                        tagKey = value;
                    }
                    else if( "value".equalsIgnoreCase(name) ) {
                        tagValue = value;
                    }
                    else if( "item".equalsIgnoreCase(name) ) {
                        if( tagKey != null && tagValue != null ) {
                            tags.put(tagKey, tagValue);
                        }
                        itemDepth--;
                        value = tagValue = tagKey = null;
                    }
                    else if( "tagSet".equalsIgnoreCase(name) ) {
                        return;
                    }
                    break;
            }
        }
    }

    private String readStateReason( XMLStreamReader parser ) throws XMLStreamException {
        String value = null;
        String returnValue = null;
        while( parser.hasNext() ) {
            int event = parser.next();
            switch( event ) {
                case XMLStreamConstants.CHARACTERS:
                    value = parser.getText().trim();
                    break;
                case XMLStreamConstants.END_ELEMENT:
                    String name = parser.getLocalName();
                    if( "message".equalsIgnoreCase(name) ) {
                        returnValue = value;
                        break;
                    }
                    else if( "stateReason".equalsIgnoreCase(name) ) {
                        return returnValue;
                    }
                    break;
            }
        }
        return null;
    }


}
