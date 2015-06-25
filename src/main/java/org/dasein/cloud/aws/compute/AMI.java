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

import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.storage.S3Method;
import org.dasein.cloud.compute.*;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.util.APITrace;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorPopulator;
import org.dasein.util.PopulatorThread;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @version 2013.01.1 Fixed a data consistency issue with AWS (issue #21)
 */
public class AMI extends AbstractImageSupport<AWSCloud> {
	static private final Logger logger = Logger.getLogger(AMI.class);
	
    private volatile transient AMICapabilities capabilities;

    AMI(AWSCloud provider) {
		super(provider);
	}

    @Override
    public void addImageShare(@Nonnull String providerImageId, @Nonnull String accountNumber) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.addImageShare");
        try {
            setPrivateShare(providerImageId, true, accountNumber);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void addPublicShare(@Nonnull String providerImageId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.addPublicShare");
        try {
            setPublicShare(providerImageId, true);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public ImageCapabilities getCapabilities() {
        if( capabilities == null ) {
            capabilities = new AMICapabilities(getProvider());
        }
        return capabilities;
    }

    @Override
    protected MachineImage capture(@Nonnull ImageCreateOptions options, @Nullable AsynchronousTask<MachineImage> task) throws CloudException, InternalException {
        ProviderContext ctx = getProvider(). getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        return captureImage(ctx, options, task);
    }
    
    private @Nonnull MachineImage captureImage(@Nonnull ProviderContext ctx, @Nonnull ImageCreateOptions options, @Nullable AsynchronousTask<MachineImage> task) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "captureImage");
        try {
            if( task != null ) {
                task.setStartTime(System.currentTimeMillis());
            }
            VirtualMachine vm = null;

            long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 30L);

            while( timeout > System.currentTimeMillis() ) {
                try {
                    //noinspection ConstantConditions
                    vm = getProvider(). getComputeServices().getVirtualMachineSupport().getVirtualMachine(options.getVirtualMachineId());
                    if( vm == null || VmState.TERMINATED.equals(vm.getCurrentState()) ) {
                        break;
                    }
                    
                    if( VmState.RUNNING.equals(vm.getCurrentState()) || VmState.STOPPED.equals(vm.getCurrentState()) ) {
                        break;
                    }
                    
                    if( !vm.isPersistent() ) {
                    	if( vm.getPlatform().isWindows() ) {
                           	String bucket = getProvider(). getStorageServices().getOnlineStorageSupport().createBucket("dsnwin" + (System.currentTimeMillis() % 10000), true).getBucketName();
                            if( bucket == null ) {
                                throw new CloudException("There is no bucket");
                            }
                            return captureWindows(getProvider(). getContext(), options, bucket, task);
                        }
                    }

                }
                catch( Throwable ignore ) {
                    // ignore
                }
                try { Thread.sleep(15000L); }
                catch( InterruptedException ignore ) { }
            }
            if( vm == null ) {
                throw new CloudException("No such virtual machine: " + options.getVirtualMachineId());
            }
            String lastMessage = null;
            int attempts = 5;

            while( attempts > 0 ) {
                Map<String,String> parameters = getProvider(). getStandardParameters(getProvider(). getContext(), EC2Method.CREATE_IMAGE);
                NodeList blocks;
                EC2Method method;
                Document doc;

                /* need to perform the opposite of "reboot" as Amazon's API takes "NoReboot"
                  Therefore:
                  If reboot == false, then NoReboot = true
                  If reboot == true, then NoReboot = false
                 */
                Boolean reboot = options.getReboot();
                if( reboot != null ) {
                  reboot = !reboot;
                  parameters.put("NoReboot", reboot.toString());
                }

                parameters.put("InstanceId", options.getVirtualMachineId());
                parameters.put("Name", options.getName());
                parameters.put("Description", options.getDescription());
                method = new EC2Method(getProvider(), parameters);
                try {
                    doc = method.invoke();
                }
                catch( EC2Exception e ) {
                    logger.error(e.getSummary());
                    throw new CloudException(e);
                }
                blocks = doc.getElementsByTagName("imageId");
                if( blocks.getLength() > 0 ) {
                    Node imageIdNode = blocks.item(0);
                    String id = imageIdNode.getFirstChild().getNodeValue().trim();
                    MachineImage img = getImage(id);

                    if( img == null ) {
                        for( int i=0; i<5; i++ ) {
                            try { Thread.sleep(5000L * i); }
                            catch( InterruptedException ignore ) { }
                            img = getImage(id);
                            if( img != null ) {
                                break;
                            }
                        }
                        if( img == null ) {
                            throw new CloudException("No image exists for " + id + " as created during the capture process");
                        }
                    }
                    if( MachineImageState.DELETED.equals(img.getCurrentState()) ) {
                        String errorMessage = (String)img.getTag("stateReason");

                        if( errorMessage != null ) {
                            if( errorMessage.contains("try again") ) {
                                lastMessage = errorMessage;
                                attempts--;
                                try { Thread.sleep(CalendarWrapper.MINUTE); }
                                catch( InterruptedException ignore ) { }
                                continue;
                            }
                            throw new CloudException(errorMessage);
                        }
                    }
                    
                    // Add tags
                    List<Tag> tags = new ArrayList<Tag>();
                    Map<String, Object> meta = options.getMetaData();
                    meta.put("Name", options.getName());
                    meta.put("Description", options.getDescription());
                    for( Map.Entry<String, Object> entry : meta.entrySet() ) 
                        tags.add(new Tag(entry.getKey(), entry.getValue() == null ? "" : entry.getValue().toString()));

                    if( !tags.isEmpty() ) {
                    	getProvider().createTags(EC2Method.SERVICE_ID, id, tags.toArray(new Tag[tags.size()]));
                    }     
                    return img;
                }
                throw new CloudException("No error occurred during imaging, but no machine image was specified");
            }
            if( lastMessage == null ) {
                lastMessage = "Unknown error";
            }
            throw new CloudException(lastMessage);
        }
        finally {
            APITrace.end();
        }
    }

    private MachineImage captureWindows(@Nonnull ProviderContext ctx, @Nonnull ImageCreateOptions options, @Nonnull String bucket, @Nullable AsynchronousTask<MachineImage> task) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.captureWindows");
        try {
            Map<String,String> parameters = getProvider(). getStandardParameters(getProvider(). getContext(), EC2Method.BUNDLE_INSTANCE);
            StringBuilder uploadPolicy = new StringBuilder();
            NodeList blocks;
            EC2Method method;
            Document doc;

            uploadPolicy.append("{");
            uploadPolicy.append("\"expiration\":\"");
            {
                SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                Date date = new Date(System.currentTimeMillis() + (CalendarWrapper.HOUR*12L));

                uploadPolicy.append(fmt.format(date));
            }
            uploadPolicy.append("\",\"conditions\":");
            {
                uploadPolicy.append("[");
                uploadPolicy.append("{\"bucket\":\"");
                uploadPolicy.append(bucket);
                uploadPolicy.append("\"},");
                uploadPolicy.append("{\"acl\": \"ec2-bundle-read\"},");
                uploadPolicy.append("[\"starts-with\", \"$key\", \"");
                uploadPolicy.append(options.getName());
                uploadPolicy.append("\"]");
                uploadPolicy.append("]");
            }
            uploadPolicy.append("}");
            String base64Policy;

            try {
                base64Policy = S3Method.toBase64(uploadPolicy.toString().getBytes("utf-8"));
            }
            catch( UnsupportedEncodingException e ) {
                logger.error(e);
                e.printStackTrace();
                throw new InternalException(e);
            }
            parameters.put("InstanceId", options.getVirtualMachineId());
            parameters.put("Storage.S3.Bucket", bucket);
            parameters.put("Storage.S3.Prefix", options.getName());
            parameters.put("Storage.S3.AWSAccessKeyId", ctx.getAccountNumber());
            parameters.put("Storage.S3.UploadPolicy", base64Policy);
            parameters.put("Storage.S3.UploadPolicySignature", getProvider(). signUploadPolicy(base64Policy));
            method = new EC2Method(getProvider(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("bundleId");
            if( blocks.getLength() < 1 ) {
                throw new CloudException("Unable to identify the bundle task ID.");
            }
            String bundleId = blocks.item(0).getFirstChild().getNodeValue();
            String manifest = (bucket + "/" + options.getName() + ".manifest.xml");

            if( task == null ) {
                task = new AsynchronousTask<MachineImage>();
                task.setStartTime(System.currentTimeMillis());
            }
            waitForBundle(bundleId, manifest, options.getPlatform(), options.getName(), options.getDescription(), task);

            Throwable t = task.getTaskError();

            if( t != null ) {
                if( t instanceof CloudException ) {
                    throw (CloudException)t;
                }
                else if( t instanceof InternalException ) {
                    throw (InternalException)t;
                }
                throw new InternalException(t);
            }
            MachineImage img = task.getResult();

            if( img == null ) {
                throw new CloudException("No image was created, but no error was given");
            }
            return img;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String copyImage(@Nonnull ImageCopyOptions options) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "copyImage");
        AWSCloud targetProvider = null;
        try {
            /* Steps overview:
             * 1. Connect to target region using the same account
             * 2. Invoke EC2 'copyImage' method in the context of target region
             */
            final ProviderContext ctx = getProvider(). getContext();
            if( ctx == null ) {
                throw new CloudException( "Provider context is necessary for this request" );
            }
            final String sourceRegionId = ctx.getRegionId();
            final String targetRegionId = options.getTargetRegionId();

            final ProviderContext targetContext = ctx.copy(targetRegionId);
            targetProvider = ( AWSCloud ) targetContext.connect();
            if ( targetProvider.testContext() == null ) {
                throw new CloudException( "Could not connect with the same account to the copy target region: " +
                                                  targetRegionId );
            }

            // Invoke the EC2 method
            Map<String,String> parameters = targetProvider.getStandardParameters(
                    targetProvider.getContext(), EC2Method.COPY_IMAGE);

            parameters.put( "SourceRegion", sourceRegionId );
            parameters.put( "SourceImageId", options.getProviderImageId() );
            if (options.getName() != null) {
                parameters.put( "Name", options.getName() );
            }
            if (options.getDescription() != null) {
                parameters.put( "Description", options.getDescription() );
            }

            Document doc;
            try {
                EC2Method method = new EC2Method(targetProvider, parameters);
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            NodeList blocks = doc.getElementsByTagName( "imageId" );
            if( blocks.getLength() > 0 ) {
                Node imageIdNode = blocks.item(0);
                return imageIdNode.getFirstChild().getNodeValue().trim();
            }
            throw new CloudException( "No error occurred during imaging, but no machine image was specified" );
        }
        finally {
            if ( targetProvider != null ) {
                targetProvider.close();
            }
            APITrace.end();
        }
    }

    private @Nonnull Iterable<MachineImage> executeImageSearch(int pass, boolean forPublic, @Nonnull ImageFilterOptions options) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.executeImageSearch");
        try {
            final ProviderContext ctx = getProvider(). getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            final String regionId = ctx.getRegionId();
            if( regionId == null ) {
                throw new CloudException("No region was set for this request");
            }

            Architecture architecture = options.getArchitecture();

            if( architecture != null && !architecture.equals(Architecture.I32) && !architecture.equals(Architecture.I64) ) {
                if( !options.isMatchesAny() ) {
                    return Collections.emptyList();
                }
            }
            Map<String,String> parameters = getProvider(). getStandardParameters(getProvider(). getContext(), EC2Method.DESCRIBE_IMAGES);

            final List<MachineImage> list = new ArrayList<MachineImage>();

            if( forPublic ) {
                if( pass == 1 ) {
                    parameters.put("ExecutableBy.1", "all");
                }
                else {
                    parameters.put("ExecutableBy.1", "self");
                }
            }
            else {
                if( pass ==  1 ) {
                    parameters.put("ExecutableBy.1", "self");
                }
                else {
                    parameters.put("Owner", "self");
                }
            }
            final ImageFilterOptions finalOptions = fillImageFilterParameters(forPublic, options, parameters);

            EC2Method method = new EC2Method(getProvider(), parameters);
            try {
                method.invoke(
                        new DescribeImagesResponseParser(
                                getProvider(). getContext().getRegionId(),
                                (getProvider(). getEC2Provider().isAWS() ? null : getProvider(). getContext().getAccountNumber()),
                                finalOptions,
                                list));
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            return list;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nullable MachineImage getImage(@Nonnull String providerImageId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.getImage");
        try {
            ProviderContext ctx = getProvider(). getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            if( getProvider(). getEC2Provider().isAWS() ) {
                Map<String,String> parameters = getProvider(). getStandardParameters(ctx, EC2Method.DESCRIBE_IMAGES);
                NodeList blocks;
                EC2Method method;
                Document doc;

                parameters.put("ImageId", providerImageId);
                method = new EC2Method(getProvider(), parameters);
                try {
                    doc = method.invoke();
                }
                catch( EC2Exception e ) {
                    String code = e.getCode();

                    if( code != null && code.startsWith("InvalidAMIID") ) {
                        return null;
                    }
                    logger.error(e.getSummary());
                    throw new CloudException(e);
                }
                blocks = doc.getElementsByTagName("imagesSet");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    NodeList instances = blocks.item(i).getChildNodes();

                    for( int j=0; j<instances.getLength(); j++ ) {
                        Node instance = instances.item(j);

                        if( instance.getNodeName().equals("item") ) {
                            MachineImage image = toMachineImage(instance);

                            if( image != null && image.getProviderMachineImageId().equals(providerImageId) ) {
                                return image;
                            }
                        }
                    }
                }
                return null;
            }
            else {
                ImageFilterOptions options = ImageFilterOptions.getInstance();

                for( MachineImage image : searchPublicImages(options) ) {
                    if( image.getProviderMachineImageId().equals(providerImageId) ) {
                        return image;
                    }
                }
                for( MachineImage image : listImages(options) ) {
                    if( image.getProviderMachineImageId().equals(providerImageId) ) {
                        return image;
                    }
                }
                return null;
            }
        }
        finally {
            APITrace.end();
        }
    }
	
    @Override
    public boolean isImageSharedWithPublic(@Nonnull String machineImageId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.isImageSharedWithPublic");
        try {
            MachineImage image = getMachineImage(machineImageId);

            if( image == null ) {
                return false;
            }
            // checking the flag should be enough
            if( image.isPublic() ) {
                return true;
            }
            // but just in case check the legacy tag
            String p = (String)image.getTag("public");
            return (p != null && p.equalsIgnoreCase("true"));
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.isSubscribed");
        try {
            Map<String,String> parameters = getProvider(). getStandardParameters(getContext(), EC2Method.DESCRIBE_IMAGES);
            if( getProvider(). getEC2Provider().isAWS() ) {
                parameters.put("Owner", getContext().getAccountNumber());
            }
            EC2Method method = new EC2Method(getProvider(), parameters);
            try {
                method.invoke();
                return true;
            }
            catch( EC2Exception e ) {
                String msg = e.getSummary();

                if( msg != null && msg.contains("not able to validate the provided access credentials") ) {
                    return false;
                }
                logger.error("AWS Error checking subscription: " + e.getCode() + "/" + e.getSummary());
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

    public @Nonnull Iterable<ResourceStatus> listImageStatus(final @Nonnull ImageClass cls) throws CloudException, InternalException {
            getProvider(). hold();
            PopulatorThread<ResourceStatus> populator = new PopulatorThread<ResourceStatus>(new JiteratorPopulator<ResourceStatus>() {
                @Override
                public void populate(@Nonnull Jiterator<ResourceStatus> iterator) throws Exception {
                    APITrace.begin(getProvider(), "Image.listImageStatus");
                    try {
                        try {
                            TreeSet<String> ids = new TreeSet<String>();

                            for( ResourceStatus status : executeStatusList(1, cls) ) {
                                ids.add(status.getProviderResourceId());
                                iterator.push(status);
                            }
                            for( ResourceStatus status : executeStatusList(2, cls) ) {
                                if( !ids.contains(status.getProviderResourceId()) ) {
                                    iterator.push(status);
                                }
                            }
                        }
                        finally {
                            getProvider(). release();
                        }
                    }
                    finally {
                        APITrace.end();
                    }
                }
            });

            populator.populate();
            return populator.getResult();

    }

    private @Nonnull Iterable<ResourceStatus> executeStatusList(int pass, @Nonnull ImageClass cls) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.executeStatusList");
        try {
            ProviderContext ctx = getProvider(). getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }

            Map<String,String> parameters = getProvider(). getStandardParameters(getProvider(). getContext(), EC2Method.DESCRIBE_IMAGES);
            ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();
            EC2Method method;
            NodeList blocks;
            Document doc;

            String accountNumber = ctx.getAccountNumber();

            if( pass ==  1 ) {
                parameters.put("ExecutableBy.1", "self");
            }
            else if( getProvider(). getEC2Provider().isAWS() ) {
                parameters.put("Owner", "self");
            }

            String t = "machine";

            switch( cls ) {
                case MACHINE: t = "machine"; break;
                case KERNEL: t = "kernel"; break;
                case RAMDISK: t = "ramdisk"; break;
            }
            parameters.put("Filter.1.Name", "image-type");
            parameters.put("Filter.1.Value", t);
            method = new EC2Method(getProvider(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("imagesSet");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList instances = blocks.item(i).getChildNodes();

                for( int j=0; j<instances.getLength(); j++ ) {
                    Node instance = instances.item(j);

                    if( instance.getNodeName().equals("item") ) {
                        ResourceStatus status = toStatus(instance);

                        if( status != null ) {
                            list.add(status);
                        }
                    }
                }
            }
            if( getProvider(). getEC2Provider().isAWS() ) {
                parameters = getProvider(). getStandardParameters(getProvider(). getContext(), EC2Method.DESCRIBE_IMAGES);
                parameters.put("ExecutableBy", accountNumber);
                parameters.put("Filter.1.Name", "image-type");
                parameters.put("Filter.1.Value", t);
                method = new EC2Method(getProvider(), parameters);
                try {
                    doc = method.invoke();
                }
                catch( EC2Exception e ) {
                    logger.error(e.getSummary());
                    throw new CloudException(e);
                }
                blocks = doc.getElementsByTagName("imagesSet");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    NodeList instances = blocks.item(i).getChildNodes();

                    for( int j=0; j<instances.getLength(); j++ ) {
                        Node instance = instances.item(j);

                        if( instance.getNodeName().equals("item") ) {
                            ResourceStatus status = toStatus(instance);

                            if( status != null ) {
                                list.add(status);
                            }
                        }
                    }
                }
            }
            return list;
        }
        finally {
            APITrace.end();
        }
    }

    private @Nonnull ImageFilterOptions fillImageFilterParameters(boolean forPublic, @Nonnull ImageFilterOptions options, @Nonnull Map<String,String> parameters) throws CloudException, InternalException {
        int filter = 1;

        if( forPublic ) {
            parameters.put("Filter." + filter + ".Name", "state");
            parameters.put("Filter." + (filter++) + ".Value.1", "available");
        }

        if( options.isMatchesAny() && options.getCriteriaCount() > 1 ) {
            if( forPublic ) {
                return options;
            }
            else {
                options.withAccountNumber(getContext().getAccountNumber());
                return options;
            }
        }

        String owner = options.getAccountNumber();

        if( owner != null ) {
            parameters.put("Owner", owner);
        }

        Architecture architecture = options.getArchitecture();

        if( architecture != null && (architecture.equals(Architecture.I32) || architecture.equals(Architecture.I64)) ) {
            parameters.put("Filter." + filter + ".Name", "architecture");
            parameters.put("Filter." + (filter++) + ".Value.1", Architecture.I32.equals(options.getArchitecture()) ? "i386" : "x86_64");
        }

        Platform platform = options.getPlatform();

        if( platform != null && platform.equals(Platform.WINDOWS) ) {
            parameters.put("Filter." + filter + ".Name", "platform");
            parameters.put("Filter." + (filter++) + ".Value.1", "windows");
        }

        ImageClass cls= options.getImageClass();
        String t = "machine";

        if( cls != null ) {
            switch( cls ) {
                case MACHINE: t = "machine"; break;
                case KERNEL: t = "kernel"; break;
                case RAMDISK: t = "ramdisk"; break;
            }
            parameters.put("Filter." + filter + ".Name", "image-type");
            parameters.put("Filter." + (filter++) + ".Value.1", t);
        }

        Map<String, String> extraParameters = new HashMap<String, String>();

        AWSCloud.addExtraParameters( extraParameters, getProvider(). getTagFilterParams( options.getTags(), filter ) );
        parameters.putAll(extraParameters);
        String regex = options.getRegex();

        options = ImageFilterOptions.getInstance();

        if( regex != null ) {
            options.matchingRegex(regex);
        }
        if( platform != null ) {
            options.onPlatform(platform);
        }
        return options;
    }

    @Override
    public @Nonnull Iterable<String> listShares(@Nonnull String forMachineImageId) throws CloudException, InternalException {
        return sharesAsList(forMachineImageId);
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        if( action.equals(MachineImageSupport.ANY) ) {
            return new String[] { EC2Method.EC2_PREFIX + "*" };
        }
        if( action.equals(MachineImageSupport.DOWNLOAD_IMAGE) ) {
            return new String[0];
        }
        else if( action.equals(MachineImageSupport.GET_IMAGE) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_IMAGES };
        }
        else if( action.equals(MachineImageSupport.IMAGE_VM) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.CREATE_IMAGE, EC2Method.EC2_PREFIX + EC2Method.REGISTER_IMAGE };
        }
        else if( action.equals(MachineImageSupport.COPY_IMAGE) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.COPY_IMAGE };
        }
        else if( action.equals(MachineImageSupport.LIST_IMAGE) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_IMAGES };
        }
        else if( action.equals(MachineImageSupport.MAKE_PUBLIC) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.MODIFY_IMAGE_ATTRIBUTE };
        }
        else if( action.equals(MachineImageSupport.REGISTER_IMAGE) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.REGISTER_IMAGE };
        }
        else if( action.equals(MachineImageSupport.REMOVE_IMAGE) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DEREGISTER_IMAGE };
        }
        else if( action.equals(MachineImageSupport.SHARE_IMAGE) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.MODIFY_IMAGE_ATTRIBUTE };
        }
        else if( action.equals(MachineImageSupport.UPLOAD_IMAGE) ) {
            return new String[0];
        }
        return new String[0];
    }

    /*
	private void populateImages(@Nonnull ProviderContext ctx, @Nullable String accountNumber, @Nonnull Jiterator<MachineImage> iterator, Map<String,String> extraParameters) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "populateImages");
        try {
            Map<String,String> parameters = getProvider(). getStandardParameters(getProvider(). getContext(), EC2Method.DESCRIBE_IMAGES);
            EC2Method method;
            NodeList blocks;
            Document doc;

            if( accountNumber == null ) {
                accountNumber = ctx.getAccountNumber();
            }
            if( getProvider(). getEC2Provider().isAWS() ) {
                parameters.put("Owner", accountNumber);
            }

            getProvider(). putExtraParameters( parameters, extraParameters );

            method = new EC2Method(getProvider(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("imagesSet");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList instances = blocks.item(i).getChildNodes();

                for( int j=0; j<instances.getLength(); j++ ) {
                    Node instance = instances.item(j);

                    if( instance.getNodeName().equals("item") ) {
                        MachineImage image = toMachineImage(instance);

                        if( image != null ) {
                            iterator.push(image);
                        }
                    }
                }
            }
            if( getProvider(). getEC2Provider().isAWS() ) {
                parameters = getProvider(). getStandardParameters(getProvider(). getContext(), EC2Method.DESCRIBE_IMAGES);
                parameters.put("ExecutableBy", accountNumber);
                getProvider(). putExtraParameters( parameters, extraParameters );
                method = new EC2Method(getProvider(), parameters);
                try {
                    doc = method.invoke();
                }
                catch( EC2Exception e ) {
                    logger.error(e.getSummary());
                    throw new CloudException(e);
                }
                blocks = doc.getElementsByTagName("imagesSet");
                for( int i=0; i<blocks.getLength(); i++ ) {
                    NodeList instances = blocks.item(i).getChildNodes();

                    for( int j=0; j<instances.getLength(); j++ ) {
                        Node instance = instances.item(j);

                        if( instance.getNodeName().equals("item") ) {
                            MachineImage image = toMachineImage(instance);

                            if( image != null ) {
                                iterator.push(image);
                            }
                        }
                    }
                }
            }
        }
        finally {
            APITrace.end();
        }
	}
    */

    @Override
    public @Nonnull MachineImage registerImageBundle(@Nonnull ImageCreateOptions options) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.registerImageBundle");
        try {
            if( !MachineImageFormat.AWS.equals(options.getBundleFormat()) ) {
                throw new CloudException("Unsupported bundle format: " + options.getBundleFormat());
            }
            if( options.getBundleLocation() == null ) {
                throw new OperationNotSupportedException("A valid bundle location in object storage was not provided");
            }
            Map<String,String> parameters = getProvider(). getStandardParameters(getProvider(). getContext(), EC2Method.REGISTER_IMAGE);
            NodeList blocks;
            EC2Method method;
            Document doc;

            parameters.put("ImageLocation", options.getBundleLocation());
            method = new EC2Method(getProvider(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("imageId");
            if( blocks.getLength() > 0 ) {
                Node imageIdNode = blocks.item(0);
                String id = imageIdNode.getFirstChild().getNodeValue().trim();
                MachineImage img = getMachineImage(id);

                if( img == null ) {
                    throw new CloudException("Expected to find newly registered machine image '" + id + "', but none was found");
                }
                return img;
            }
            throw new CloudException("No machine image was registered, but no error was thrown");
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void remove( @Nonnull String providerImageId, boolean checkState ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.remove");
        try {
            if ( checkState ) {
                long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 30L);

                while ( timeout > System.currentTimeMillis() ) {
                    try {
                        MachineImage img = getMachineImage( providerImageId );

                        if ( img == null || MachineImageState.DELETED.equals( img.getCurrentState() ) ) {
                            return;
                        }
                        if ( MachineImageState.ACTIVE.equals( img.getCurrentState() ) ) {
                            break;
                        }
                    } catch ( Throwable ignore ) {
                        // ignore
                    }
                    try {
                        Thread.sleep( 15000L );
                    }
                    catch ( InterruptedException ignore ) {
                    }
                }
            }

            Map<String, String> parameters = getProvider(). getStandardParameters( getProvider(). getContext(), EC2Method.DEREGISTER_IMAGE );
            NodeList blocks;
            EC2Method method;
            Document doc;

            parameters.put( "ImageId", providerImageId );
            method = new EC2Method( getProvider(), parameters );
            try {
                doc = method.invoke();
            } catch ( EC2Exception e ) {
                logger.error( e.getSummary() );
                throw new CloudException( e );
            }
            blocks = doc.getElementsByTagName( "return" );
            if ( blocks.getLength() > 0 ) {
                Node imageIdNode = blocks.item( 0 );

                if ( !imageIdNode.getFirstChild().getNodeValue().trim().equals( "true" ) ) {
                    throw new CloudException( "Failed to de-register image " + providerImageId );
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeAllImageShares(@Nonnull String providerImageId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.removeAllImageShares");
        try {
            List<String> shares = sharesAsList(providerImageId);

            setPrivateShare(providerImageId, false, shares.toArray(new String[shares.size()]));
            setPublicShare(providerImageId, false);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeImageShare(@Nonnull String providerImageId, @Nonnull String accountNumber) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.removeImageShare");
        try {
            setPrivateShare(providerImageId, false, accountNumber);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removePublicShare(@Nonnull String providerImageId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.removePublicShare");
        try {
            setPublicShare(providerImageId, false);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<MachineImage> listImages(@Nullable ImageFilterOptions options) throws CloudException, InternalException {
        final ImageFilterOptions opts;

        if( options == null ) {
            opts = ImageFilterOptions.getInstance();
        }
        else {
            opts = options;
        }
        getProvider(). hold();
        PopulatorThread<MachineImage> populator = new PopulatorThread<MachineImage>(new JiteratorPopulator<MachineImage>() {
            @Override
            public void populate(@Nonnull Jiterator<MachineImage> iterator) throws Exception {
                APITrace.begin(getProvider(), "Image.listImages");
                try {
                    try {
                        Set<String> ids = new TreeSet<String>();

                        for( MachineImage img : executeImageSearch(1, false, opts) ) {
                            ids.add(img.getProviderMachineImageId());
                            iterator.push(img);
                        }
                        for( MachineImage img : executeImageSearch(2, false, opts) ) {
                            if( !ids.contains(img.getProviderMachineImageId()) ) {
                                iterator.push(img);
                            }
                        }
                    }
                    finally {
                        getProvider(). release();
                    }
                }
                finally {
                    APITrace.end();
                }
            }
        });

        populator.populate();
        return populator.getResult();
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchPublicImages(final @Nonnull ImageFilterOptions options) throws CloudException, InternalException {
        getProvider(). hold();
        PopulatorThread<MachineImage> populator = new PopulatorThread<MachineImage>(new JiteratorPopulator<MachineImage>() {
            @Override
            public void populate(@Nonnull Jiterator<MachineImage> iterator) throws Exception {
                APITrace.begin(getProvider(), "searchPublicImages");
                try {
                    try {
                        for( MachineImage img : executeImageSearch(1, true, options) ) {
                            iterator.push(img);
                        }
                        for( MachineImage img : executeImageSearch(2, true, options) ) {
                            iterator.push(img);
                        }
                    }
                    finally {
                        getProvider(). release();
                    }
                }
                finally {
                    APITrace.end();
                }
            }
        });

        populator.populate();
        return populator.getResult();
    }

    private void setPrivateShare(@Nonnull String imageId, boolean allowed, @Nonnull String ... accountIds) throws CloudException, InternalException {
        if( accountIds == null || accountIds.length < 1 ) {
            return;
        }
        long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 30L);

        while( timeout > System.currentTimeMillis() ) {
            try {
                MachineImage img = getMachineImage(imageId);

                if( img == null ) {
                    throw new CloudException("The machine image " + imageId + " disappeared while waiting to set sharing");
                }
                if( MachineImageState.ACTIVE.equals(img.getCurrentState()) ) {
                    break;
                }
            }
            catch( Throwable ignore ) {
                // ignore
            }
            try { Thread.sleep(15000L); }
            catch( InterruptedException ignore ) { }
        }
        Map<String,String> parameters = getProvider(). getStandardParameters(getProvider(). getContext(), EC2Method.MODIFY_IMAGE_ATTRIBUTE);
        EC2Method method;
        NodeList blocks;
        Document doc;

        parameters.put("ImageId", imageId);
        for( int i=0; i<accountIds.length; i++ ) {
            if( allowed ) {
                parameters.put("LaunchPermission.Add." + (i+1) + ".UserId", accountIds[i]);
            }
            else {
                parameters.put("LaunchPermission.Remove." + (i+1) + ".UserId", accountIds[i]);
            }
        }
        method = new EC2Method(getProvider(), parameters);
        try {
            doc = method.invoke();
        }
        catch( EC2Exception e ) {
            String code = e.getCode();

            if( code != null && code.startsWith("InvalidImageID") ) {
                return;
            }
            logger.error(e.getSummary());
            throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("return");
        if( blocks.getLength() > 0 ) {
            if( !blocks.item(0).getFirstChild().getNodeValue().equalsIgnoreCase("true") ) {
                throw new CloudException("Share of image failed without explanation.");
            }
        }
        timeout = System.currentTimeMillis() + (CalendarWrapper.SECOND * 30);
        while( timeout > System.currentTimeMillis() ) {
            try {
                MachineImage img = getMachineImage(imageId);

                if( img == null ) {
                    return;
                }
                boolean present = true;

                for( String accountId : accountIds ) {
                    boolean found = false;
                    for( String share : listShares(imageId) ) {
                        if( share.equals(accountId) ) {
                            found = true;
                            break;
                        }
                    }
                    if( !found ) {
                        present = false;
                        break;
                    }
                }
                if( present == allowed ) {
                    return;
                }
            }
            catch( Throwable ignore ) {
                // ignore
            }
            try { Thread.sleep(2000L); }
            catch( InterruptedException ignore ) { }
        }
    }

    private void setPublicShare(@Nonnull String imageId, boolean allowed) throws CloudException, InternalException {
        long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 30L);

        while( timeout > System.currentTimeMillis() ) {
            try {
                MachineImage img = getMachineImage(imageId);

                if( img == null ) {
                    throw new CloudException("The machine image " + imageId + " disappeared while waiting to set sharing");
                }
                if( MachineImageState.ACTIVE.equals(img.getCurrentState()) ) {
                    break;
                }
            }
            catch( Throwable ignore ) {
                // ignore
            }
            try { Thread.sleep(15000L); }
            catch( InterruptedException ignore ) { }
        }
        Map<String,String> parameters = getProvider(). getStandardParameters(getProvider(). getContext(), EC2Method.MODIFY_IMAGE_ATTRIBUTE);
        EC2Method method;
        NodeList blocks;
        Document doc;

        parameters.put("ImageId", imageId);
        if( allowed ) {
            parameters.put("LaunchPermission.Add.1.Group", "all");
        }
        else {
            parameters.put("LaunchPermission.Remove.1.Group", "all");
        }
        method = new EC2Method(getProvider(), parameters);
        try {
            doc = method.invoke();
        }
        catch( EC2Exception e ) {
            String code = e.getCode();

            if( code != null && code.startsWith("InvalidImageID") ) {
                return;
            }
            logger.error(e.getSummary());
            throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("return");
        if( blocks.getLength() > 0 ) {
            if( !blocks.item(0).getFirstChild().getNodeValue().equalsIgnoreCase("true") ) {
                throw new CloudException("Share of image failed without explanation.");
            }
        }
        timeout = System.currentTimeMillis() + (CalendarWrapper.SECOND * 30);
        while( timeout > System.currentTimeMillis() ) {
            try {
                MachineImage img = getMachineImage(imageId);

                if( img == null ) {
                    return;
                }
                if( allowed == isImageSharedWithPublic(imageId) ) {
                    return;
                }
            }
            catch( Throwable ignore ) {
                // ignore
            }
            try { Thread.sleep(2000L); }
            catch( InterruptedException ignore ) { }
        }
    }

    private @Nonnull List<String> sharesAsList(@Nonnull String forMachineImageId) throws CloudException, InternalException {
        Map<String,String> parameters = getProvider(). getStandardParameters(getProvider(). getContext(), EC2Method.DESCRIBE_IMAGE_ATTRIBUTE);
        ArrayList<String> list = new ArrayList<String>();
        EC2Method method;
        NodeList blocks;
        Document doc;

        parameters.put("ImageId", forMachineImageId);
        parameters.put("Attribute", "launchPermission");
        method = new EC2Method(getProvider(), parameters);
        try {
            doc = method.invoke();
        }
        catch( EC2Exception e ) {
            String code = e.getCode();

            if( code != null && code.startsWith("InvalidImageID") ) {
                return list;
            }
            logger.error(e.getSummary());
            throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("launchPermission");
        for( int i=0; i<blocks.getLength(); i++ ) {
            NodeList items = blocks.item(i).getChildNodes();

            for( int j=0; j<items.getLength(); j++ ) {
                Node item = items.item(j);

                if( item.getNodeName().equals("item") ) {
                    NodeList attrs = item.getChildNodes();

                    for( int k=0; k<attrs.getLength(); k++ ) {
                        Node attr = attrs.item(k);

                        if( attr.getNodeName().equals("userId") ) {
                            String userId = attr.getFirstChild().getNodeValue();

                            if( userId != null ) {
                                userId = userId.trim();
                                if( userId.length() > 0 ) {
                                    list.add(userId);
                                }
                            }
                        }
                    }
                }
            }
        }
        return list;
    }

    @Override
    public boolean supportsCustomImages() {
        return true;
    }

    @Override
    public boolean supportsImageCapture(@Nonnull MachineImageType type) throws CloudException, InternalException {
        return getCapabilities().supportsImageCapture(type);
    }

    @Override
    public boolean supportsImageSharing() throws CloudException, InternalException{
        return getCapabilities().supportsImageSharing();
    }

    @Override
    public boolean supportsImageSharingWithPublic() throws CloudException, InternalException{
        return getCapabilities().supportsImageSharingWithPublic();
    }

    @Override
    public boolean supportsPublicLibrary(@Nonnull ImageClass cls) throws CloudException, InternalException {
        return getCapabilities().supportsPublicLibrary(cls);
    }

    private static class BundleTask {
        public String bundleId;
        public double progress;
        public String message;
        public String state;
    }

    private BundleTask toBundleTask(Node node) throws CloudException, InternalException {
        NodeList attributes = node.getChildNodes();
        BundleTask task = new BundleTask();

        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);
            String name = attribute.getNodeName();

            if( name.equals("bundleId") ) {
                task.bundleId = attribute.getFirstChild().getNodeValue();
            }
            else if( name.equals("state") ) {
                task.state = attribute.getFirstChild().getNodeValue();
            }
            else if( name.equals("message") ) {
                if( attribute.hasChildNodes() ) {
                    task.message = attribute.getFirstChild().getNodeValue();
                }
            }
            else if( name.equals("progress") ) {
                String tmp = attribute.getFirstChild().getNodeValue();

                if( tmp == null ) {
                    task.progress = 0.0;
                }
                else {
                    if( tmp.endsWith("%") ) {
                        if( tmp.equals("%") ) {
                            task.progress = 0.0;
                        }
                        else {
                            try {
                                task.progress = Double.parseDouble(tmp.substring(0, tmp.length()-1));
                            }
                            catch( NumberFormatException e ) {
                                task.progress = 0.0;
                            }
                        }
                    }
                    else {
                        try {
                            task.progress = Double.parseDouble(tmp);
                        }
                        catch( NumberFormatException e ) {
                            task.progress = 0.0;
                        }
                    }
                }
            }
        }
        return task;
    }

    private @Nullable ResourceStatus toStatus(@Nullable Node node) throws CloudException, InternalException {
        if( node == null ) {
            return null;
        }
        ProviderContext ctx = getProvider(). getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was set for this request");
        }
        NodeList attributes = node.getChildNodes();
        MachineImageState state = MachineImageState.PENDING;
        String imageId = null;

        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);
            String name = attribute.getNodeName();

            if( name.equals("imageId") ) {
                imageId = attribute.getFirstChild().getNodeValue().trim();
            }
            else if( name.equals("imageState") ) {
                String value = null;

                if( attribute.getChildNodes().getLength() > 0 ) {
                    value = attribute.getFirstChild().getNodeValue().trim();
                }
                if( value != null && value.equalsIgnoreCase("available") ) {
                    state = MachineImageState.ACTIVE;
                }
                else if( value != null && value.equalsIgnoreCase("failed") ) {
                    state = MachineImageState.DELETED;
                }
                else {
                    state = MachineImageState.PENDING;
                }
            }
        }
        if( imageId == null ) {
            return null;
        }
        return new ResourceStatus(imageId, state);
    }

    private @Nullable MachineImage toMachineImage(@Nullable Node node) throws CloudException, InternalException {
        if( node == null ) {
            return null;
        }
        ProviderContext ctx = getProvider(). getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was set for this request");
        }
        NodeList attributes = node.getChildNodes();
        List<MachineImageVolume> volumes = new ArrayList<MachineImageVolume>();
        String location = null;
        ImageClass imgClass = ImageClass.MACHINE;
        MachineImageState state = null;
        String amiId = null;
        String reason = null;
        String ownerId = null;
        boolean isPublic = false;
        Architecture arch = Architecture.I64;
        Platform platform = null;
        String imgName = null;
        String description = null;
        MachineImageType type = null;
        MachineImageFormat storageFormat = null;
        Node tagsNode = null;
        String virtualizationType = null;
        String hypervisor = null;

        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);
            String name = attribute.getNodeName();

            if( name.equals("imageType") ) {
                String value = attribute.getFirstChild().getNodeValue().trim();

                if( value.equalsIgnoreCase("machine") ) {
                    imgClass = ImageClass.MACHINE;
                }
                else if( value.equalsIgnoreCase("kernel") ) {
                    imgClass = ImageClass.KERNEL;
                }
                else if( value.equalsIgnoreCase("ramdisk") ) {
                    imgClass = ImageClass.RAMDISK;
                }
            }
            else if( name.equals("imageId") ) {
                amiId = attribute.getFirstChild().getNodeValue().trim();

                if( amiId.startsWith("ami") ) {
                    imgClass = ImageClass.MACHINE;
                }
                else if( amiId.startsWith("aki") ) {
                    imgClass = ImageClass.KERNEL;
                }
                else if( amiId.startsWith("ari") ) {
                    imgClass = ImageClass.KERNEL;
                }
            }
            else if( name.equals("imageLocation") ) {
                location = attribute.getFirstChild().getNodeValue().trim();
            }
            else if( name.equals("imageState") ) {
                String value = null;

                if( attribute.getChildNodes().getLength() > 0 ) {
                    value = attribute.getFirstChild().getNodeValue().trim();
                }
                if( value != null && value.equalsIgnoreCase("available") ) {
                    state = MachineImageState.ACTIVE;
                }
                else if( value != null && value.equalsIgnoreCase("failed") ) {
                    state = MachineImageState.DELETED;
                }
                else {
                    state = MachineImageState.PENDING;
                }
            }
            else if( name.equalsIgnoreCase("statereason") && attribute.hasChildNodes() ) {
                NodeList parts = attribute.getChildNodes();

                for( int j=0; j<parts.getLength(); j++ ) {
                    Node part = parts.item(j);

                    if( part.getNodeName().equalsIgnoreCase("message") && part.hasChildNodes() ) {
                        reason = part.getFirstChild().getNodeValue().trim();
                    }
                }
            }
            else if( name.equals("imageOwnerId") ) {
                String value = null;

                if( attribute.getChildNodes().getLength() > 0 ) {
                    value = attribute.getFirstChild().getNodeValue();
                }
                if( value != null ) {
                    ownerId = value.trim();
                }
            }
            else if( name.equals("isPublic") ) {
                String value = null;

                if( attribute.getChildNodes().getLength() > 0 ) {
                    value = attribute.getFirstChild().getNodeValue();
                }
                isPublic = value != null && value.trim().equalsIgnoreCase("true");
            }
            else if( name.equals("architecture") ) {
                String value = attribute.getFirstChild().getNodeValue().trim();
                if( value.equals("i386") ) {
                    arch = Architecture.I32;
                }
            }
            else if( name.equals("platform") ) {
                if( attribute.hasChildNodes() ) {
                    platform = Platform.guess(attribute.getFirstChild().getNodeValue());
                }
            }
            else if( name.equals("name") ) {
                if( attribute.hasChildNodes() ) {
                    imgName = attribute.getFirstChild().getNodeValue();
                }
            }
            else if( name.equals("description") ) {
                if( attribute.hasChildNodes() ) {
                    description = attribute.getFirstChild().getNodeValue();
                }
            }
            else if( name.equals("rootDeviceType") ) {
                if( attribute.hasChildNodes() ) {
                    String value = attribute.getFirstChild().getNodeValue();

                    if( value.equalsIgnoreCase("ebs") ) {
                        type = MachineImageType.VOLUME; // ebs
                    }
                    else {
                        type = MachineImageType.STORAGE; // instance-store
                        storageFormat = MachineImageFormat.AWS;
                    }
                }
            }
            else if ( "virtualizationType".equals(name) ) {
                virtualizationType = attribute.getFirstChild().getNodeValue();
            }
            else if ( "hypervisor".equals(name) ) {
                hypervisor = attribute.getFirstChild().getNodeValue();
            }
            else if ( name.equals("tagSet")) {
                tagsNode = attribute;
            }
            else if( name.equalsIgnoreCase("blockDeviceMapping") ) {

                if( attribute.hasChildNodes() ) {
                    NodeList devices = attribute.getChildNodes();

                    for( int z = 0; z < devices.getLength(); z++ ) {
                        NodeList param = devices.item(z).getChildNodes();
                        String deviceName = null;
                        String snapshotId = null;
                        Integer volumeSize = null;
                        String volumeType = null;
                        Integer iops = null;

                        if( devices.item(z).getNodeName().equalsIgnoreCase("item") ) {
                            for( int j = 0; j < param.getLength(); j++ ) {
                                String nodeName = param.item(j).getNodeName();

                                if( nodeName.equalsIgnoreCase("deviceName") ) {
                                    deviceName = param.item(j).getFirstChild().getNodeValue().trim();
                                }
                                else if( nodeName.equalsIgnoreCase("ebs") ) {
                                    NodeList ebs = param.item(j).getChildNodes();

                                    for( int k = 0; k < ebs.getLength(); k++ ) {
                                        String ebsName = ebs.item(k).getNodeName();

                                        if( ebsName.equalsIgnoreCase("snapshotId") ) {
                                            snapshotId = ebs.item(k).getFirstChild().getNodeValue().trim();
                                        }
                                        else if( ebsName.equalsIgnoreCase("volumeSize") ) {
                                            volumeSize = Integer.valueOf(ebs.item(k).getFirstChild().getNodeValue().trim());
                                        }
                                        else if( ebsName.equalsIgnoreCase("volumeType") ) {
                                            volumeType = ebs.item(k).getFirstChild().getNodeValue().trim();
                                        }
                                        else if( ebsName.equalsIgnoreCase("iops") ) {
                                            iops = Integer.valueOf(ebs.item(k).getFirstChild().getNodeValue().trim());
                                        }
                                    }
                                }
                            }

                            if( deviceName != null || snapshotId != null || volumeSize != null || volumeType != null || iops != null ) {
                                volumes.add(MachineImageVolume.getInstance(deviceName, snapshotId, volumeSize, volumeType, iops));
                            }
                        }
                    }
                }
            }
		}
		if( platform == null ) {
		    if( location != null ) {
		        platform = Platform.guess(location);
		    }
		    else {
                platform = Platform.UNKNOWN;
		    }
		}
        if( Platform.UNKNOWN.equals(platform) && description != null ) {
            platform = Platform.guess(description);
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
            if( imgName == null || imgName.isEmpty() ) {
                imgName = location;
            }
            if( description == null || description.isEmpty() ) {
                description = createDescription(location, arch, platform);
            }
        }
        else {
            if( imgName == null || imgName.isEmpty() ) {
                imgName = amiId;
            }
            if( description == null || description.isEmpty() ) {
                description = createDescription(imgName, arch, platform);
            }
        }
        if( !getProvider(). getEC2Provider().isAWS() ) {
            ownerId = ctx.getAccountNumber();
        }
        MachineImage image = MachineImage.getInstance(ownerId, regionId, amiId, imgClass, state, imgName, description, arch, platform);
        image.withVolumes(volumes);
        if( hypervisor != null ) {
            image.getProviderMetadata().put("hypervisor", hypervisor);
        }
        if( virtualizationType != null ) {
            image.getProviderMetadata().put("virtualizationType", virtualizationType);
        }
        if( isPublic ) {
            image.sharedWithPublic();
        }
        image.setTag("public", String.valueOf(isPublic)); // for compatibility's sake
        image.setTag("stateReason", reason);
        if( tagsNode != null ) {
            getProvider().setTags(tagsNode, image);
        }
        if( type != null ) {
            image.withType(type);
        }
        if( storageFormat != null ) {
            image.withStorageFormat(storageFormat);
        }
        return image;
    }

    public static String createDescription(String title, Architecture arch, Platform platform) {
        StringBuilder sb = new StringBuilder();
        sb.append(title);
        if( arch != null || platform != null ) {
            sb.append(" (");
            if( arch != null ) {
                sb.append(arch.toString()).append(" ");
            }
            if( platform != null ) {
                sb.append(platform.toString());
            }
            sb.append(")");
        }
        return sb.toString();
    }

    @Override
    public void updateTags(@Nonnull String imageId, @Nonnull Tag... tags) throws CloudException, InternalException {
        updateTags(new String[]{imageId}, tags);
    }

    @Override
    public void updateTags(@Nonnull String[] imageIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.updateTags");
        try {
            getProvider(). createTags(EC2Method.SERVICE_ID, imageIds, tags);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeTags(@Nonnull String imageId, @Nonnull Tag... tags) throws CloudException, InternalException {
        removeTags(new String[]{imageId}, tags);
    }

    @Override
    public void removeTags(@Nonnull String[] imageIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.removeTags");
        try {
            getProvider(). removeTags(EC2Method.SERVICE_ID, imageIds, tags);
        }
        finally {
            APITrace.end();
        }
    }

    private void waitForBundle(@Nonnull String bundleId, @Nonnull String manifest, @Nonnull Platform platform, @Nonnull String name, @Nonnull String description, AsynchronousTask<MachineImage> task) {
        APITrace.begin(getProvider(), "Image.waitForBundle");
        try {
            try {
                long failurePoint = -1L;

                while( !task.isComplete() ) {
                    Map<String,String> parameters = getProvider(). getStandardParameters(getProvider(). getContext(), EC2Method.DESCRIBE_BUNDLE_TASKS);
                    NodeList blocks;
                    EC2Method method;
                    Document doc;

                    parameters.put("BundleId", bundleId);
                    method = new EC2Method(getProvider(), parameters);
                    try {
                        doc = method.invoke();
                    }
                    catch( EC2Exception e ) {
                        throw new CloudException(e);
                    }
                    blocks = doc.getElementsByTagName("bundleInstanceTasksSet");
                    for( int i=0; i<blocks.getLength(); i++ ) {
                        NodeList instances = blocks.item(i).getChildNodes();

                        for( int j=0; j<instances.getLength(); j++ ) {
                            Node node = instances.item(j);

                            if( node.getNodeName().equals("item") ) {
                                BundleTask bt = toBundleTask(node);

                                if( bt.bundleId.equals(bundleId) ) {
                                    // pending | waiting-for-shutdown | storing | canceling | complete | failed
                                    if( bt.state.equals("complete") ) {
                                        String imageId;

                                        task.setPercentComplete(99.0);
                                        imageId = registerImageBundle(ImageCreateOptions.getInstance(MachineImageFormat.AWS, manifest, platform, name, description)).getProviderMachineImageId();
                                        task.setPercentComplete(100.00);
                                        task.completeWithResult(getMachineImage(imageId));
                                    }
                                    else if( bt.state.equals("failed") ) {
                                        String message = bt.message;

                                        if( message == null ) {
                                            if( failurePoint == -1L ) {
                                                failurePoint = System.currentTimeMillis();
                                            }
                                            if( (System.currentTimeMillis() - failurePoint) > (CalendarWrapper.MINUTE * 2) ) {
                                                message = "Bundle failed without further information.";
                                            }
                                        }
                                        if( message != null ) {
                                            task.complete(new CloudException(message));
                                        }
                                    }
                                    else if( bt.state.equals("pending") || bt.state.equals("waiting-for-shutdown") ) {
                                        task.setPercentComplete(0.0);
                                    }
                                    else if( bt.state.equals("bundling") ) {
                                        double p = bt.progress/2;

                                        if( p > 50.00 ) {
                                            p = 50.00;
                                        }
                                        task.setPercentComplete(p);
                                    }
                                    else if( bt.state.equals("storing") ) {
                                        double p = 50.0 + bt.progress/2;

                                        if( p > 100.0 ) {
                                            p = 100.0;
                                        }
                                        task.setPercentComplete(p);
                                    }
                                    else {
                                        task.setPercentComplete(0.0);
                                    }
                                }
                            }
                        }
                    }
                    if( !task.isComplete() ) {
                        try { Thread.sleep(20000L); }
                        catch( InterruptedException ignore ) { }
                    }
                }
            }
            catch( Throwable t ) {
                logger.error(t);
                t.printStackTrace();
                task.complete(t);
            }
        }
        finally {
            APITrace.end();
        }
	}
}
