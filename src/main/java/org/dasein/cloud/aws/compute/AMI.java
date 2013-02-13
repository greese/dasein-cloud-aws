/**
 * Copyright (C) 2009-2012 enStratus Networks Inc
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

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.log4j.Logger;
import org.dasein.cloud.AsynchronousTask;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.Tag;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.storage.S3Method;
import org.dasein.cloud.compute.AbstractImageSupport;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.ImageCreateOptions;
import org.dasein.cloud.compute.ImageFilterOptions;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.MachineImageFormat;
import org.dasein.cloud.compute.MachineImageState;
import org.dasein.cloud.compute.MachineImageSupport;
import org.dasein.cloud.compute.MachineImageType;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.util.APITrace;
import org.dasein.util.CalendarWrapper;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @version 2013.01.1 Fixed a data consistency issue with AWS (issue #21)
 */
public class AMI extends AbstractImageSupport {
	static private final Logger logger = Logger.getLogger(AMI.class);
	
	private AWSCloud provider = null;
	
	AMI(AWSCloud provider) {
		super(provider);
        this.provider = provider;
	}

    @Override
    public void addImageShare(@Nonnull String providerImageId, @Nonnull String accountNumber) throws CloudException, InternalException {
        APITrace.begin(provider, "addImageShare");
        try {
            setPrivateShare(providerImageId, true, accountNumber);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void addPublicShare(@Nonnull String providerImageId) throws CloudException, InternalException {
        APITrace.begin(provider, "addPublicShare");
        try {
            setPublicShare(providerImageId, true);
        }
        finally {
            APITrace.end();
        }
    }

    private @Nonnull MachineImage captureImage(@Nonnull ProviderContext ctx, @Nonnull ImageCreateOptions options, @Nullable AsynchronousTask<MachineImage> task) throws CloudException, InternalException {
        APITrace.begin(provider, "captureImage");
        try {
            if( task != null ) {
                task.setStartTime(System.currentTimeMillis());
            }
            VirtualMachine vm = null;

            long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 30L);

            while( timeout > System.currentTimeMillis() ) {
                try {
                    //noinspection ConstantConditions
                    vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(options.getVirtualMachineId());
                    if( vm == null || VmState.TERMINATED.equals(vm.getCurrentState()) ) {
                        break;
                    }
                    if( !vm.isPersistent() ) {
                        throw new OperationNotSupportedException("You cannot capture instance-backed virtual machines");
                    }
                    if( VmState.RUNNING.equals(vm.getCurrentState()) || VmState.STOPPED.equals(vm.getCurrentState()) ) {
                        break;
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
                if( vm.getPlatform().isWindows() ) {
                    String bucket = provider.getStorageServices().getBlobStoreSupport().createBucket("dsnwin" + (System.currentTimeMillis() % 10000), true).getBucketName();

                    if( bucket == null ) {
                        throw new CloudException("There is no bucket");
                    }
                    return captureWindows(ctx, options, bucket, task);
                }
                else {
                    Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.CREATE_IMAGE);
                    NodeList blocks;
                    EC2Method method;
                    Document doc;

                    parameters.put("InstanceId", options.getVirtualMachineId());
                    parameters.put("Name", options.getName());
                    parameters.put("Description", options.getDescription());
                    method = new EC2Method(provider, provider.getEc2Url(), parameters);
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
                        return img;
                    }
                    throw new CloudException("No error occurred during imaging, but no machine image was specified");
                }
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

    @Override
    protected MachineImage capture(@Nonnull ImageCreateOptions options, @Nullable AsynchronousTask<MachineImage> task) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        return captureImage(ctx, options, task);
    }

    private MachineImage captureWindows(@Nonnull ProviderContext ctx, @Nonnull ImageCreateOptions options, @Nonnull String bucket, @Nullable AsynchronousTask<MachineImage> task) throws CloudException, InternalException {
        APITrace.begin(provider, "captureWindows");
        try {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.BUNDLE_INSTANCE);
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
            parameters.put("Storage.S3.UploadPolicySignature", provider.signUploadPolicy(base64Policy));
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
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

    private @Nonnull Iterable<MachineImage> executeImageSearch(boolean forPublic, @Nonnull ImageFilterOptions options) throws CloudException, InternalException {
        APITrace.begin(provider, "executeImageSearch");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_IMAGES);

            ArrayList<MachineImage> list = new ArrayList<MachineImage>();
            NodeList blocks;
            EC2Method method;
            Document doc;

            parameters.put("ExecutableBy.1", ctx.getAccountNumber());
            if( forPublic ) {
                parameters.put("ExecutableBy.2", "all");
            }
            options = fillImageFilterParameters(false, options, parameters);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
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
                            if( options.matches(image, getContext().getAccountNumber()) ) {
                                list.add(image);
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

    @Override
    public @Nullable MachineImage getImage(@Nonnull String providerImageId) throws CloudException, InternalException {
        APITrace.begin(provider, "getImage");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            if( provider.getEC2Provider().isAWS() ) {
                Map<String,String> parameters = provider.getStandardParameters(ctx, EC2Method.DESCRIBE_IMAGES);
                NodeList blocks;
                EC2Method method;
                Document doc;

                parameters.put("ImageId", providerImageId);
                method = new EC2Method(provider, provider.getEc2Url(), parameters);
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
                for( MachineImage image : listImages(ImageClass.MACHINE) ) {
                    if( image.getProviderMachineImageId().equals(providerImageId) ) {
                        return image;
                    }
                }
                for( MachineImage image : listImages(ImageClass.KERNEL) ) {
                    if( image.getProviderMachineImageId().equals(providerImageId) ) {
                        return image;
                    }
                }
                for( MachineImage image : listImages(ImageClass.RAMDISK) ) {
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
	public @Nonnull String getProviderTermForImage(@Nonnull Locale locale) {
        return getProviderTermForImage(locale, ImageClass.MACHINE);
    }

    @Override
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale, @Nonnull ImageClass cls) {
        switch( cls ) {
            case MACHINE: return "AMI";
            case KERNEL: return "AKI";
            case RAMDISK: return "ARI";
        }
        return "image";
    }

    @Override
    public @Nonnull String getProviderTermForCustomImage(@Nonnull Locale locale, @Nonnull ImageClass cls) {
        return getProviderTermForImage(locale, cls);
    }

    @Override
    public boolean hasPublicLibrary() {
        return true;
    }

    @Override
    public @Nonnull Requirement identifyLocalBundlingRequirement() throws CloudException, InternalException {
        return Requirement.REQUIRED;
    }

    @Override
    public boolean isImageSharedWithPublic(@Nonnull String machineImageId) throws CloudException, InternalException {
        APITrace.begin(provider, "isImageSharedWithPublic");
        try {
            MachineImage image = getMachineImage(machineImageId);

            if( image == null ) {
                return false;
            }
            String p = (String)image.getTag("public");

            return (p != null && p.equalsIgnoreCase("true"));
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(provider, "isSubscribed");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_IMAGES);
            EC2Method method;

            if( provider.getEC2Provider().isAWS() ) {
                parameters.put("Owner", ctx.getAccountNumber());
            }
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
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

    public @Nonnull Iterable<ResourceStatus> listImageStatus(@Nonnull ImageClass cls) throws CloudException, InternalException {
        APITrace.begin(provider, "listImageStatus");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }

            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_IMAGES);
            ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();
            EC2Method method;
            NodeList blocks;
            Document doc;

            String accountNumber = ctx.getAccountNumber();

            if( provider.getEC2Provider().isAWS() ) {
                parameters.put("Owner", accountNumber);
            }
            String t = "machine";

            switch( cls ) {
                case MACHINE: t = "machine"; break;
                case KERNEL: t = "kernel"; break;
                case RAMDISK: t = "ramdisk"; break;
            }
            parameters.put("Filter.1.Name", "image-type");
            parameters.put("Filter.1.Value", t);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
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
            if( provider.getEC2Provider().isAWS() ) {
                parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_IMAGES);
                parameters.put("ExecutableBy", accountNumber);
                parameters.put("Filter.1.Name", "image-type");
                parameters.put("Filter.1.Value", t);
                method = new EC2Method(provider, provider.getEc2Url(), parameters);
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

        parameters.put("Filter." + filter + ".Name", "state");
        parameters.put("Filter." + filter + ".Value.1", "available");

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
            parameters.put("Filter." + filter + ".Name", "owner");
            parameters.put("Filter." + filter + ".Value.1", owner);
        }
        else if( !forPublic ) {
            parameters.put("Filter." + filter + ".Name", "owner");
            parameters.put("Filter." + filter + ".Value.1", "self");
        }
        if( options.getArchitecture() != null ) {
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

        provider.putExtraParameters( extraParameters, provider.getTagFilterParams( options.getTags(), filter ) );
        parameters.putAll(extraParameters);
        String regex = options.getRegex();

        if( regex != null ) {
            return ImageFilterOptions.getInstance(regex);
        }
        else {
            return ImageFilterOptions.getInstance();
        }
    }

    @Override
    public @Nonnull Iterable<String> listShares(@Nonnull String forMachineImageId) throws CloudException, InternalException {
        return sharesAsList(forMachineImageId);
    }

    static private Collection<ImageClass> supportedClasses;

    @Override
    public @Nonnull Iterable<ImageClass> listSupportedImageClasses() throws CloudException, InternalException {
        if( supportedClasses == null ) {
            ArrayList<ImageClass> list = new ArrayList<ImageClass>();

            list.add(ImageClass.MACHINE);
            list.add(ImageClass.KERNEL);
            list.add(ImageClass.RAMDISK);
            supportedClasses = Collections.unmodifiableCollection(list);
        }
        return supportedClasses;
    }

    static private Collection<MachineImageType> supportedTypes;

    @Override
    public @Nonnull Iterable<MachineImageType> listSupportedImageTypes() throws CloudException, InternalException {
        if( supportedTypes == null ) {
            ArrayList<MachineImageType> types = new ArrayList<MachineImageType>();

            types.add(MachineImageType.STORAGE);
            types.add(MachineImageType.VOLUME);
            supportedTypes = Collections.unmodifiableCollection(types);
        }
        return supportedTypes;
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormats() throws CloudException, InternalException {
        return Collections.singletonList(MachineImageFormat.AWS);
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormatsForBundling() throws CloudException, InternalException {
        return Collections.singletonList(MachineImageFormat.AWS);
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
        APITrace.begin(provider, "populateImages");
        try {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_IMAGES);
            EC2Method method;
            NodeList blocks;
            Document doc;

            if( accountNumber == null ) {
                accountNumber = ctx.getAccountNumber();
            }
            if( provider.getEC2Provider().isAWS() ) {
                parameters.put("Owner", accountNumber);
            }

            provider.putExtraParameters( parameters, extraParameters );

            method = new EC2Method(provider, provider.getEc2Url(), parameters);
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
            if( provider.getEC2Provider().isAWS() ) {
                parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_IMAGES);
                parameters.put("ExecutableBy", accountNumber);
                provider.putExtraParameters( parameters, extraParameters );
                method = new EC2Method(provider, provider.getEc2Url(), parameters);
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
        APITrace.begin(provider, "registerImageBundle");
        try {
            if( !MachineImageFormat.AWS.equals(options.getBundleFormat()) ) {
                throw new CloudException("Unsupported bundle format: " + options.getBundleFormat());
            }
            if( options.getBundleLocation() == null ) {
                throw new OperationNotSupportedException("A valid bundle location in object storage was not provided");
            }
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.REGISTER_IMAGE);
            NodeList blocks;
            EC2Method method;
            Document doc;

            parameters.put("ImageLocation", options.getBundleLocation());
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
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
  public void remove( @Nonnull String providerImageId ) throws CloudException, InternalException {
    APITrace.begin( provider, "remove" );
    try {
      Map<String, String> parameters = provider.getStandardParameters( provider.getContext(), EC2Method.DEREGISTER_IMAGE );
      NodeList blocks;
      EC2Method method;
      Document doc;

      parameters.put( "ImageId", providerImageId );
      method = new EC2Method( provider, provider.getEc2Url(), parameters );
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
    } finally {
      APITrace.end();
    }
  }

  @Override
  public void remove( @Nonnull String providerImageId, boolean checkState ) throws CloudException, InternalException {
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
        } catch ( InterruptedException ignore ) {
        }
      }
    }

    remove( providerImageId );
  }

    @Override
    public void removeAllImageShares(@Nonnull String providerImageId) throws CloudException, InternalException {
        APITrace.begin(provider, "removeAllImageShares");
        try {
            List<String> shares = sharesAsList(providerImageId);

            setPrivateShare(providerImageId, false, shares.toArray(new String[shares.size()]));
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeImageShare(@Nonnull String providerImageId, @Nonnull String accountNumber) throws CloudException, InternalException {
        APITrace.begin(provider, "removeImageShare");
        try {
            setPrivateShare(providerImageId, false, accountNumber);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removePublicShare(@Nonnull String providerImageId) throws CloudException, InternalException {
        APITrace.begin(provider, "removePublicShare");
        try {
            setPublicShare(providerImageId, false);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<MachineImage> listImages(@Nullable ImageFilterOptions options) throws CloudException, InternalException {
        APITrace.begin(provider, "searchImages");
        try {
            if( options == null ) {
                options = ImageFilterOptions.getInstance();
            }
            return executeImageSearch(false, options);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    @Deprecated
    public @Nonnull Iterable<MachineImage> searchMachineImages(@Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture) throws InternalException, CloudException {
        return searchPublicImages(keyword, platform, architecture, ImageClass.MACHINE);
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchPublicImages(@Nonnull ImageFilterOptions options) throws CloudException, InternalException {
        APITrace.begin(provider, "searchPublicImages");
        try {
            return executeImageSearch(true, options);
        }
        finally {
            APITrace.end();
        }
    }

    private void setPrivateShare(@Nonnull String imageId, boolean allowed, @Nonnull String ... accountIds) throws CloudException, InternalException {
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
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.MODIFY_IMAGE_ATTRIBUTE);
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
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
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
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.MODIFY_IMAGE_ATTRIBUTE);
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
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
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
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_IMAGE_ATTRIBUTE);
        ArrayList<String> list = new ArrayList<String>();
        EC2Method method;
        NodeList blocks;
        Document doc;

        parameters.put("ImageId", forMachineImageId);
        parameters.put("Attribute", "launchPermission");
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
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
        return MachineImageType.VOLUME.equals(type);
    }

    @Override
    public boolean supportsImageSharing() {
        return true;
    }

    @Override
    public boolean supportsImageSharingWithPublic() {
        return true;
    }

    @Override
    public boolean supportsPublicLibrary(@Nonnull ImageClass cls) throws CloudException, InternalException {
        return true;
    }

    private class BundleTask {
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
        ProviderContext ctx = provider.getContext();

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
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was set for this request");
        }
		NodeList attributes = node.getChildNodes();
		MachineImage image = new MachineImage();
		String location = null;
		
		image.setSoftware(""); // TODO: guess software
		image.setProviderRegionId(regionId);
        image.setImageClass(ImageClass.MACHINE);
		for( int i=0; i<attributes.getLength(); i++ ) {
			Node attribute = attributes.item(i);
			String name = attribute.getNodeName();
			
			if( name.equals("imageType") ) {
				String value = attribute.getFirstChild().getNodeValue().trim();

                if( value.equalsIgnoreCase("machine") ) {
                    image.setImageClass(ImageClass.MACHINE);
                }
                else if( value.equalsIgnoreCase("kernel") ) {
                    image.setImageClass(ImageClass.KERNEL);
                }
                else if( value.equalsIgnoreCase("ramdisk") ) {
                    image.setImageClass(ImageClass.RAMDISK);
                }
			}
			else if( name.equals("imageId") ) {
				String value = attribute.getFirstChild().getNodeValue().trim();				
				
				image.setProviderMachineImageId(value);
                if( value.startsWith("ami") ) {
                    image.setImageClass(ImageClass.MACHINE);
                }
                else if( value.startsWith("aki") ) {
                    image.setImageClass(ImageClass.KERNEL);
                }
                else if( value.startsWith("ari") ) {
                    image.setImageClass(ImageClass.RAMDISK);
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
				    image.setCurrentState(MachineImageState.ACTIVE);
				}
                else if( value != null && value.equalsIgnoreCase("failed") ) {
                    image.setCurrentState(MachineImageState.DELETED);
                }
				else {
				    image.setCurrentState(MachineImageState.PENDING);
				}
			}
            else if( name.equalsIgnoreCase("statereason") && attribute.hasChildNodes() ) {
                NodeList parts = attribute.getChildNodes();

                for( int j=0; j<parts.getLength(); j++ ) {
                    Node part = parts.item(j);

                    if( part.getNodeName().equalsIgnoreCase("message") && part.hasChildNodes() ) {
                        String value = part.getFirstChild().getNodeValue().trim();

                        image.setTag("stateReason", value);
                    }
                }
            }
			else if( name.equals("imageOwnerId") ) {
				String value = null;
				
				if( attribute.getChildNodes().getLength() > 0 ) {
					value = attribute.getFirstChild().getNodeValue();
				}
				image.setProviderOwnerId(value == null ? value : value.trim());
			}
			else if( name.equals("isPublic") ) {
				String value = null;
				
				if( attribute.getChildNodes().getLength() > 0 ) {
					value = attribute.getFirstChild().getNodeValue();
				}
				image.addTag("public", String.valueOf(value != null && value.trim().equalsIgnoreCase("true")));
			}
			else if( name.equals("architecture") ) {
				String value = attribute.getFirstChild().getNodeValue().trim();
				
				if( value.equals("i386") ) {
					image.setArchitecture(Architecture.I32);
				}
				else {
					image.setArchitecture(Architecture.I64);
				}
			}
            else if( name.equals("platform") ) {
                if( attribute.hasChildNodes() ) {
                    String value = attribute.getFirstChild().getNodeValue();
                
                    image.setPlatform(Platform.guess(value));
                }
            }
            else if( name.equals("name") ) {
                if( attribute.hasChildNodes() ) {
                    String value = attribute.getFirstChild().getNodeValue();
                
                    image.setName(value);
                }
            }
            else if( name.equals("description") ) {
                if( attribute.hasChildNodes() ) {
                    String value = attribute.getFirstChild().getNodeValue();
                
                    image.setDescription(value);
                }
            }
            else if( name.equals("rootDeviceType") ) {
                if( attribute.hasChildNodes() ) {
                    String type = attribute.getFirstChild().getNodeValue();
                    
                    if( type.equalsIgnoreCase("ebs") ) {
                        image.setType(MachineImageType.VOLUME);
                    }
                    else {
                        image.setType(MachineImageType.STORAGE);
                    }
                }
            }
            else if ( name.equals("tagSet")) {
                provider.setTags( attribute, image );
            }
		}
		if( image.getPlatform() == null ) {
		    if( location != null ) {
		        image.setPlatform(Platform.guess(location));
		    }
		    else {
		        image.setPlatform(Platform.UNKNOWN);
		    }
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
            if( image.getName() == null || image.getName().equals("") ) {
                image.setName(location);
            }
            if( image.getDescription() == null || image.getDescription().equals("") ) {
                image.setDescription(location +  " (" + image.getArchitecture().toString() + " " + image.getPlatform().toString() + ")");
            }
		}
		else {
            if( image.getName() == null || image.getName().equals("") ) {
                image.setName(image.getProviderMachineImageId());
            }
            if( image.getDescription() == null || image.getDescription().equals("") ) {
                image.setDescription(image.getName() +  " (" + image.getArchitecture().toString() + " " + image.getPlatform().toString() + ")");
            }
		}
		if( !provider.getEC2Provider().isAWS() ) {
		    image.setProviderOwnerId(ctx.getAccountNumber());
		}
		return image;
	}

    @Override
    public void updateTags(@Nonnull String imageId, @Nonnull Tag... tags) throws CloudException, InternalException {
        updateTags(new String[]{imageId}, tags);
    }

    @Override
    public void updateTags(@Nonnull String[] imageIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        provider.createTags(imageIds, tags);
    }

    @Override
    public void removeTags(@Nonnull String imageId, @Nonnull Tag... tags) throws CloudException, InternalException {
        removeTags(new String[]{imageId}, tags);
    }

    @Override
    public void removeTags(@Nonnull String[] imageIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        provider.removeTags(imageIds, tags);
    }

    private void waitForBundle(@Nonnull String bundleId, @Nonnull String manifest, @Nonnull Platform platform, @Nonnull String name, @Nonnull String description, AsynchronousTask<MachineImage> task) {
		try {
			long failurePoint = -1L;
			
			while( !task.isComplete() ) {
				Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_BUNDLE_TASKS);
		        NodeList blocks;
				EC2Method method;
				Document doc;
		        
				parameters.put("BundleId", bundleId);
		        method = new EC2Method(provider, provider.getEc2Url(), parameters);
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
}
