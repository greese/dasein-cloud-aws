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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dasein.cloud.AsynchronousTask;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.storage.S3Method;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.MachineImageFormat;
import org.dasein.cloud.compute.MachineImageState;
import org.dasein.cloud.compute.MachineImageSupport;
import org.dasein.cloud.compute.MachineImageType;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.storage.Blob;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorPopulator;
import org.dasein.util.PopulatorThread;
import org.dasein.util.uom.storage.Storage;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;

public class AMI implements MachineImageSupport {
	static private final Logger logger = Logger.getLogger(AMI.class);
	
	private AWSCloud provider = null;
	
	AMI(AWSCloud provider) {
		this.provider = provider;
	}

    @Override
    public void downloadImage(String machineImageId, OutputStream toOutput) throws CloudException, InternalException {
        Blob manifest = getManifest(machineImageId);
        
        if( manifest == null ) {
            throw new CloudException("No such image manifest: " + machineImageId);
        }
        String name = manifest.getObjectName();
        if( name == null ) {
            throw new CloudException("Manifest name is empty");
        }
        int idx = name.indexOf(".manifest.xml");
        
        if( idx < 1 ) {
            throw new CloudException("Nonsense manifest: " + name);
        }
        name = name.substring(0, idx);
        idx = 0;
        while( true ) {
            String postfix = ".part." + (idx < 10 ? ("0" + idx) : String.valueOf(idx));
            Storage<org.dasein.util.uom.storage.Byte>  size = provider.getStorageServices().getBlobStoreSupport().getObjectSize(manifest.getBucketName(), name + postfix);

            if( size == null ) {
                return;
            }
            // TODO: get source file
            // TODO: read to stream
           // provider.getStorageServices().getBlobStoreSupport().download(sourceFile, toFile)
        }
    }

	private String getImageLocation(String imageId) throws CloudException, InternalException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_IMAGES);
        NodeList blocks;
		EC2Method method;
		Document doc;
        
		parameters.put("ImageId", imageId);
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
        };
        blocks = doc.getElementsByTagName("imagesSet");
        for( int i=0; i<blocks.getLength(); i++ ) {
        	NodeList instances = blocks.item(i).getChildNodes();
        	
            for( int j=0; j<instances.getLength(); j++ ) {
            	Node instance = instances.item(j);
            	
            	if( instance.getNodeName().equals("item") ) {
            		NodeList attributes = instance.getChildNodes();
            		
            		for( int k=0; k<attributes.getLength(); k++ ) {
            			Node attribute = attributes.item(i);
            			String name = attribute.getNodeName();
            			
            			if( name.equals("imageLocation") ) {
            				return attribute.getFirstChild().getNodeValue().trim();
            			}
            		}
            	}
            }
        }
        return null;		
	}
	
    
    @Override
    public MachineImage getMachineImage(String imageId) throws InternalException, CloudException {
        if( provider.isAmazon() ) {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_IMAGES);
            NodeList blocks;
            EC2Method method;
            Document doc;
            
            parameters.put("ImageId", imageId);
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
                        
                        if( image != null && image.getProviderMachineImageId().equals(imageId) ) {
                            return image;
                        }
                    }
                }
            }
            return null;
        }
        else {
            for( MachineImage image : listMachineImages() ) {
                if( image.getProviderMachineImageId().equals(imageId) ) {
                    return image;
                }
            }
            return null;
        }
    }
    
	private Blob getManifest(String imageId) throws CloudException, InternalException {
        String location = getImageLocation(imageId);
        
        if( location != null ) {
	        String[] parts = location.split("/");

            return provider.getStorageServices().getBlobStoreSupport().getObject(parts[0], parts[1]);
        }
        return null;
	}		
	
	@Override
	public String getProviderTermForImage(Locale locale) {
		return "AMI";
	}
	
    @Override
    public boolean hasPublicLibrary() {
        return true;
    }

    @Override
    public AsynchronousTask<String> imageVirtualMachine(String vmId, String name, String description) throws CloudException, InternalException {
        final AsynchronousTask<String> task = new AsynchronousTask<String>();
        final String fvmId = vmId;
        final String fname = name;
        final String fdesc = description;

        Thread t = new Thread() { 
            public void run() {
                try {
                    task.completeWithResult(imageVirtualMachine(fvmId, fname, fdesc, task));
                }
                catch( Throwable t ) {
                    task.complete(t);
                }
            }
        };
        
        t.setName("Imaging " + vmId + " as " + name);
        t.setDaemon(true);
        t.start();
        return task;
    }
    
    private String imageVirtualMachine(String vmId, String name, String description, AsynchronousTask<String> task) throws CloudException, InternalException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.CREATE_IMAGE);           
        NodeList blocks;
        EC2Method method;
        Document doc;
        
        parameters.put("InstanceId", vmId);
        parameters.put("Name", name + "-" + System.currentTimeMillis());
        parameters.put("Description", description);
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
            
            return imageIdNode.getFirstChild().getNodeValue().trim();
        }
        return null;
    }

    @Override
    public AsynchronousTask<String> imageVirtualMachineToStorage(String vmId, String name, String description, String directory) throws CloudException, InternalException {
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
            uploadPolicy.append(directory);
            uploadPolicy.append("\"},");
            uploadPolicy.append("{\"acl\": \"ec2-bundle-read\"},");
            uploadPolicy.append("[\"starts-with\", \"$key\", \"");
            uploadPolicy.append(name);
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
        parameters.put("InstanceId", vmId);
        parameters.put("Storage.S3.Bucket", directory);
        parameters.put("Storage.S3.Prefix", name);
        parameters.put("Storage.S3.AWSAccessKeyId", provider.getContext().getAccountNumber());
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
        final String bundleId = blocks.item(0).getFirstChild().getNodeValue();
        final AsynchronousTask<String> task = new AsynchronousTask<String>();
        final String manifest = (directory + "/" + name + ".manifest.xml");
        
        Thread t = new Thread() {   
            public void run() {
                waitForBundle(bundleId, manifest, task);
                
            }
        };
        
        t.setName("Bundle Task: " + manifest);
        t.setDaemon(true);
        t.start();
        return task;
    }
    
    @Override
    public String installImageFromUpload(MachineImageFormat format, InputStream imageStream) throws CloudException, InternalException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public boolean isImageSharedWithPublic(String machineImageId) throws CloudException, InternalException {
        MachineImage image = getMachineImage(machineImageId);
        
        if( image == null ) {
            return false;
        }
        String p = (String)image.getTag("public");
        
        return (p != null && p.equalsIgnoreCase("true"));
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_IMAGES);
        EC2Method method;

        if( provider.isAmazon() ) {
            parameters.put("Owner", provider.getContext().getAccountNumber());
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
    
	@Override
    public Collection<MachineImage> listMachineImages() throws InternalException, CloudException {
        PopulatorThread<MachineImage> populator;
        
        provider.hold();
        populator = new PopulatorThread<MachineImage>(new JiteratorPopulator<MachineImage>() {
            public void populate(Jiterator<MachineImage> iterator) throws CloudException, InternalException {
                try {
                    populateImages(null, iterator);
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
    public Collection<MachineImage> listMachineImagesOwnedBy(String owner) throws InternalException, CloudException {
        PopulatorThread<MachineImage> populator;
        final String acct = owner;
        
        provider.hold();
        populator = new PopulatorThread<MachineImage>(new JiteratorPopulator<MachineImage>() {
            public void populate(Jiterator<MachineImage> iterator) throws CloudException, InternalException {
                try {
                    populateImages(acct, iterator);
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
    public Iterable<String> listShares(String forMachineImageId) throws CloudException, InternalException {
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
        };
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
    public Iterable<MachineImageFormat> listSupportedFormats() throws CloudException, InternalException {
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
    
    private boolean matches(MachineImage image, String keyword, Platform platform) {
        if( platform != null && !platform.equals(Platform.UNKNOWN) ) {
            Platform mine = image.getPlatform();
            
            if( platform.isWindows() && !mine.isWindows() ) {
                return false;
            }
            if( platform.isUnix() && !mine.isUnix() ) {
                return false;
            }
            if( platform.isBsd() && !mine.isBsd() ) {
                return false;
            }
            if( platform.isLinux() && !mine.isLinux() ) {
                return false;
            }
            if( platform.equals(Platform.UNIX) ) {
                if( !mine.isUnix() ) {
                    return false;
                }
            }
            else if( !platform.equals(mine) ) {
                return false;
            }
        }
        if( keyword != null ) {
            keyword = keyword.toLowerCase();
            if( !image.getDescription().toLowerCase().contains(keyword) ) {
                if( !image.getName().toLowerCase().contains(keyword) ) {
                    if( !image.getProviderMachineImageId().toLowerCase().contains(keyword) ) {
                        return false;
                    }
                }
            }
        }
        return true;
    }
    
	private void populateImages(String accountNumber, Jiterator<MachineImage> iterator) throws CloudException, InternalException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_IMAGES);
        EC2Method method;
        NodeList blocks;
		Document doc;

		if( accountNumber == null ) {
			accountNumber = provider.getContext().getAccountNumber();
		}
		if( provider.isAmazon() ) {
		    parameters.put("Owner", accountNumber);
		}
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
        if( provider.isAmazon() ) {
            parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_IMAGES);
            parameters.put("ExecutableBy", accountNumber);
            method = new EC2Method(provider, provider.getEc2Url(), parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            };
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
    
    @Override
    public String registerMachineImage(String atStorageLocation) throws CloudException, InternalException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.REGISTER_IMAGE);
        NodeList blocks;
        EC2Method method;
        Document doc;
        
        parameters.put("ImageLocation", atStorageLocation);
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
            
            return imageIdNode.getFirstChild().getNodeValue().trim();
        }
        return null;
    }
    
    @Override
    public void remove(String imageId) throws InternalException, CloudException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DEREGISTER_IMAGE);
        NodeList blocks;
        EC2Method method;
        Document doc;
        
        parameters.put("ImageId", imageId);
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            doc = method.invoke();
        }
        catch( EC2Exception e ) {
            logger.error(e.getSummary());
            throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("return");
        if( blocks.getLength() > 0 ) {
            Node imageIdNode = blocks.item(0);
            
            if( !imageIdNode.getFirstChild().getNodeValue().trim().equals("true") ) {
                throw new CloudException("Failed to de-register image " + imageId);
            }
        }
    }
    
    @Override
    public Iterable<MachineImage> searchMachineImages(String keyword, Platform platform, Architecture architecture) throws InternalException, CloudException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_IMAGES);
        ArrayList<MachineImage> list = new ArrayList<MachineImage>();
        NodeList blocks;
        EC2Method method;
        Document doc;
        
        parameters.put("ExecutableBy.1", "all");
        int filter = 1;
        if( architecture != null ) {
            parameters.put("Filter." + filter + ".Name", "architecture");
            parameters.put("Filter." + (filter++) + ".Value.1", architecture.equals(Architecture.I32) ? "i386" : "x86_64");
        }
        if( platform != null && platform.equals(Platform.WINDOWS) ) {
            parameters.put("Filter." + filter + ".Name", "platform");
            parameters.put("Filter." + (filter++) + ".Value.1", "windows");
        }
        parameters.put("Filter." + filter + ".Name", "state");
        parameters.put("Filter." + (filter++) + ".Value.1", "available");
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
                        if( matches(image, keyword, platform) ) {
                            list.add(image);
                        }
                    }
                }
            }
        }
        return list;
    }
	
    @Override
    public void shareMachineImage(String machineImageId, String withAccountId, boolean allowShare) throws CloudException, InternalException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.MODIFY_IMAGE_ATTRIBUTE);
		EC2Method method;
        NodeList blocks;
		Document doc;

		parameters.put("ImageId", machineImageId);
		if( withAccountId == null ) {
		    if( allowShare ) {
                parameters.put("LaunchPermission.Add.1.Group", "all");
		    }
		    else {
                parameters.put("LaunchPermission.Remove.1.Group", "all");		        
		    }
		}
		else {
		    if( allowShare ) {
                parameters.put("LaunchPermission.Add.1.UserId", withAccountId);
		    }
		    else {
                parameters.put("LaunchPermission.Remove.1.UserId", withAccountId);
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
        };
        blocks = doc.getElementsByTagName("return");
        if( blocks.getLength() > 0 ) {
        	if( !blocks.item(0).getFirstChild().getNodeValue().equalsIgnoreCase("true") ) {
        		throw new CloudException("Share of image failed without explanation.");
        	}
        }
	}
	
    @Override
    public boolean supportsCustomImages() {
        return true;
    }

    @Override
    public boolean supportsImageSharing() {
        return true;
    }

    @Override
    public boolean supportsImageSharingWithPublic() {
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
	
	private MachineImage toMachineImage(Node node) throws CloudException, InternalException {
		NodeList attributes = node.getChildNodes();
		MachineImage image = new MachineImage();
		String location = null;
		
		image.setSoftware(""); // TODO: guess software
		image.setProviderRegionId(provider.getContext().getRegionId());
		for( int i=0; i<attributes.getLength(); i++ ) {
			Node attribute = attributes.item(i);
			String name = attribute.getNodeName();
			
			if( name.equals("imageType") ) {
				String value = attribute.getFirstChild().getNodeValue().trim();
				
				if( !value.equals("machine") ) {
					return null;
				}
			}
			else if( name.equals("imageId") ) {
				String value = attribute.getFirstChild().getNodeValue().trim();				
				
				image.setProviderMachineImageId(value);
			}
			else if( name.equals("imageLocation") ) {
				location = attribute.getFirstChild().getNodeValue().trim();				
			}
			else if( name.equals("imageState") ) {
				String value = null;
				
				if( attribute.getChildNodes().getLength() > 0 ) {
					value = attribute.getFirstChild().getNodeValue();
				}
				if( value != null && value.trim().equalsIgnoreCase("available") ) {
				    image.setCurrentState(MachineImageState.ACTIVE);
				}
				else {
				    image.setCurrentState(MachineImageState.PENDING);
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
		if( !provider.isAmazon() ) {
		    image.setProviderOwnerId(provider.getContext().getAccountNumber());
		}
		return image;
	}
	
    @Override
    public String transfer(CloudProvider fromCloud, String machineImageId) throws CloudException, InternalException {
        // TODO Auto-generated method stub
        return null;
    }
    
	private void waitForBundle(String bundleId, String manifest, AsynchronousTask<String> task) {
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
		        };
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
		            				imageId = registerMachineImage(manifest);
		            				task.setPercentComplete(100.00);
		            				task.completeWithResult(imageId);
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
		        	catch( InterruptedException e ) { }
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
