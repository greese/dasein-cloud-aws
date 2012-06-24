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

package org.dasein.cloud.aws.storage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.http.Header;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.storage.S3Method.S3Response;
import org.dasein.cloud.encryption.Encryption;
import org.dasein.cloud.encryption.EncryptionException;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.storage.BlobStoreSupport;
import org.dasein.cloud.storage.CloudStoreObject;
import org.dasein.cloud.storage.FileTransfer;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorPopulator;
import org.dasein.util.PopulatorThread;
import org.dasein.util.Retry;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletResponse;

public class S3 implements BlobStoreSupport {
    static private final Logger logger = AWSCloud.getLogger(S3.class);
    
    private AWSCloud provider = null;
    
    public S3(AWSCloud provider) {
        this.provider = provider;
    }
    
    public void clear(String bucket) throws CloudException, InternalException {
        for( CloudStoreObject file : listFiles(bucket) ) {
            if( file.isContainer() ) {
                clear(bucket + "." + file.getName());
            }
            else {
                removeFile(file.getDirectory(), file.getName());
            }
        }
        removeDirectory(bucket);
    }
    
    public CloudStoreObject copy(CloudStoreObject file, CloudStoreObject toDirectory, String copyName) throws InternalException, CloudException {
    	if( file.isContainer() ) {
    		CloudStoreObject directory = new CloudStoreObject();
    		String pathName;
    		int idx;
             
    		directory.setContainer(true);
    		directory.setCreationDate(new Date());
    		directory.setSize(0);
    		if( file.getDirectory() != null ) {
    			pathName = createDirectory(file.getDirectory() + "." + copyName, true);
    		}
    		else {
    			pathName = createDirectory(copyName, true);
    		}
    		idx = pathName.lastIndexOf('.');
    		String tmp = pathName;
    		while( idx > -1 && idx == tmp.length()-1 ) {
    			tmp = tmp.substring(0, idx);
    			idx = tmp.lastIndexOf('.');
    		}
    		if( idx == -1 ) {
    			directory.setDirectory(null);
    			directory.setName(pathName);
    		}
    		else {
    			directory.setDirectory(pathName.substring(0, idx));
    			directory.setName(pathName.substring(idx+1));
    		}
    		for( CloudStoreObject f : listFiles(file.getDirectory()) ) {
    			copy(f, directory, f.getName());
    		}
    		return directory;
    	}
    	else {
    		return copyFile(file, toDirectory, copyName);
    	}
    }

    private void copy(InputStream input, OutputStream output, FileTransfer xfer) throws IOException {
        try {
            byte[] bytes = new byte[10240];
            long total = 0L;
            int count;
            
            if( xfer != null ) {
            	xfer.setBytesTransferred(0L);
            }
            while( (count = input.read(bytes, 0, 10240)) != -1 ) {
                if( count > 0 ) {
                    output.write(bytes, 0, count);
                	total = total + count;
                	if( xfer != null ) {
                		xfer.setBytesTransferred(total);
                	}
                }
            }
            output.flush();
        }
        finally {
            input.close();
            output.close();
        }
    }

    private CloudStoreObject copyFile(CloudStoreObject file, CloudStoreObject toDirectory, String newName) throws InternalException, CloudException {
    	HashMap<String,String> headers = new HashMap<String,String>();
    	CloudStoreObject replacement;
    	S3Method method;
    	
    	newName = verifyName(newName);
    	headers.put("x-amz-copy-source", "/" + file.getDirectory() + "/" + file.getName());
    	method = new S3Method(provider, S3Action.COPY_OBJECT, null, headers);
    	String source = (toDirectory.getDirectory() == null ? toDirectory.getName() : toDirectory.getDirectory() + "." + toDirectory.getName());
		try {
			method.invoke(source, newName);
		}
		catch( S3Exception e ) {
			logger.error("Error copying files in S3: " + e.getSummary());
			throw new CloudException(e);
		}
		replacement = new CloudStoreObject();
		replacement.setContainer(false);
		replacement.setCreationDate(new Date());
		replacement.setDirectory(source);
		replacement.setLocation("http://" + replacement.getDirectory() + ".s3.amazonaws.com/" + newName);
		replacement.setName(newName);
		replacement.setProviderRegionId(provider.getContext().getRegionId());
		replacement.setSize(file.getSize());
		return replacement;
    }
    
    public String createDirectory(String bucket, boolean findFreeName) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();
        
        if( ctx == null ) {
            throw new InternalException("No context was set for this request");
        }
    	String regionId = ctx.getRegionId();
        
        if( regionId == null ) {
            throw new InternalException("No region ID was specified for this request");
        }
    	StringBuilder body = null;
    	boolean success;
    	int idx;

    	bucket = verifyName(bucket);
    	idx = bucket.lastIndexOf(".");
    	if( idx > -1 ) {
    		String dirName = bucket.substring(0,idx);
    		
    		bucket = createDirectory(dirName, true) + "." + bucket.substring(idx+1);
    	}
    	success = false;
    	if( regionId.equals("eu-west-1") ) {
        	body = new StringBuilder();
        	body.append("<CreateBucketConfiguration>\r\n");
        	body.append("<LocationConstraint>");
        	body.append("EU");
        	body.append("</LocationConstraint>\r\n");
        	body.append("</CreateBucketConfiguration>\r\n");
    	}
    	else if( regionId.equals("us-west-1") ) {
            body = new StringBuilder();
            body.append("<CreateBucketConfiguration>\r\n");
            body.append("<LocationConstraint>");
            body.append("us-west-1");
            body.append("</LocationConstraint>\r\n");
            body.append("</CreateBucketConfiguration>\r\n");
    	}      
    	else if( regionId.equals("ap-southeast-1") ) {
            body = new StringBuilder();
            body.append("<CreateBucketConfiguration>\r\n");
            body.append("<LocationConstraint>");
            body.append("ap-southeast-1");
            body.append("</LocationConstraint>\r\n");
            body.append("</CreateBucketConfiguration>\r\n");
        }
    	else if( !regionId.equals("us-east-1") ) {
            body = new StringBuilder();
            body.append("<CreateBucketConfiguration>\r\n");
            body.append("<LocationConstraint>");
            body.append(regionId);
            body.append("</LocationConstraint>\r\n");
            body.append("</CreateBucketConfiguration>\r\n");    	    
    	}
    	while( !success ) {
    		String ct = (body == null ? null : "text/xml; charset=utf-8");
    		S3Method method;
		
    		method = new S3Method(provider, S3Action.CREATE_BUCKET, null, null, ct, body == null ? null : body.toString());
    		try {
    			method.invoke(bucket, null);
    			success = true;
    		}
    		catch( S3Exception e ) {
    			String code = e.getCode();
    			
    			if( code != null && (code.equals("BucketAlreadyExists") || code.equals("BucketAlreadyOwnedByYou")) ) {
    				if( code.equals("BucketAlreadyOwnedByYou") ) {
    				    if( !isLocation(bucket) ) {
    				        bucket = findFreeName(bucket);
    				    }
    				    else {
    				        return bucket;
    				    }
    				}
    				else if( findFreeName ) {
    					bucket = findFreeName(bucket);
    				}
    				else {
    	    			throw new CloudException(e);    					
    				}
    			}
    			else {
    				logger.error(e.getSummary());
        			throw new CloudException(e);
    			}
    		}
    	}
        return bucket;
    }
    
    public FileTransfer download(CloudStoreObject cloudFile, File diskFile) throws CloudException, InternalException {
    	final FileTransfer transfer = new FileTransfer();
        final CloudStoreObject source = cloudFile;
        final File target = diskFile;
        
        transfer.setBytesToTransfer(exists(cloudFile.getDirectory(), cloudFile.getName(), false));
        if( transfer.getBytesToTransfer() == -1L ) {
        	throw new CloudException("No such file: " + cloudFile.getDirectory() + "." + cloudFile.getName());
        }
        Thread t = new Thread() {
        	public void run() {
		        Callable<Object> operation = new Callable<Object>() {
		            public Object call() throws Exception {
		                boolean success = false;
		                
		                try {
		                    get(source.getDirectory(), source.getName(), target, transfer);
		                    success = true;
		                    return null;
		                }
		                finally {
		                    if( !success ) {
		                        if( target.exists() ) {
		                            target.delete();
		                        }
		                    }
		                }
		            }
		        };
		        try {
		            (new Retry<Object>()).retry(5, operation);
                    transfer.complete(null);
		        }
		        catch( CloudException e ) {
		        	transfer.complete(e);
		        }
		        catch( InternalException e ) {
		        	transfer.complete(e);
		        }
		        catch( Throwable t ) {
		            logger.error(t);
		            t.printStackTrace();
		            transfer.complete(t);
		        }
        	}
        };
        
        t.setDaemon(true);
        t.start();
        return transfer;
    }

    public FileTransfer download(String directory, String fileName, File toFile, Encryption encryption) throws InternalException, CloudException {
    	final FileTransfer transfer = new FileTransfer();
        final Encryption enc = encryption;
        final String dname = directory;
        final String fname = fileName;
        final File target = toFile;

        Thread t = new Thread() {
        	public void run() {
                try {
		            Callable<Object> operation = new Callable<Object>() {
		                public Object call() throws Exception {
		                    boolean success = false;
		                    
		                    try {
		                        downloadMultipartFile(dname, fname, target, transfer, enc);
		                        success = true;
		                        return null;
		                    }
		                    finally {
		                        if( !success ) {
		                            if( target.exists() ) {
		                                target.delete();
		                            }
		                        }
		                    }
		                }
		            };
		            try {
		                (new Retry<Object>()).retry(5, operation);
		                transfer.complete(null);
		            }
		            catch( CloudException e ) {
		            	transfer.complete(e);
		            }
		            catch( InternalException e ) {
		            	transfer.complete(e);
		            }
		            catch( Throwable t ) {
		                logger.error(t);
		                t.printStackTrace();
		                transfer.complete(t);
		            }
		        }
		        finally {
		            if( enc != null ) {
		                enc.clear();
		            }
		        }
            }
        };
        
        t.setDaemon(true);
        t.start();
        return transfer;
    }
    
    private void downloadMultipartFile(String directory, String fileName, File restoreFile, FileTransfer transfer, Encryption encryption) throws CloudException, InternalException {
        try {
            File f;
            String str;
            int parts;
            
            if( restoreFile.exists() ) {
                if( !restoreFile.delete() ) {
                    throw new InternalException("Unable to delete restore file: " + restoreFile.getAbsolutePath());
                }
            }
            f = File.createTempFile("download", ".dl");
            f.deleteOnExit();
            Properties props = new Properties();
            try {
                get(directory, fileName + ".properties", f, transfer);                
                props.load(new FileInputStream(f));
            }
            finally {
                f.delete();
            }
            try {
                str = props.getProperty("parts");
                parts = (str == null ? 1 : Integer.parseInt(str));
                
                String checksum = props.getProperty("checksum");
                
                File encFile = null;
                
                if( encryption != null ) {
                    encFile = new File(restoreFile.getAbsolutePath() + ".enc");
                    if( encFile.exists() ) {
                        encFile.delete();
                    }
                }
                for( int i = 1; i<=parts; i++ ) {
                    FileOutputStream out;
                    FileInputStream in;
                    
                    if( f.exists() ) {
                        f.delete();
                    }
                    f = File.createTempFile("part", "." + i);
                    get(directory, fileName + ".part." + i, f, transfer);
                    in = new FileInputStream(f);
                    if( encryption != null ) {
                        out = new FileOutputStream(encFile, true);
                    }
                    else {
                        out = new FileOutputStream(restoreFile, true);                
                    }
                    copy(in, out, transfer);
                }
                if( encryption != null ) {
                    try {
                        try {
                            try {
                                if( !S3Method.getChecksum(encFile).equals(checksum) ) {
                                    throw new IOException("Checksum mismatch.");
                                }
                            }
                            catch( NoSuchAlgorithmException e ) {
                                logger.error(e);
                                e.printStackTrace();
                                throw new InternalException(e.getMessage());
                            }
                            encryption.decrypt(new FileInputStream(encFile), new FileOutputStream(restoreFile));
                        }
                        finally {
                            if( encFile.exists() ) {
                                encFile.delete();
                            }
                        }
                    }
                    catch( EncryptionException e ) {
                        logger.error(e);
                        e.printStackTrace();
                        throw new InternalException(e);
                    }
                }
                else {
                    try {
                        if( !S3Method.getChecksum(restoreFile).equals(checksum) ) {
                            throw new IOException("Checksum mismatch.");
                        }
                    }
                    catch( NoSuchAlgorithmException e ) {
                    	logger.error(e);
                        e.printStackTrace();
                        throw new InternalException(e.getMessage());
                    }
                }
            }
            finally {
                if( f != null && f.exists() ) {
                    f.delete();
                }
            }
        }
        catch( IOException e ) {
            logger.error(e);
            e.printStackTrace();
            throw new InternalException(e);
        }
    }
    
    public boolean exists(String bucket) throws InternalException, CloudException {
		S3Method method = new S3Method(provider, S3Action.LOCATE_BUCKET);
		
		try {
			method.invoke(bucket, "?location");
			return true;
		}
		catch( S3Exception e ) {
			if( e.getStatus() != HttpServletResponse.SC_NOT_FOUND ) {
				String code = e.getCode();

				if( code != null && code.equals("AccessDenied") ) {
					return true;
				}
				if( code == null || !code.equals("NoSuchBucket") ) {
					logger.error(e.getSummary());
					throw new CloudException(e);
				}
			}
		}
		return false;
    }
    
    public boolean belongsToAnother(String bucket) throws InternalException, CloudException {
		S3Method method = new S3Method(provider, S3Action.LOCATE_BUCKET);
		
		try {
			method.invoke(bucket, "?location");
			return false;
		}
		catch( S3Exception e ) {
			if( e.getStatus() != HttpServletResponse.SC_NOT_FOUND ) {
				String code = e.getCode();
			
				if( code != null && code.equals("AccessDenied") ) {
					return true;
				}
				if( code == null || !code.equals("NoSuchBucket") ) {
				    String message = e.getMessage();
				    
				    if( message != null ) {
				        if( message.contains("Access forbidden") ) {
				            return true;
				        }
				    }
					logger.error(e.getSummary() + " (" + e.getCode() + ")");
					throw new CloudException(e);
				}
			}
		}
		return false;
    }
    
    public long exists(String bucket, String object, boolean multiPart) throws InternalException, CloudException {
    	if( object == null ) {
    		return 0L;
    	}
    	if( !multiPart ) {
    		S3Method method = new S3Method(provider, S3Action.OBJECT_EXISTS);
    		S3Response response;
    		
    		try {
    			response = method.invoke(bucket, object);
    			if( response != null && response.headers != null ) {
    			    for( Header header : response.headers ) {
    			        if( header.getName().equalsIgnoreCase("Content-Length") ) {
    			            return Long.parseLong(header.getValue());
    			        }
    				}
    			}
    			return 0L;
    		}
    		catch( S3Exception e ) {
    			if( e.getStatus() != HttpServletResponse.SC_NOT_FOUND ) {
    				String code = e.getCode();
    			
    				if( code == null || (!code.equals("NoSuchBucket") && !code.equals("NoSuchKey")) ) {
    					logger.error(e.getSummary());
    					throw new CloudException(e);
    				}
    			}
    			return -1L;
    		}    		
    	}
    	else {
    		if( exists(bucket, object + ".properties", false) == -1L ) {
    			return -1L;
    		}
    		try {
		        File propsFile = File.createTempFile("props", ".properties");
                Properties props = new Properties();
                String str;

		        try {
		            get(bucket, object + ".properties", propsFile, null);        
		            props.load(new FileInputStream(propsFile));
		        }
		        finally {
		            propsFile.delete();
		        }
		        str = props.getProperty("length");
		        if( str == null ) {
		        	return 0L;
		        }
		        else {
		        	return Long.parseLong(str);
		        }
    		}
    		catch( IOException e ) {
    			logger.error(e);
    			e.printStackTrace();
    			throw new InternalException(e);
    		}
    	}
    }
    
    private String findFreeName(String bucket) throws InternalException, CloudException {
    	int idx = bucket.lastIndexOf(".");
    	String prefix, rawName;
    	
    	if( idx == -1 ) {
    		prefix = null;
    		rawName = bucket;
    		bucket = rawName;
    	}
    	else {
    		prefix = bucket.substring(0, idx);
    		rawName = bucket.substring(idx+1);
    		bucket = prefix + "." + rawName;
    	}
    	while( belongsToAnother(bucket) || (exists(bucket) && !isLocation(bucket)) ) {
    		idx = rawName.lastIndexOf("-");
    		if( idx == -1 ) {
    			rawName = rawName + "-1";
    		}
    		else if( idx == rawName.length()-1 ) {
    			rawName = rawName + "1";
    		}
    		else {
    			String postfix = rawName.substring(idx+1);
    			int x;
    			
    			try {
    			    x = Integer.parseInt(postfix) + 1;
                    rawName = rawName.substring(0,idx) + "-" + x;
    			}
    			catch( NumberFormatException e ) {
    			    rawName = rawName + "-1";
    			}
    		}
    		if( prefix == null) {
    			bucket = rawName;
    		}
    		else {
    			bucket = prefix + "." + rawName;
    		}
    	}
    	return bucket;
    }
    
    private void get(String bucket, String object, File toFile, FileTransfer transfer) throws InternalException, CloudException {
    	IOException lastError = null;
    	int attempts = 0;
    	
    	while( attempts < 5 ) {
    		S3Method method = new S3Method(provider, S3Action.GET_OBJECT);
    		S3Response response;
		
    		try {
    		    response = method.invoke(bucket, object);
        		try {
        			copy(response.input, new FileOutputStream(toFile), transfer);
        			return;
        		} 
        		catch( FileNotFoundException e ) {
        			logger.error(e);
        			e.printStackTrace();
        			throw new InternalException(e);
        		} 
        		catch( IOException e ) {
        			lastError = e;
        			logger.warn(e);
        			try { Thread.sleep(10000L); }
        			catch( InterruptedException ignore ) { }
        		}
        		finally {
        			response.close();
        		}
    		}
    		catch( S3Exception e ) {
    			logger.error(e.getSummary());
    			throw new CloudException(e);
    		} 
    		attempts++;
    	}
		logger.error(lastError);
		lastError.printStackTrace();
		throw new InternalException(lastError);
    }

    public Document getAcl(String bucket, String object) throws CloudException, InternalException {
		S3Method method;
	
		method = new S3Method(provider, S3Action.GET_ACL);
		try {
			S3Response response = method.invoke(bucket, object == null ? "?acl" : object + "?acl");
			
			return (response == null ? null : response.document);
		}
		catch( S3Exception e ) {
			logger.error(e.getSummary());
			throw new CloudException(e);
		}      	
    }
    
    public long getMaxFileSizeInBytes() {
        return 5000000000L;
    }
    
    public String getProviderTermForDirectory(Locale locale) {
        return "bucket";
    }
    
    public String getProviderTermForFile(Locale locale) {
        return "object";
    }
    
    private boolean isLocation(String bucket) throws CloudException, InternalException {
        if( bucket != null ) {
    		S3Method method = new S3Method(provider, S3Action.LOCATE_BUCKET);
    		String regionId = provider.getContext().getRegionId();
    		S3Response response;
    		
    		try {
    			response = method.invoke(bucket, "?location");
    		}
    		catch( S3Exception e ) {
    			String code = e.getCode();
    			
    			if( code == null || !code.equals("NoSuchBucket") ) {
    				logger.error(e.getSummary());
    				throw new CloudException(e);
    			}
    			response = null;
    		}
    		if( response != null ) {
    			NodeList constraints = response.document.getElementsByTagName("LocationConstraint");
    			if( constraints.getLength() > 0 ) {
    				Node constraint = constraints.item(0);
    				
    				if( constraint != null && constraint.hasChildNodes() ) {
    					String location = constraint.getFirstChild().getNodeValue().trim();
    
    					if( location.equals("EU") && !regionId.equals("eu-west-1") ) {
    						return false;
    					}
    					else if( location.equals("us-west-1") && !regionId.equals("us-west-1") ) {
    					    return false;
    					}
    					else if( location.startsWith("ap-") && !regionId.equals(location) ) {
    					    return false;
    					}
    					else if( location.equals("US") && !regionId.equals("us-east-1") ) {
    						return false;
    					}
    				}
                    else {
                        return regionId.equals("us-east-1");
                    }
    			}
                else {
                    return regionId.equals("us-east-1");
                }
    		}
        }
		return true;
    }
    
    public boolean isPublic(String bucket, String object) throws CloudException, InternalException {
    	Document acl = getAcl(bucket, object);
    	NodeList grants;
    	
    	grants = acl.getElementsByTagName("Grant");
    	for( int i=0; i<grants.getLength(); i++ ) {
    		boolean isAll = false, isRead = false;
    		Node grant = grants.item(i);
    		NodeList grantData;
    		
    		grantData = grant.getChildNodes();
    		for( int j=0; j<grantData.getLength(); j++ ) {
    			Node item = grantData.item(j);
    			
    			if( item.getNodeName().equals("Grantee") ) {
    				String type = item.getAttributes().getNamedItem("xsi:type").getNodeValue();
    			
    				if( type.equals("Group") ) {
    					NodeList items = item.getChildNodes();
    				
    					for( int k=0; k<items.getLength(); k++ ) {
    						Node n = items.item(k);
    					
    						if( n.getNodeName().equals("URI") ) {
    							if( n.hasChildNodes() ) {
    								String uri = n.getFirstChild().getNodeValue();
    							
    								if( uri.equals("http://acs.amazonaws.com/groups/global/AllUsers") ) {
    									isAll = true;
    									break;
    								}
    							}
    						}
        					if( isAll ) {
        						break;
        					}
    					}
    				}
    			}
    			else if( item.getNodeName().equals("Permission") ) {
    				if( item.hasChildNodes() ) {
    					String perm = item.getFirstChild().getNodeValue();
    					
    					isRead = (perm.equals("READ") || perm.equals("FULL_CONTROL")); 
    				}
    			}
    		}
    		if( isAll ) {
    			return isRead;
    		}
    	}
    	return false;
    }
    
    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        S3Method method = new S3Method(provider, S3Action.LIST_BUCKETS);
        
        try {
            method.invoke(null, null);
            return true;
        }
        catch( S3Exception e ) {
            return false;
        }
    }
    
    public Collection<CloudStoreObject> listFiles(String bucket) throws CloudException, InternalException {
    	PopulatorThread <CloudStoreObject> populator;
    	final String dir = bucket;
    	
    	if( !isLocation(dir) ) {
    		throw new CloudException("No such bucket in target region: " + dir + "/" + provider.getContext().getRegionId());
    	}
    	provider.hold();
    	populator = new PopulatorThread<CloudStoreObject>(new JiteratorPopulator<CloudStoreObject>() {
    		public void populate(Jiterator<CloudStoreObject> iterator) throws CloudException, InternalException {
    			listFiles(dir, iterator);
    			provider.release();
    		}
    	});
    	populator.populate();
    	return populator.getResult();
    }

    private void listFiles(String bucket, Jiterator<CloudStoreObject> iterator) throws CloudException, InternalException {
    	loadDirectories(bucket, iterator);
    	if( bucket != null ) {
    		loadFiles(bucket, iterator);
    	}
    }

    private void loadDirectories(String bucket, Jiterator<CloudStoreObject> iterator) throws CloudException, InternalException {
    	S3Method method = new S3Method(provider, S3Action.LIST_BUCKETS);
		S3Response response;
		NodeList blocks;		
		
		try {
			response = method.invoke(null, null);
		}
		catch( S3Exception e ) {
			logger.error(e.getSummary());
			throw new CloudException(e);
		}
		blocks = response.document.getElementsByTagName("Bucket");
		for( int i=0; i<blocks.getLength(); i++ ) {
			String dateString = null, name = null;
			Node object = blocks.item(i);
			NodeList attrs;
			
			attrs = object.getChildNodes();
			for( int j=0; j<attrs.getLength(); j++ ) {
				Node attr = attrs.item(j);
				
				if( attr.getNodeName().equals("Name") ) {
					name = attr.getFirstChild().getNodeValue().trim();
				}
				else if( attr.getNodeName().equals("CreationDate") ) {
					dateString = attr.getFirstChild().getNodeValue().trim();
				}
			}
			if( name == null ) { 
				throw new CloudException("Bad response from server.");
			}
			if( bucket == null ) {
				if( name.indexOf(".") > -1 ) { // a sub-directory of another directory
					continue;
				}
			}
			else if( name.equals(bucket) ) {
				continue;
			}
			else if( !name.startsWith(bucket + ".") ) {
				continue;
			}
			else if( bucket.equals(name) ) {
				continue;
			}
			if( bucket != null ) {
				String tmp = name.substring(bucket.length() + 1);
				int idx = tmp.indexOf(".");
				
				if( idx > 0 /* yes 0, not -1 */ && idx < (tmp.length()-1) ) { // this is a sub of a sub
					continue;
				}
			}
			if( bucket == null ) {
    			method = new S3Method(provider, S3Action.LOCATE_BUCKET);
    			try {
    				response = method.invoke(name, "?location");
    			}
    			catch( S3Exception e ) {
    				if( e.getStatus() != HttpServletResponse.SC_NOT_FOUND ) {
    					String code = e.getCode();
    				
    					if( code == null || !code.equals("NoSuchBucket") ) {
    						logger.error(e.getSummary());
    						throw new CloudException(e);
    					}
    				}
    				response = null;
    			}
    			if( response != null ) {
    				NodeList constraints = response.document.getElementsByTagName("LocationConstraint");
    				if( constraints.getLength() > 0 ) {
    					Node constraint = constraints.item(0);
    					
    					if( constraint != null && constraint.hasChildNodes() ) {
    						String location = constraint.getFirstChild().getNodeValue().trim();
    	
    						if( location.equals("EU") && !provider.getContext().getRegionId().equals("eu-west-1") ) {
    							continue;
    						}
    						else if( location.equals("us-west-1") && !provider.getContext().getRegionId().equals("us-west-1") ) {
    						    continue;
    						}
    						else if( location.startsWith("ap-") && !provider.getContext().getRegionId().equals(location) ) {
    						    continue;
    						}
    						else if( location.equals("US") && !provider.getContext().getRegionId().equals("us-east-1") ) {
    							continue;
    						}
    					}
                        else if( !provider.getContext().getRegionId().equals("us-east-1") ){
                            continue;
                        }
    				}
                    else if( !provider.getContext().getRegionId().equals("us-east-1") ){
    				    continue;
    				}
    			}
    			else {
    				continue;
    			}
			}
			CloudStoreObject file = new CloudStoreObject();
			String[] parts = name.split("\\.");
			
			if( parts == null || parts.length < 2 ) {
				file.setName(name);
				file.setDirectory(null);
			}
			else {
				StringBuilder dirName = new StringBuilder();
				
				file.setName(parts[parts.length-1]);
				for( int part=0; part<parts.length-1; part++ ) {
					if( dirName.length() > 0 ) {
						dirName.append(".");
					}
					dirName.append(parts[part]);
				}
				file.setDirectory(dirName.toString());
			}
			file.setContainer(true);
			file.setProviderRegionId(provider.getContext().getRegionId());
			file.setLocation("http://" + name + ".s3.amazonaws.com");
			file.setSize(0L);
			if( dateString != null ) {
				try {
					SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
				
					file.setCreationDate(fmt.parse(dateString));
				} 
				catch( ParseException e ) {
					logger.error(e);
					e.printStackTrace();
					throw new CloudException(e);
				}
			}
			else {
				file.setCreationDate(new Date());
			}
			iterator.push(file);
		}    	
    }
    
    private void loadFiles(String bucket, Jiterator<CloudStoreObject> iterator) throws CloudException, InternalException {
		HashMap<String,String> parameters = new HashMap<String,String>();
		S3Response response;
		String marker = null;
		boolean done = false;
		S3Method method;
		
		while( !done ) {
			NodeList blocks;
			
			parameters.clear();
			if( marker != null ) {
				parameters.put("marker", marker);
			}
			parameters.put("max-keys", String.valueOf(30));
			method = new S3Method(provider, S3Action.LIST_CONTENTS, parameters, null);
			try {
				response = method.invoke(bucket, null);
			}
			catch( S3Exception e ) {
			    String code = e.getCode();
			    
			    if( code == null || !code.equals("SignatureDoesNotMatch") ) {
			        throw new CloudException(e);
			    }
				logger.error(e.getSummary());
				throw new CloudException(e);
			}
			blocks = response.document.getElementsByTagName("IsTruncated");
			if( blocks.getLength() > 0 ) {
				done = blocks.item(0).getFirstChild().getNodeValue().trim().equalsIgnoreCase("false");
			}
			blocks = response.document.getElementsByTagName("Contents");
			for( int i=0; i<blocks.getLength(); i++ ) {
				CloudStoreObject file = new CloudStoreObject();
				Node object = blocks.item(i);
				
				file.setDirectory(bucket);
				file.setContainer(false);
				file.setProviderRegionId(provider.getContext().getRegionId());
				if( object.hasChildNodes() ) {
					NodeList attrs = object.getChildNodes();
					
					for( int j=0; j<attrs.getLength(); j++ ) {
						Node attr = attrs.item(j);
						String name;
						
						name = attr.getNodeName();
						if( name.equals("Key") ) {
							String key = attr.getFirstChild().getNodeValue().trim();
							
							file.setName(key);
							file.setLocation("http://" + bucket + ".s3.amazonaws.com/" + key);
							marker = key;
						}
						else if( name.equals("Size") ) {
							file.setSize(Long.parseLong(attr.getFirstChild().getNodeValue().trim()));
						}
						else if( name.equals("LastModified") ) {
							SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
							String dateString = attr.getFirstChild().getNodeValue().trim();
							
							try {
								file.setCreationDate(fmt.parse(dateString));
							} 
							catch( ParseException e ) {
								logger.error(e);
								e.printStackTrace();
								throw new CloudException(e);
							}
						}
					}
				}
				iterator.push(file);
			}
		}    	
    }
    
    public void makePublic(String bucket) throws InternalException, CloudException {
    	makePublic(bucket, null);
    }
    
    public void makePublic(String bucket, String object) throws InternalException, CloudException {
    	Document current = getAcl(bucket, object);
    	StringBuilder xml = new StringBuilder();
    	NodeList blocks;

    	blocks = current.getDocumentElement().getChildNodes();
    	xml.append("<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    	for( int i=0; i<blocks.getLength(); i++ ) {
    		Node n = blocks.item(i);
    		
    		if( n.getNodeName().equals("Owner") ) {
    			NodeList attrs = n.getChildNodes();
    			
    			xml.append("<Owner>");
    			for( int j=0; j<attrs.getLength(); j++ ) {
    				Node attr = attrs.item(j);
    				
    				if( attr.getNodeName().equals("ID") ) {
    					xml.append("<ID>");
    					xml.append(attr.getFirstChild().getNodeValue().trim());
    					xml.append("</ID>");
    				}
    				else if( attr.getNodeName().equals("DisplayName") ) {
    					xml.append("<DisplayName>");
    					xml.append(attr.getFirstChild().getNodeValue().trim());
    					xml.append("</DisplayName>");
    				}
    			}
    			xml.append("</Owner>");
    		}
    		else if( n.getNodeName().equals("AccessControlList") ) {
    			NodeList attrs = n.getChildNodes();
    			boolean found = false;
    			
    			xml.append("<AccessControlList>");
    			for( int j=0; j<attrs.getLength(); j++ ) {
    				Node attr = attrs.item(j);
    				
    				if( attr.getNodeName().equals("Grant") ) {
    					NodeList subList = attr.getChildNodes();
    					boolean isAll = false;
    					
    					xml.append("<Grant>");
    					for( int k=0; k<subList.getLength(); k++ ) {
    						Node sub = subList.item(k);
    						
    						if( sub.getNodeName().equals("Grantee") ) {
    							String type = sub.getAttributes().getNamedItem("xsi:type").getNodeValue();
								NodeList agentInfo = sub.getChildNodes();
    							
    							xml.append("<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"");
    							xml.append(type);
    							xml.append("\">");								
								for( int l=0; l<agentInfo.getLength(); l++ ) {
									Node item = agentInfo.item(l);
									
									xml.append("<");
									xml.append(item.getNodeName());
									if( item.hasChildNodes() ) {
										String val = item.getFirstChild().getNodeValue();
										
										if( type.equals("Group") && item.getNodeName().equals("URI") && val.equals("http://acs.amazonaws.com/groups/global/AllUsers") ) {
											found = true;
											isAll = true;
										}
										xml.append(">");
										xml.append(item.getFirstChild().getNodeValue());
										xml.append("</");
										xml.append(item.getNodeName());
										xml.append(">");
									}
									else {
										xml.append("/>");
									}
								}
    							xml.append("</Grantee>");
    						}
    						else if( sub.getNodeName().equals("Permission") ) {
    							if( isAll ) {
    								xml.append("<Permission>READ</Permission>");
    							}
    							else {
    								xml.append("<Permission");
    								if( sub.hasChildNodes() ) {
    									xml.append(">");
    									xml.append(sub.getFirstChild().getNodeValue());
        								xml.append("</Permission>");
    								}
    								else {
    									xml.append("/>");
    								}
    							}
    						}
    					}
    					xml.append("</Grant>");
    				}
    			}
    			if( !found ) {
    				xml.append("<Grant>");
    				xml.append("<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\">");
    				xml.append("<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>");
    				xml.append("</Grantee>");
    				xml.append("<Permission>READ</Permission>");
    				xml.append("</Grant>");
    			}
    			xml.append("</AccessControlList>");
    		}
    	}
    	xml.append("</AccessControlPolicy>\r\n");    	
    	setAcl(bucket, object, xml.toString());
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        if( action.equals(BlobStoreSupport.ANY) ) {
            return new String[] { S3Method.S3_PREFIX + "*" };
        }
        else if( action.equals(BlobStoreSupport.CREATE_BUCKET) ) {
            return new String[] { S3Method.S3_PREFIX + "CreateBucket" };
        }
        else if( action.equals(BlobStoreSupport.DOWNLOAD) ) {
            return new String[] { S3Method.S3_PREFIX + "GetObject" };
        }
        else if( action.equals(BlobStoreSupport.GET_BUCKET) ) {
            return new String[] { S3Method.S3_PREFIX + "GetBucket" };
        }
        else if( action.equals(BlobStoreSupport.LIST_BUCKET) ) {
            return new String[] { S3Method.S3_PREFIX + "ListBucket" };
        }
        else if( action.equals(BlobStoreSupport.LIST_BUCKET_CONTENTS) ) {
            return new String[] { S3Method.S3_PREFIX + "ListBucket" };
        }
        else if( action.equals(BlobStoreSupport.MAKE_PUBLIC) ) {
            return new String[] { S3Method.S3_PREFIX + "PutAccessControlPolicy" };
        }
        else if( action.equals(BlobStoreSupport.REMOVE_BUCKET) ) {
            return new String[] { S3Method.S3_PREFIX + "DeleteBucket" };
        }
        else if( action.equals(BlobStoreSupport.UPLOAD) ) {
            return new String[] { S3Method.S3_PREFIX + "PutObject" };
        }
        return new String[0]; 
    }

    public void moveFile(String sourceBucket, String object, String targetBucket) throws InternalException, CloudException {
    	CloudStoreObject directory = new CloudStoreObject();
    	CloudStoreObject file = new CloudStoreObject();
    	String[] parts = targetBucket.split("\\.");
    	String dirPath, dirName;
    	
    	if( parts == null || parts.length < 2 ) {
    		dirPath = null;
    		dirName = targetBucket;
    	}
    	else {
    		StringBuilder str = new StringBuilder();
    		
    		dirName = parts[parts.length-1];
    		for( int i = 0; i<parts.length-1; i++ ) {
    			if( i > 0 ) {
    				str.append(".");
    			}
    			str.append(parts[i]);
    		}
    		dirPath = str.toString();
    	}
    	file.setContainer(false);
    	file.setDirectory(sourceBucket);
    	file.setName(object);
    	file.setProviderRegionId(provider.getContext().getRegionId());
    	directory.setContainer(true);
    	directory.setDirectory(dirPath);
    	directory.setName(dirName);
    	directory.setProviderRegionId(provider.getContext().getRegionId());
    	copy(file, directory, object);
    	removeFile(sourceBucket, object, false);    	
    }

    private void put(String bucket, String object, File file) throws CloudException, InternalException {
		boolean bucketIsPublic = isPublic(bucket, null);
		HashMap<String,String> headers = null;
    	S3Method method;
    	
    	if( bucketIsPublic ) {
    		headers = new HashMap<String,String>();
    		headers.put("x-amz-acl", "public-read");
    	}
    	method = new S3Method(provider, S3Action.PUT_OBJECT, null, headers, "application/octet-stream", file);
		try {			
			method.invoke(bucket, object);
		}
		catch( S3Exception e ) {
			throw new CloudException(e);
		}
    }

    private void put(String bucket, String object, String content) throws CloudException, InternalException {
		boolean bucketIsPublic = isPublic(bucket, null);
		HashMap<String,String> headers = null;
    	S3Method method;

    	if( bucketIsPublic ) {
    		headers = new HashMap<String,String>();
    		headers.put("x-amz-acl", "public-read");
    	}
    	File file = null;
    	try {
        	try {
        	    file = File.createTempFile(object, ".txt");
        	    PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file)));
        	    writer.print(content);
        	    writer.flush();
        	    writer.close();
        	}
        	catch( IOException e ) {
        	    logger.error(e);
        	    e.printStackTrace();
        	    throw new InternalException(e);
        	}
        	method = new S3Method(provider, S3Action.PUT_OBJECT, null, headers, "text/plain", file);
        	try {
    			method.invoke(bucket, object);
    		}
    		catch( S3Exception e ) {
    			logger.error(e.getSummary());
    			throw new CloudException(e);
    		}
    	}
        finally {
            if( file != null ) {
                file.delete();
            }
        }
    }
    
    public void removeDirectory(String bucket) throws CloudException, InternalException {
    	S3Method method = new S3Method(provider, S3Action.DELETE_BUCKET);
    	
		try {
			method.invoke(bucket, null);
		}
		catch( S3Exception e ) {
			String code = e.getCode();
			
			if( code != null && (code.equals("NoSuchBucket")) ) {
				return;
			}
			logger.error(e.getSummary());
			throw new CloudException(e);
		}
    }

    public void removeFile(String directory, String name, boolean multipartFile) throws CloudException, InternalException {
        if( !multipartFile ) {
            removeFile(directory, name);
        }
        else {
        	removeMultipart(directory, name);
        }
    }

    private void removeFile(String bucket, String name) throws CloudException, InternalException {
    	S3Method method = new S3Method(provider, S3Action.DELETE_OBJECT);
    	
		try {
			method.invoke(bucket, name);
		}
		catch( S3Exception e ) {
			String code = e.getCode();
			
			if( code != null && (code.equals("NoSuchBucket") || code.equals("NoSuchKey")) ) {
				return;
			}
			logger.error(e.getSummary());
			throw new CloudException(e);
		}
    }
    
    private void removeMultipart(String bucket, String object) throws InternalException, CloudException {
    	try {
	        File propsFile = File.createTempFile("props", ".properties");
	        Properties props = new Properties();
	        String str;
            int parts;
	        
            try {
                get(bucket, object + ".properties", propsFile, null);        
                props.load(new FileInputStream(propsFile));
            }
            finally {
                propsFile.delete();
            }
	        str = props.getProperty("parts");
	        parts = (str == null ? 1 : Integer.parseInt(str));
	        removeFile(bucket, object + ".properties");
	        for( int i = 1; i<=parts; i++ ) {
	        	removeFile(bucket, object + ".part." + i);
	        }  
    	}
    	catch( IOException e ) {
    		logger.error(e);
    		e.printStackTrace();
    		throw new InternalException(e);
    	}
    }
    
    public String renameDirectory(String oldName, String newName, boolean findFreeName) throws CloudException, InternalException {
        newName = createDirectory(newName, findFreeName);
        for( CloudStoreObject file : listFiles(oldName) ) {
            int retries = 10;
            
            while( true ) {
                retries--;
                try {
                    if( file.isContainer() ) {
                        renameDirectory(oldName + "." + file.getName(), newName + "." + file.getName(), true);
                    }
                    else {
                        moveFile(oldName, file.getName(), newName);
                    }                            
                    break;
                }
                catch( CloudException e ) {
                    if( retries < 1 ) {
                        throw e;
                    }
                }
                try { Thread.sleep(retries * 10000L); } 
                catch( InterruptedException e ) { }
            }
        }
        boolean ok = true;
        for( CloudStoreObject file : listFiles(oldName ) ) {
            if( file != null ) {
                ok = false;
            }
        }
        if( ok ) {
            removeDirectory(oldName);
        }
        return newName;
    }

    public void renameFile(String bucket, String object, String newName) throws CloudException, InternalException {
    	CloudStoreObject directory = new CloudStoreObject();
    	CloudStoreObject file = new CloudStoreObject();
    	String[] parts = bucket.split("\\.");
    	String dirPath, dirName;
    	
    	if( parts == null || parts.length < 2 ) {
    		dirPath = null;
    		dirName = bucket;
    	}
    	else {
    		StringBuilder str = new StringBuilder();
    		
    		dirName = parts[parts.length-1];
    		for( int i = 0; i<parts.length-1; i++ ) {
    			if( i > 0 ) {
    				str.append(".");
    			}
    			str.append(parts[i]);
    		}
    		dirPath = str.toString();
    	}
    	file.setContainer(false);
    	file.setDirectory(bucket);
    	file.setName(object);
    	file.setProviderRegionId(provider.getContext().getRegionId());
    	directory.setContainer(true);
    	directory.setDirectory(dirPath);
    	directory.setName(dirName);
    	directory.setProviderRegionId(provider.getContext().getRegionId());
    	copy(file, directory, newName);
    	removeFile(bucket, object, false);
    }
    
    private void setAcl(String bucket, String object, String body) throws CloudException, InternalException {
    	String ct = "text/xml; charset=utf-8";
		S3Method method;
	
		method = new S3Method(provider, S3Action.SET_ACL, null, null, null /* ct */, body);
		try {
			method.invoke(bucket, object == null ? "?acl" : object + "?acl");
		}
		catch( S3Exception e ) {
			logger.error(e.getSummary());
			throw new CloudException(e);
		}    	
    }
    
    public void upload(File source, String directory, String fileName, boolean multipart, Encryption encryption) throws CloudException, InternalException {
    	if( !exists(directory) ) {
    		createDirectory(directory, false);
    	}
    	if( encryption != null ) {
    	    multipart = true;
    	}
    	if( multipart ) {
            try {
                uploadMultipartFile(source, directory, fileName, encryption);
            }
            catch( InterruptedException e ) {
                logger.error(e);
                e.printStackTrace();
                throw new CloudException(e.getMessage());
            }
    	}
    	else {
    		put(directory, fileName, source);
    	}
    }
    
    private void uploadMultipartFile(File sourceFile, String directory, String fileName, Encryption encryption) throws InterruptedException, InternalException, CloudException {
    	long fileSize = sourceFile.length();
        String checksum;
        File toUpload;
        
        fileName = verifyName(fileName);
        if( encryption == null ) {
            try {
                toUpload = File.createTempFile(fileName, ".upl");
                copy(new FileInputStream(sourceFile), new FileOutputStream(toUpload), null);
            }
            catch( IOException e ) {
                logger.error(e);
                e.printStackTrace();
                throw new InternalException(e);
            }
        }
        else {
            try {
                File encryptedFile = File.createTempFile(sourceFile.getName(), ".enc");
                FileInputStream input = new FileInputStream(sourceFile);
                FileOutputStream output;
                
                encryptedFile.deleteOnExit();
                output = new FileOutputStream(encryptedFile);
                encryption.encrypt(input, output);
                input.close();
                output.flush();
                output.close();
                toUpload = encryptedFile;
            }
            catch( EncryptionException e ) {
                logger.error(e);
                e.printStackTrace();
                throw new InternalException(e);
            }
            catch( IOException e ) {
                logger.error(e);
                e.printStackTrace();
                throw new InternalException(e);
            }
        }
        try {
        	try {
				checksum = S3Method.getChecksum(toUpload);
			} 
        	catch( NoSuchAlgorithmException e ) {
        		logger.error(e);
        		e.printStackTrace();
        		throw new InternalException(e);
			}
            try {
                BufferedOutputStream output;
                BufferedInputStream input;
                byte[] buffer = new byte[1024];
                long count = 0;
                int b, partNumber = 1;
                File part;
                
                input = new BufferedInputStream(new FileInputStream(toUpload));
                part = new File(toUpload.getParent() + "/" + fileName + ".part." + partNumber);
                output = new BufferedOutputStream(new FileOutputStream(part));
                while( (b = input.read(buffer, 0, 1024)) > 0 ) {
                    count += b;
                    output.write(buffer, 0, b);
                    if( count >= 2000000000L ) {
                        int tries = 5;
                        
                        output.flush();
                        output.close();
                        while( true ) {
                            tries--;
                            try {
                                put(directory, fileName + ".part." + partNumber, part);
                                part.delete();
                                partNumber++;
                                part = new File(toUpload.getParent() + "/" + fileName + ".part." + partNumber);
                                output = new BufferedOutputStream(new FileOutputStream(part));
                                count = 0L;
                                break;
                            } 
                            catch( Exception e ) {
                                e.printStackTrace();
                                if( tries < 1 ) {
                                    throw new InternalException("Unable to complete upload for part " + partNumber + " of " + part.getAbsolutePath());
                                }
                            }
                        }
                    }
                }
                if( count > 0L ) {
                    int tries = 5;
                    
                    output.flush();
                    output.close();
                    while( true ) {
                        tries--;
                        try {
                            put(directory, fileName + ".part." + partNumber, part);
                            part.delete();
                            break;
                        } 
                        catch( Exception e ) {
                            e.printStackTrace();
                            if( tries < 1 ) {
                                throw new InternalException("Unable to complete upload for part " + partNumber + " of " + part.getAbsolutePath());
                            }
                        }
                    }                
                    part.delete();            
                }
                String content = "parts=" + partNumber + "\nchecksum=" + checksum + "\nlength=" + fileSize;
                
                if( encryption != null ) {
                    content = content + "\nencrypted=true\n";
                    content = content + "encryptionVersion=" + encryption.getClass().getName() + "\n";
                }
                else {
                    content = content + "\nencrypted=false\n";
                }
                int tries = 5;
                while( true ) {
                    tries--;
                    try {
                        put(directory, fileName + ".properties", content);
                        break;
                    } 
                    catch( Exception e ) {
                        e.printStackTrace();
                        if( tries < 1 ) {
                            throw new InternalException("Unable to complete upload for properties of " + part.getAbsolutePath());
                        }
                    }
                }
            }
            finally {
                toUpload.delete();
            }
        }
        catch( IOException e ) {
            logger.error(e);
            e.printStackTrace();
            throw new InternalException(e);
        }
    }
    
    static private String verifyName(String name) throws CloudException {
    	if( name == null ) {
    		return null;
    	}
    	StringBuilder str = new StringBuilder();
    	name = name.toLowerCase().trim();
    	if( name.length() > 255 ) {
    		String extra = name.substring(255);
    		int idx = extra.indexOf(".");
    		
    		if( idx > -1 ) {
    			throw new CloudException("S3 names are limited to 255 characters.");
    		}
    		name = name.substring(0,255);
    	}
        while( name.indexOf("--") != -1 ) {
            name = name.replaceAll("--", "-");         
        }
        while( name.indexOf("..") != -1 ) {
            name = name.replaceAll("\\.\\.", ".");         
        }
        while( name.indexOf(".-") != -1 ) {
            name = name.replaceAll("\\.-", ".");         
        }
        while( name.indexOf("-.") != -1 ) {
            name = name.replaceAll("-\\.", ".");         
        }
    	for( int i=0; i<name.length(); i++ ) {
    		char c = name.charAt(i);
    		
    		if( Character.isLetterOrDigit(c) ) {
    			str.append(c);
    		}
    		else {
    			if( i > 0 ) {
    				if( c == '/' ) {
    					c = '.';
    				}
    				else if( c != '.' && c != '-' ) {
    					c = '-';
    				}
    				str.append(c);
    			}
    		}
    	}
    	name = str.toString();
        while( name.indexOf("..") != -1 ) {
            name = name.replaceAll("\\.\\.", ".");         
        }
        if( name.length() < 1 ) { 
            return "000";
        }
    	while( name.charAt(name.length()-1) == '-' || name.charAt(name.length()-1) == '.' ) {
    		name = name.substring(0,name.length()-1);
            if( name.length() < 1 ) { 
                return "000";
            }
    	}
        if( name.length() < 1 ) { 
            return "000";
        }
        else if( name.length() == 1 ) {
    		name = name + "00";
    	}
    	else if ( name.length() == 2 ) {
    		name = name + "0";
    	}
    	return name;
    }
}
