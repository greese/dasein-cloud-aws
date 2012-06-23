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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Tag;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineProduct;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.compute.VmStatistics;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.util.CalendarWrapper;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class EC2Instance implements VirtualMachineSupport {
	static private final Logger logger = Logger.getLogger(EC2Instance.class);
	
    static private List<VirtualMachineProduct> sixtyFours;
    static private List<VirtualMachineProduct> thirtyTwos;

    static {
        ArrayList<VirtualMachineProduct> thirtyTwoSizes = new ArrayList<VirtualMachineProduct>();
        VirtualMachineProduct product = new VirtualMachineProduct();
        
        product.setProductId("t1.micro");
        product.setName("Micro Instance/(t1.micro)");
        product.setDescription("Micro Instance/(t1.micro)");
        product.setCpuCount(2);
        product.setDiskSizeInGb(1);
        product.setRamInMb(613);
        thirtyTwoSizes.add(product);
        
        product = new VirtualMachineProduct();
        product.setProductId("m1.small");
        product.setName("Small Instance (m1.small)");
        product.setDescription("Small Instance (m1.small)");
        product.setCpuCount(1);
        product.setDiskSizeInGb(160);
        product.setRamInMb(1700);
        thirtyTwoSizes.add(product);
        
        //AWS support medium
        product = new VirtualMachineProduct();
        product.setProductId("m1.medium");
        product.setName("Medium Instance (m1.medium)");
        product.setDescription("Medium Instance (m1.medium)");
        product.setCpuCount(2);
        product.setDiskSizeInGb(410);
        product.setRamInMb(3750);
        thirtyTwoSizes.add(product);        
        
        product = new VirtualMachineProduct();
        product.setProductId("c1.medium");
        product.setName("High-CPU Medium Instance (c1.medium)");
        product.setDescription("High-CPU Medium Instance (c1.medium)");
        product.setCpuCount(5);
        product.setDiskSizeInGb(350);
        product.setRamInMb(1700);
        thirtyTwoSizes.add(product);   

        thirtyTwos = Collections.unmodifiableList(thirtyTwoSizes);
        

        ArrayList<VirtualMachineProduct> sixtyFourSizes = new ArrayList<VirtualMachineProduct>();
       
        // EC2's m1.small, m1.medium  and c1.medium have 64bit platform
        sixtyFourSizes.addAll(thirtyTwoSizes);
            
        product = new VirtualMachineProduct();
        product.setProductId("m1.large");
        product.setName("Large Instance (m1.large)");
        product.setDescription("Large Instance (m1.large)");
        product.setCpuCount(4);
        product.setDiskSizeInGb(850);
        product.setRamInMb(7500);
        sixtyFourSizes.add(product); 
        
        product = new VirtualMachineProduct();
        product.setProductId("m1.xlarge");
        product.setName("Extra Large Instance (m1.xlarge)");
        product.setDescription("Extra Large Instance (m1.xlarge)");
        product.setCpuCount(8);
        product.setDiskSizeInGb(1690);
        product.setRamInMb(15000);
        sixtyFourSizes.add(product); 
        
        product = new VirtualMachineProduct();
        product.setProductId("c1.xlarge");
        product.setName("High-CPU Extra Large Instance (c1.xlarge)");
        product.setDescription("High-CPU Extra Large Instance (c1.xlarge)");
        product.setCpuCount(20);
        product.setDiskSizeInGb(1690);
        product.setRamInMb(7000);
        sixtyFourSizes.add(product); 
        
        product = new VirtualMachineProduct();
        product.setProductId("m2.xlarge");
        product.setName("High-Memory Extra Large Instance (m2.xlarge)");
        product.setDescription("High-Memory Extra Large Instance (m2.xlarge)");
        product.setCpuCount(7);
        product.setDiskSizeInGb(420);
        product.setRamInMb(17100);
        sixtyFourSizes.add(product); 
        
        product = new VirtualMachineProduct();
        product.setProductId("m2.2xlarge");
        product.setName("High-Memory Double Extra Large Instance (m2.2xlarge)");
        product.setDescription("High-Memory Double Extra Large Instance (m2.2xlarge)");
        product.setCpuCount(13);
        product.setDiskSizeInGb(850);
        product.setRamInMb(34200);
        sixtyFourSizes.add(product); 
        
        product = new VirtualMachineProduct();
        product.setProductId("m2.4xlarge");
        product.setName("High-Memory Quadruple Extra Large Instance (m2.4xlarge)");
        product.setDescription("High-Memory Quadruple Extra Large Instance (m2.4xlarge)");
        product.setCpuCount(26);
        product.setDiskSizeInGb(1690);
        product.setRamInMb(68400);
        sixtyFourSizes.add(product);
        
        product = new VirtualMachineProduct();
        product.setProductId("cc1.4xlarge");
        product.setName("Cluster Compute Quadruple Extra Large (cc1.4xlarge)");
        product.setDescription("33.5 EC2 Compute Units (2 x Intel Xeon X5570, quad-core \"Nehalem\" architecture)");
        product.setCpuCount(34);
        product.setDiskSizeInGb(1690);
        product.setRamInMb(23000);
        sixtyFourSizes.add(product);        
        sixtyFours = Collections.unmodifiableList(sixtyFourSizes);        
    }
	private AWSCloud provider = null;
	
	EC2Instance(AWSCloud provider) {
		this.provider = provider;
	}

	@Override
	public void boot(@Nonnull String instanceId) throws InternalException, CloudException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.START_INSTANCES);
        EC2Method method;
        
        parameters.put("InstanceId.1", instanceId);
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            method.invoke();
        }
        catch( EC2Exception e ) {
            logger.error(e.getSummary());
            throw new CloudException(e);
        }
	}

    static private class Metric implements Comparable<Metric> {
        int samples = 0;
        long timestamp = -1L;
        double minimum = -1.0;
        double maximum = 0.0;
        double average = 0.0;
        
        public int compareTo(Metric other) {
            if( other == this ) {
                return 0;
            }
            return (new Long(timestamp)).compareTo(other.timestamp);
        }
	}
	
	private Set<Metric> calculate(String metric, String unit, String instanceId, long startTimestamp, long endTimestamp) throws CloudException, InternalException {
	    if( !provider.isAmazon() ) {
	        return new TreeSet<Metric>();
	    }
        Map<String,String> parameters = provider.getStandardCloudWatchParameters(provider.getContext(), EC2Method.GET_METRIC_STATISTICS);
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        EC2Method method;
        NodeList blocks;
        Document doc;
        
        parameters.put("EndTime", fmt.format(new Date(endTimestamp)));
        parameters.put("StartTime", fmt.format(new Date(startTimestamp)));
        parameters.put("MeasureName", metric);
        parameters.put("Namespace", "AWS/EC2");
        parameters.put("Unit", unit);
        parameters.put("Dimensions.member.Name.1", "InstanceId");
        parameters.put("Dimensions.member.Value.1", instanceId);
        parameters.put("Statistics.member.1", "Average");
        parameters.put("Statistics.member.2", "Minimum");
        parameters.put("Statistics.member.3", "Maximum");
        parameters.put("Period", "60");
        method = new EC2Method(provider, getCloudWatchUrl(), parameters);
        try {
            doc = method.invoke();
        }
        catch( EC2Exception e ) {
            logger.error(e.getSummary());
            throw new CloudException(e);
        }
        TreeSet<Metric> metrics = new TreeSet<Metric>();
        fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        blocks = doc.getElementsByTagName("member");
        for( int i=0; i<blocks.getLength(); i++ ) {
            NodeList items = blocks.item(i).getChildNodes();
            Metric m = new Metric();
            
            for( int j=0; j<items.getLength(); j++ ) {
                Node item = items.item(j);
                
                if( item.getNodeName().equals("Timestamp") ) {
                    String dateString = item.getFirstChild().getNodeValue();
                    
                    try {
                        m.timestamp = fmt.parse(dateString).getTime();
                    }
                    catch( ParseException e ) {
                        logger.error(e);
                        e.printStackTrace();
                        throw new InternalException(e);
                    }
                }
                else if( item.getNodeName().equals("Average") ) {
                    m.average = Double.parseDouble(item.getFirstChild().getNodeValue());
                }
                else if( item.getNodeName().equals("Minimum") ) {
                    m.minimum = Double.parseDouble(item.getFirstChild().getNodeValue());
                }
                else if( item.getNodeName().equals("Maximum") ) {
                    m.maximum = Double.parseDouble(item.getFirstChild().getNodeValue());
                }
                else if( item.getNodeName().equals("Samples") ) {
                    m.samples = (int)Double.parseDouble(item.getFirstChild().getNodeValue());
                }
            }
            metrics.add(m);
        }
        return metrics;
    }
    
	private interface ApplyCalcs {
	    public void apply(VmStatistics stats, long start, long end, int samples, double average, double minimum, double maximum);
	}
	
    private void calculate(VmStatistics stats, String metricName, String unit, String instanceId, long startTimestamp, long endTimestamp, ApplyCalcs apply) throws CloudException, InternalException {
        Set<Metric> metrics = calculate(metricName, unit, instanceId, startTimestamp, endTimestamp);
        double minimum = -1.0, maximum = 0.0, sum = 0.0;
        long start = -1L, end = 0L;
        int samples = 0;
        
        for( Metric metric : metrics ) {
            if( start < 0L ) {
                start = metric.timestamp;
            }
            if( metric.timestamp > end ) {
                end = metric.timestamp;
            }
            samples++;
            if( metric.minimum < minimum || minimum < 0.0 ) {
                minimum = metric.minimum;
            }
            if( metric.maximum > maximum ) {
                maximum = metric.maximum;
            }
            sum += metric.average;
        }
        if( start < 0L ) {
            start = startTimestamp;
        }
        if( end < 0L ) {
            end = endTimestamp;
        }
        if( minimum < 0.0 ) {
            minimum = 0.0;
        }
        apply.apply(stats, start, end, samples, sum/samples, minimum, maximum);
    }
    
	private void calculateCpuUtilization(VmStatistics statistics, String instanceId, long startTimestamp, long endTimestamp) throws CloudException, InternalException {
	    ApplyCalcs apply = new ApplyCalcs() {
	        public void apply(VmStatistics stats, long start, long end, int samples, double average, double minimum, double maximum) {
	            stats.setSamples(samples);
	            stats.setStartTimestamp(start);
	            stats.setMinimumCpuUtilization(minimum);
	            stats.setAverageCpuUtilization(average);
	            stats.setMaximumCpuUtilization(maximum);
	            stats.setEndTimestamp(end);
	        }
	    };
	    calculate(statistics, "CPUUtilization", "Percent", instanceId, startTimestamp, endTimestamp, apply);
	}
	   
    private void calculateDiskReadBytes(VmStatistics statistics, String instanceId, long startTimestamp, long endTimestamp) throws CloudException, InternalException {
        ApplyCalcs apply = new ApplyCalcs() {
            public void apply(VmStatistics stats, long start, long end, int samples, double average, double minimum, double maximum) {
                stats.setMinimumDiskReadBytes(minimum);
                stats.setAverageDiskReadBytes(average);
                stats.setMaximumDiskReadBytes(maximum);
            }
        };
        calculate(statistics, "DiskReadBytes", "Bytes", instanceId, startTimestamp, endTimestamp, apply);
    }
    
    private void calculateDiskReadOps(VmStatistics statistics, String instanceId, long startTimestamp, long endTimestamp) throws CloudException, InternalException {
        ApplyCalcs apply = new ApplyCalcs() {
            public void apply(VmStatistics stats, long start, long end, int samples, double average, double minimum, double maximum) {
                stats.setMinimumDiskReadOperations(minimum);
                stats.setAverageDiskReadOperations(average);
                stats.setMaximumDiskReadOperations(maximum);
            }
        };
        calculate(statistics, "DiskReadOps", "Count", instanceId, startTimestamp, endTimestamp, apply);
    }
    
    private void calculateDiskWriteBytes(VmStatistics statistics, String instanceId, long startTimestamp, long endTimestamp) throws CloudException, InternalException {
        ApplyCalcs apply = new ApplyCalcs() {
            public void apply(VmStatistics stats, long start, long end, int samples, double average, double minimum, double maximum) {
                stats.setMinimumDiskWriteBytes(minimum);
                stats.setAverageDiskWriteBytes(average);
                stats.setMaximumDiskWriteBytes(maximum);
            }
        };
        calculate(statistics, "DiskWriteBytes", "Bytes", instanceId, startTimestamp, endTimestamp, apply);
    }
    
    private void calculateDiskWriteOps(VmStatistics statistics, String instanceId, long startTimestamp, long endTimestamp) throws CloudException, InternalException {
        ApplyCalcs apply = new ApplyCalcs() {
            public void apply(VmStatistics stats, long start, long end, int samples, double average, double minimum, double maximum) {
                stats.setMinimumDiskWriteOperations(minimum);
                stats.setAverageDiskWriteOperations(average);
                stats.setMaximumDiskWriteOperations(maximum);
            }
        };
        calculate(statistics, "DiskWriteOps", "Count", instanceId, startTimestamp, endTimestamp, apply);
    }

    private void calculateNetworkIn(VmStatistics statistics, String instanceId, long startTimestamp, long endTimestamp) throws CloudException, InternalException {
        ApplyCalcs apply = new ApplyCalcs() {
            public void apply(VmStatistics stats, long start, long end, int samples, double average, double minimum, double maximum) {
                stats.setMinimumNetworkIn(minimum);
                stats.setAverageNetworkIn(average);
                stats.setMaximumNetworkIn(maximum);
            }
        };
        calculate(statistics, "NetworkIn", "Bytes", instanceId, startTimestamp, endTimestamp, apply);
    }
    
    private void calculateNetworkOut(VmStatistics statistics, String instanceId, long startTimestamp, long endTimestamp) throws CloudException, InternalException {
        ApplyCalcs apply = new ApplyCalcs() {
            public void apply(VmStatistics stats, long start, long end, int samples, double average, double minimum, double maximum) {
                stats.setMinimumNetworkOut(minimum);
                stats.setAverageNetworkOut(average);
                stats.setMaximumNetworkOut(maximum);
            }
        };
        calculate(statistics, "NetworkOut", "Bytes", instanceId, startTimestamp, endTimestamp, apply);
    }

    @Override
    public @Nonnull VirtualMachine clone(@Nonnull String vmId, @Nonnull String intoDcId, @Nonnull String name, @Nonnull String description, boolean powerOn, @Nullable String... firewallIds) throws InternalException, CloudException {
        /*
        final ArrayList<Volume> oldVolumes = new ArrayList<Volume>();
        final ArrayList<Volume> newVolumes = new ArrayList<Volume>();
        final String id = serverId;
        final String zoneId = inZoneId;

        for( Volume volume : provider.getVolumeServices().list() ) {
            String svr = volume.getServerId();
            
            if( svr == null || !svr.equals(serverId)) {
                continue;
            }
            oldVolumes.add(volume);
        }
        Callable<ServerImage> imageTask = new Callable<ServerImage>() {
            @Override
            public ServerImage call() throws Exception {
                provider.getImageServices().create(id);
            }
            
        };
        Callable<Boolean> snapshotTask = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                for( Volume volume : oldVolumes ) {
                    String snapshotId = provider.getSnapshotServices().create(volume.getProviderVolumeId(), "Clone of " + volume.getName());
                    String volumeId = provider.getVolumeServices().create(snapshotId, volume.getSizeInGigabytes(), zoneId);
                    
                    newVolumes.add(provider.getVolumeServices().getVolume(volumeId));
                }
                return true;
            }
        };
        */
        throw new OperationNotSupportedException("AWS instances cannot be cloned.");
    }


    @Override
    public void enableAnalytics(String instanceId) throws InternalException, CloudException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.MONITOR_INSTANCES);
        EC2Method method;
        
        parameters.put("InstanceId.1", instanceId);
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            method.invoke();
        }
        catch( EC2Exception e ) {
            logger.error(e.getSummary());
            throw new CloudException(e);
        }
    }
    
	private Architecture getArchitecture(String size) {
		if( size.equals("m1.small") || size.equals("c1.medium") ) {
			return Architecture.I32;
		}
		else {
			return Architecture.I64;
		}
	}
	
	private String getCloudWatchUrl() {
        return "http://monitoring.amazonaws.com";
    }
    
	@Override
	public @Nonnull String getConsoleOutput(@Nonnull String instanceId) throws InternalException, CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.GET_CONSOLE_OUTPUT);
		String output = null;
		EC2Method method;
        NodeList blocks;
		Document doc;
        
		parameters.put("InstanceId", instanceId);
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
        	doc = method.invoke();
        }
        catch( EC2Exception e ) {
        	String code = e.getCode();
        	
        	if( code != null && code.startsWith("InvalidInstanceID") ) {
        		return "";
        	}
        	logger.error(e.getSummary());
        	throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("timestamp");
        for( int i=0; i<blocks.getLength(); i++ ) {
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            String ts = blocks.item(i).getFirstChild().getNodeValue();
            long timestamp;

            try {
                timestamp = fmt.parse(ts).getTime();
            } 
            catch( ParseException e ) {
                logger.error(e);
                e.printStackTrace();
                throw new CloudException(e);
            }
        	if( timestamp > -1L ) {
        		break;
        	}
        }
        blocks = doc.getElementsByTagName("output");
        for( int i=0; i<blocks.getLength(); i++ ) {
        	Node item = blocks.item(i);
        	
        	if( item.hasChildNodes() ) {
        	    output = item.getFirstChild().getNodeValue().trim();
        	    if( output != null ) {
        	        break;
        	    }
        	}
        }
        if( output != null ) {
            try {
                return new String(Base64.decodeBase64(output.getBytes("utf-8")), "utf-8");
            }
            catch( UnsupportedEncodingException e ) {
                logger.error(e);
                e.printStackTrace();
                throw new InternalException(e);
            }
        }
        return "";
	}

	@Override
	public @Nonnull Iterable<String> listFirewalls(@Nonnull String instanceId) throws InternalException, CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_INSTANCES);
		ArrayList<String> firewalls = new ArrayList<String>();
		EC2Method method;
        NodeList blocks;
		Document doc;
        
        parameters.put("InstanceId.1", instanceId);
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
        	doc = method.invoke();
        }
        catch( EC2Exception e ) {
        	String code = e.getCode();
        	
        	if( code != null && code.startsWith("InvalidInstanceID") ) {
        		return firewalls;
        	}
        	logger.error(e.getSummary());
        	throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("groupSet");
        for( int i=0; i<blocks.getLength(); i++ ) {
        	NodeList items = blocks.item(i).getChildNodes();
        	
            for( int j=0; j<items.getLength(); j++ ) {
            	Node item = items.item(j);
            	
            	if( item.getNodeName().equals("item") ) {
            		NodeList sub = item.getChildNodes();
            		
            		for( int k=0; k<sub.getLength(); k++ ) {
            			Node id = sub.item(k);
            			
            			if( id.getNodeName().equalsIgnoreCase("groupId") && id.hasChildNodes() ) {
            			    firewalls.add(id.getFirstChild().getNodeValue().trim());
            			    break;
            			}
            		}
            	}
            }
        }
        return firewalls;
	}
	
	@Override
	public @Nonnull String getProviderTermForServer(@Nonnull Locale locale) {
		return "instance";
	}

	@Override
	public @Nullable VirtualMachine getVirtualMachine(@Nonnull String instanceId) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();
        
        if( ctx == null ) {
            throw new CloudException("No context was established for this request");
        }
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_INSTANCES);
		EC2Method method;
        NodeList blocks;
		Document doc;
        
        parameters.put("InstanceId.1", instanceId);
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
        	doc = method.invoke();
        }
        catch( EC2Exception e ) {           
        	String code = e.getCode();
        	
        	if( code != null && code.startsWith("InvalidInstanceID") ) {
        		return null;
        	}
        	logger.error(e.getSummary());
        	throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("instancesSet");
        for( int i=0; i<blocks.getLength(); i++ ) {
        	NodeList instances = blocks.item(i).getChildNodes();
        	
            for( int j=0; j<instances.getLength(); j++ ) {
            	Node instance = instances.item(j);
            	
            	if( instance.getNodeName().equals("item") ) {
            		VirtualMachine server = toVirtualMachine(ctx, instance);
            		
            		if( server != null && server.getProviderVirtualMachineId().equals(instanceId) ) {
            			return server;
            		}
            	}
            }
        }
        return null;
	}
	
	@Override
	public @Nullable VirtualMachineProduct getProduct(@Nonnull String sizeId) {
        for( VirtualMachineProduct product : get64s() ) {
            if( product.getProductId().equals(sizeId) ) {
                return product;
            }
        }
        for( VirtualMachineProduct product : get32s() ) {
            if( product.getProductId().equals(sizeId) ) {
                return product;
            }
        }
        return null;
	}
	
	private VmState getServerState(String state) {
        if( state.equals("pending") ) {
            return VmState.PENDING;
        }
        else if( state.equals("running") ) {
            return VmState.RUNNING;
        }
        else if( state.equals("terminating") || state.equals("stopping") ) {
            return VmState.STOPPING;
        }
        else if( state.equals("stopped") ) {
            return VmState.PAUSED;
        }
        else if( state.equals("shutting-down") ) {
            return VmState.STOPPING;
        }
        else if( state.equals("terminated") ) {
            return VmState.TERMINATED;
        }
        else if( state.equals("rebooting") ) {
            return VmState.REBOOTING;
        }
        logger.warn("Unknown server state: " + state);
        return VmState.PENDING;
	}

	@Override
	public VmStatistics getVMStatistics(String instanceId, long startTimestamp, long endTimestamp) throws InternalException, CloudException {
        VmStatistics statistics = new VmStatistics();

        if( endTimestamp < 1L ) {
            endTimestamp = System.currentTimeMillis() + 1000L;
        }
        if( startTimestamp > (endTimestamp - (2L * CalendarWrapper.MINUTE)) ) {
            startTimestamp = endTimestamp - (2L * CalendarWrapper.MINUTE);
        }
        else if( startTimestamp < (System.currentTimeMillis() - (2L * CalendarWrapper.DAY)) ) {
            startTimestamp = System.currentTimeMillis() - (2L * CalendarWrapper.DAY);
        }

        calculateCpuUtilization(statistics, instanceId, startTimestamp, endTimestamp);
        calculateDiskReadBytes(statistics, instanceId, startTimestamp, endTimestamp);
        calculateDiskReadOps(statistics, instanceId, startTimestamp, endTimestamp);
        calculateDiskWriteBytes(statistics, instanceId, startTimestamp, endTimestamp);
        calculateDiskWriteOps(statistics, instanceId, startTimestamp, endTimestamp);
        calculateNetworkIn(statistics, instanceId, startTimestamp, endTimestamp);
        calculateNetworkOut(statistics, instanceId, startTimestamp, endTimestamp);
        return statistics;
	}

    @Override
    public @Nonnull Iterable<VmStatistics> getVMStatisticsForPeriod(@Nonnull String instanceId, long startTimestamp, long endTimestamp) throws InternalException, CloudException {
        if( endTimestamp < 1L ) {
            endTimestamp = System.currentTimeMillis() + 1000L;
        }
        if( startTimestamp > (endTimestamp - (2L * CalendarWrapper.MINUTE)) ) {
            startTimestamp = endTimestamp - (2L * CalendarWrapper.MINUTE);
        }
        else if( startTimestamp < (System.currentTimeMillis() - CalendarWrapper.DAY) ) {
            startTimestamp = System.currentTimeMillis() - CalendarWrapper.DAY;
        }
        TreeMap<Integer,VmStatistics> statMap = new TreeMap<Integer,VmStatistics>();
        int minutes = (int)((endTimestamp-startTimestamp)/CalendarWrapper.MINUTE);

        for( int i =1; i<=minutes; i++ ) {
            statMap.put(i, new VmStatistics());
        }
        Set<Metric> metrics = calculate("CPUUtilization", "Percent", instanceId, startTimestamp, endTimestamp);
        for( Metric m : metrics ) {
            int minute = 1 + (int)((m.timestamp - startTimestamp)/CalendarWrapper.MINUTE);
            VmStatistics stats = statMap.get(minute);
            
            if( stats == null ) {
                stats = new VmStatistics();
                statMap.put(minute, stats);
            }
            stats.setAverageCpuUtilization(m.average);
            stats.setMaximumCpuUtilization(m.maximum);
            stats.setMinimumCpuUtilization(m.minimum);
            stats.setStartTimestamp(m.timestamp);
            stats.setEndTimestamp(m.timestamp);
            stats.setSamples(m.samples);
        }
        metrics = calculate("DiskReadBytes", "Bytes", instanceId, startTimestamp, endTimestamp);
        for( Metric m : metrics ) {
            int minute = 1 + (int)((m.timestamp - startTimestamp)/CalendarWrapper.MINUTE);
            VmStatistics stats = statMap.get(minute);
            
            if( stats == null ) {
                stats = new VmStatistics();
                statMap.put(minute, stats);
            }
            stats.setAverageDiskReadBytes(m.average);
            stats.setMinimumDiskReadBytes(m.minimum);
            stats.setMaximumDiskReadBytes(m.maximum);
            if( stats.getSamples() < 1 ) {
                stats.setSamples(m.samples);
            }
        }
        metrics = calculate("DiskReadOps", "Count", instanceId, startTimestamp, endTimestamp);
        for( Metric m : metrics ) {
            int minute = 1 + (int)((m.timestamp - startTimestamp)/CalendarWrapper.MINUTE);
            VmStatistics stats = statMap.get(minute);
            
            if( stats == null ) {
                stats = new VmStatistics();
                statMap.put(minute, stats);
            }
            stats.setAverageDiskReadOperations(m.average);
            stats.setMinimumDiskReadOperations(m.minimum);
            stats.setMaximumDiskReadOperations(m.maximum);
            if( stats.getSamples() < 1 ) {
                stats.setSamples(m.samples);
            }            
        }
        metrics = calculate("DiskWriteBytes", "Bytes", instanceId, startTimestamp, endTimestamp);
        for( Metric m : metrics ) {
            int minute = 1 + (int)((m.timestamp - startTimestamp)/CalendarWrapper.MINUTE);
            VmStatistics stats = statMap.get(minute);
            
            if( stats == null ) {
                stats = new VmStatistics();
                statMap.put(minute, stats);
            }
            stats.setAverageDiskWriteBytes(m.average);
            stats.setMinimumDiskWriteBytes(m.minimum);
            stats.setMaximumDiskWriteBytes(m.maximum);
            if( stats.getSamples() < 1 ) {
                stats.setSamples(m.samples);
            }
        }
        metrics = calculate("DiskWriteOps", "Count", instanceId, startTimestamp, endTimestamp);
        for( Metric m : metrics ) {
            int minute = 1 + (int)((m.timestamp - startTimestamp)/CalendarWrapper.MINUTE);
            VmStatistics stats = statMap.get(minute);
            
            if( stats == null ) {
                stats = new VmStatistics();
                statMap.put(minute, stats);
            }
            stats.setAverageDiskWriteOperations(m.average);
            stats.setMinimumDiskWriteOperations(m.minimum);
            stats.setMaximumDiskWriteOperations(m.maximum);
            if( stats.getSamples() < 1 ) {
                stats.setSamples(m.samples);
            }
        }
        metrics = calculate("NetworkIn", "Bytes", instanceId, startTimestamp, endTimestamp);
        for( Metric m : metrics ) {
            int minute = 1 + (int)((m.timestamp - startTimestamp)/CalendarWrapper.MINUTE);
            VmStatistics stats = statMap.get(minute);
            
            if( stats == null ) {
                stats = new VmStatistics();
                statMap.put(minute, stats);
            }
            stats.setAverageNetworkIn(m.average);
            stats.setMinimumNetworkIn(m.minimum);
            stats.setMaximumNetworkIn(m.maximum);
            if( stats.getSamples() < 1 ) {
                stats.setSamples(m.samples);
            }
        }
        metrics = calculate("NetworkOut", "Bytes", instanceId, startTimestamp, endTimestamp);
        for( Metric m : metrics ) {
            int minute = 1 + (int)((m.timestamp - startTimestamp)/CalendarWrapper.MINUTE);
            VmStatistics stats = statMap.get(minute);
            
            if( stats == null ) {
                stats = new VmStatistics();
                statMap.put(minute, stats);
            }
            stats.setAverageNetworkOut(m.average);
            stats.setMinimumNetworkOut(m.minimum);
            stats.setMaximumNetworkOut(m.maximum);
            if( stats.getSamples() < 1 ) {
                stats.setSamples(m.samples);
            }
        }
        ArrayList<VmStatistics> list = new ArrayList<VmStatistics>();
        for( Map.Entry<Integer,VmStatistics> entry : statMap.entrySet() ) {
            VmStatistics stats = entry.getValue();
            
            if( stats != null && stats.getSamples() > 0 ) {
                list.add(stats);
            }
        }
        return list;
    }
    
    @Override
    public boolean isSubscribed() throws InternalException, CloudException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_INSTANCES);
        EC2Method method = new EC2Method(provider, provider.getEc2Url(), parameters);
        
        try {
            method.invoke();
            return true;
        }
        catch( EC2Exception e ) {
            if( e.getStatus() == HttpServletResponse.SC_UNAUTHORIZED || e.getStatus() == HttpServletResponse.SC_FORBIDDEN ) {
                return false;
            }
            String code = e.getCode();
            
            if( code != null && code.equals("SignatureDoesNotMatch") ) {
                return false;
            }
            logger.warn(e.getSummary());
            throw new CloudException(e);
        }
    }
    
    private List<VirtualMachineProduct> get64s() {
        return sixtyFours;
    }
    
    
    private List<VirtualMachineProduct> get32s() {
        return thirtyTwos;
    }
    
	@Override
	public Iterable<VirtualMachineProduct> listProducts(Architecture architecture) throws InternalException, CloudException {
		if( architecture == null ) {
		    return Collections.emptyList();
		}
		switch( architecture ) {
		case I32: return get32s();
		case I64: return get64s();
		default: return Collections.emptyList();
		}
	}
	
	private String guess(String privateDnsAddress) {
	    String dnsAddress = privateDnsAddress;
	    String[] parts = dnsAddress.split("\\.");
	    
	    if( parts != null && parts.length > 1 ) {
	        dnsAddress = parts[0];
	    }
	    if( dnsAddress.startsWith("ip-") ) {
	        dnsAddress = dnsAddress.replace('-', '.');
            return dnsAddress.substring(3);
	    }
	    return null;
	}
	
	@Override
    public @Nonnull VirtualMachine launch(@Nonnull String imageId, @Nonnull VirtualMachineProduct size, @Nullable String inZoneId, @Nonnull String name, @Nullable String description, @Nullable String keypair, @Nullable String inVlanId, boolean withMonitoring, boolean asImageSandbox, @Nullable String ... protectedByFirewalls) throws CloudException, InternalException {
	    return launch(imageId, size, inZoneId, name, description, keypair, inVlanId, withMonitoring, asImageSandbox, protectedByFirewalls, new Tag[0]);
	}

	@Override
	public @Nonnull VirtualMachine launch(@Nonnull String imageId, @Nonnull VirtualMachineProduct size, @Nullable String inZoneId, @Nonnull String name, @Nullable String description, @Nullable String keypair, @Nullable String inVlanId, boolean withMonitoring, boolean asImageSandbox, @Nullable String[] protectedByFirewalls, @Nullable Tag ... tags) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was established for this request");
        }
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.RUN_INSTANCES);
		EC2Method method;
        NodeList blocks;
		Document doc;
        
        parameters.put("ImageId", imageId);
        parameters.put("MinCount", "1");
        parameters.put("MaxCount", "1");
        parameters.put("InstanceType", size.getProductId());
        if( protectedByFirewalls != null && protectedByFirewalls.length > 0 ) {
        	int i = 1;
        	
        	for( String id : protectedByFirewalls ) {
        		parameters.put("SecurityGroupId." + (i++), id);
        	}
        }
        if( inZoneId != null ) {
        	parameters.put("Placement.AvailabilityZone", inZoneId);
        }
        if( keypair != null ) {
        	parameters.put("KeyName", keypair);
        }
        if( inVlanId != null ) {
        	parameters.put("SubnetId", inVlanId);
        }
        if( provider.isAmazon() ) {
            parameters.put("Monitoring.Enabled", String.valueOf(withMonitoring));
        }
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
        	doc = method.invoke();
        }
        catch( EC2Exception e ) {
            String code = e.getCode();
            
            if( code != null && code.equals("InsufficientInstanceCapacity") ) {
                return null;
            }
        	logger.error(e.getSummary());
        	throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("instancesSet");
        VirtualMachine server = null;
        for( int i=0; i<blocks.getLength(); i++ ) {
        	NodeList instances = blocks.item(i).getChildNodes();
        	
            for( int j=0; j<instances.getLength(); j++ ) {
            	Node instance = instances.item(j);
            	
            	if( instance.getNodeName().equals("item") ) {
            		server = toVirtualMachine(ctx, instance);
            		if( server != null ) {
            			break;
            		}
            	}
            }
        }
        if( server != null && keypair != null ) {
        	try {
                final String sid = server.getProviderVirtualMachineId();
                
        	    Callable<String> pwMethod = new Callable<String>() {
        	        public String call() throws CloudException {
        	            try {
        	                Map<String,String> params = provider.getStandardParameters(provider.getContext(), EC2Method.GET_PASSWORD_DATA);
        	                EC2Method m;
        	                
        	                params.put("InstanceId", sid);
        	                m = new EC2Method(provider, provider.getEc2Url(), params);
        	                
        	                Document doc = m.invoke();
        	                NodeList blocks = doc.getElementsByTagName("passwordData");
        	                
        	                if( blocks.getLength() > 0 ) {
        	                    Node pw = blocks.item(0);

        	                    if( pw.hasChildNodes() ) {
        	                        String password = pw.getFirstChild().getNodeValue();
        	                    
        	                        provider.release();
        	                        return password;
        	                    }
        	                    return null;
        	                }
        	                return null;
        	            }
        	            catch( Throwable t ) {
        	                throw new CloudException("Unable to retrieve password for " + sid + ", Let's hope it's Unix: " + t.getMessage());                       	                
        	            }
        	        }
        	    };
                
        	    provider.hold();
        	    try {
        	        String password = pwMethod.call();
        	    
                    if( password == null ) {
                        server.setRootPassword(null);
                        server.setPasswordCallback(pwMethod);
                    }
                    else {
                        server.setRootPassword(password);
                    }
                    server.setPlatform(Platform.WINDOWS);
        	    }
        	    catch( CloudException e ) {
        	        logger.warn(e.getMessage());
        	    }
        	}
        	catch( Throwable t ) {
                logger.warn("Unable to retrieve password for " + server.getProviderVirtualMachineId() + ", Let's hope it's Unix: " + t.getMessage());               
        	}
        }
        Tag[] toCreate;
        int i = 0;
        
        if( tags == null ) {
            toCreate = new Tag[2];
        }
        else {
            int count = 0;
            
            for( Tag t : tags ) {
                if( t.getKey().equalsIgnoreCase("name") || t.getKey().equalsIgnoreCase("description") ) {
                    continue;
                }
                count++;
            }
            toCreate = new Tag[count + 2];
            for( Tag t : tags ) {
                if( t.getKey().equalsIgnoreCase("name") || t.getKey().equalsIgnoreCase("description") ) {
                    continue;
                }
                toCreate[i++] = t;
            }
        }
        Tag t = new Tag();
        
        t.setKey("Name");
        t.setValue(name);
        toCreate[i++] = t;
        t = new Tag();
        t.setKey("Description");
        t.setValue(description);
        toCreate[i] = t;
        provider.createTags(server.getProviderVirtualMachineId(), toCreate);
        return server;
	}

	@Override
	public @Nonnull Iterable<VirtualMachine> listVirtualMachines() throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was established for this request");
        }
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_INSTANCES);
		EC2Method method = new EC2Method(provider, provider.getEc2Url(), parameters);
		ArrayList<VirtualMachine> list = new ArrayList<VirtualMachine>();
        NodeList blocks;
		Document doc;
        
        try {
        	doc = method.invoke();
        }
        catch( EC2Exception e ) {
        	logger.error(e.getSummary());
        	throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("instancesSet");
        for( int i=0; i<blocks.getLength(); i++ ) {
        	NodeList instances = blocks.item(i).getChildNodes();
        	
            for( int j=0; j<instances.getLength(); j++ ) {
            	Node instance = instances.item(j);
            	
            	if( instance.getNodeName().equals("item") ) {
            		list.add(toVirtualMachine(ctx, instance));
            	}
            }
        }
        return list;
	}

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        if( action.equals(VirtualMachineSupport.ANY) ) {
            return new String[] { EC2Method.EC2_PREFIX + "*" };
        }
        else if( action.equals(VirtualMachineSupport.BOOT) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.START_INSTANCES };
        }
        else if( action.equals(VirtualMachineSupport.CLONE) ) {
            return new String[0];
        }
        else if( action.equals(VirtualMachineSupport.CREATE_VM) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.RUN_INSTANCES };
        }
        else if( action.equals(VirtualMachineSupport.GET_VM) || action.equals(VirtualMachineSupport.LIST_VM) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_INSTANCES };
        }
        else if( action.equals(VirtualMachineSupport.PAUSE) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.STOP_INSTANCES };
        }
        else if( action.equals(VirtualMachineSupport.REBOOT) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.REBOOT_INSTANCES };
        }
        else if( action.equals(VirtualMachineSupport.REMOVE_VM) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.TERMINATE_INSTANCES };
        }
        else if( action.equals(VirtualMachineSupport.TOGGLE_ANALYTICS) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.MONITOR_INSTANCES };
        }
        else if( action.equals(VirtualMachineSupport.VIEW_ANALYTICS) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.GET_METRIC_STATISTICS };
        }
        else if( action.equals(VirtualMachineSupport.VIEW_CONSOLE) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.GET_CONSOLE_OUTPUT };
        }
        return new String[0];
    }
    
	@Override
	public void pause(@Nonnull String instanceId) throws InternalException, CloudException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.STOP_INSTANCES);
        EC2Method method;
        
        parameters.put("InstanceId.1", instanceId);
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
            method.invoke();
        }
        catch( EC2Exception e ) {
            logger.error(e.getSummary());
            throw new CloudException(e);
        }	    
	}

	@Override
	public void reboot(@Nonnull String instanceId) throws CloudException, InternalException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.REBOOT_INSTANCES);
		EC2Method method;
        
        parameters.put("InstanceId.1", instanceId);
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
        	method.invoke();
        }
        catch( EC2Exception e ) {
        	logger.error(e.getSummary());
        	throw new CloudException(e);
        }
	}

	private String resolve(String dnsName) {
        if( dnsName != null && dnsName.length() > 0 ) {
            InetAddress[] addresses;
            
            try {
                addresses = InetAddress.getAllByName(dnsName);
            }
            catch( UnknownHostException e ) {
                addresses = null;
            }
            if( addresses != null && addresses.length > 0 ) {
                dnsName = addresses[0].getHostAddress();
            }
            else {
                dnsName = dnsName.split("\\.")[0];
                dnsName = dnsName.replaceAll("-", "\\.");
                dnsName = dnsName.substring(4);
            }
        }        
        return dnsName;
	}

    @Override
    public boolean supportsAnalytics() throws CloudException, InternalException {
        return true;
    }
    
	@Override
	public void terminate(@Nonnull String instanceId) throws InternalException, CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.TERMINATE_INSTANCES);
		EC2Method method;
        
        parameters.put("InstanceId.1", instanceId);
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
        	method.invoke();
        }
        catch( EC2Exception e ) {
        	logger.error(e.getSummary());
        	throw new CloudException(e);
        }
	}

	private @Nullable VirtualMachine toVirtualMachine(@Nonnull ProviderContext ctx, @Nullable Node instance) throws CloudException {
        if( instance == null ) {
            return null;
        }
		NodeList attrs = instance.getChildNodes();
		VirtualMachine server = new VirtualMachine();

		server.setPersistent(false);
		server.setProviderOwnerId(ctx.getAccountNumber());
		server.setCurrentState(VmState.PENDING);
		server.setName(null);
		server.setDescription(null);
		for( int i=0; i<attrs.getLength(); i++ ) {
			Node attr = attrs.item(i);
			String name;
			
			name = attr.getNodeName();
			if( name.equals("instanceId") ) {
				String value = attr.getFirstChild().getNodeValue().trim();
				
				server.setProviderVirtualMachineId(value);
			}
			else if( name.equals("architecture") ) {
			    String value = attr.getFirstChild().getNodeValue().trim();
			    Architecture architecture;
			    
			    if( value.equalsIgnoreCase("i386") ) {
			        architecture = Architecture.I32;
			    }
			    else {
			        architecture = Architecture.I64;
			    }
			    server.setArchitecture(architecture);
			}
			else if( name.equals("imageId") ) {
				String value = attr.getFirstChild().getNodeValue().trim();
				
				server.setProviderMachineImageId(value);
			}
			else if( name.equalsIgnoreCase("subnetId") ) {
                server.setProviderSubnetId(attr.getFirstChild().getNodeValue().trim());
			}
			else if( name.equalsIgnoreCase("vpcId") ) {
                server.setProviderVlanId(attr.getFirstChild().getNodeValue().trim());			    
			}
			else if( name.equals("instanceState") ) {
				NodeList details = attr.getChildNodes();
				
				for( int j=0; j<details.getLength(); j++ ) {
					Node detail = details.item(j);
					
					name = detail.getNodeName();
					if( name.equals("name") ) {
						String value = detail.getFirstChild().getNodeValue().trim();
						
						server.setCurrentState(getServerState(value));
					}
				}
			}
			else if( name.equals("privateDnsName") ) {
				if( attr.hasChildNodes() ) {
					String value = attr.getFirstChild().getNodeValue();
				
					server.setPrivateDnsAddress(value);
                    if( server.getPrivateIpAddresses() == null || server.getPrivateIpAddresses().length < 1 ) {
					    value = guess(value);
					    if( value != null ) {
					        server.setPrivateIpAddresses(new String[] { value });
					    }
					    else {
					        server.setPrivateIpAddresses(new String[0]);
					    }
					}
				}
			}
			else if( name.equals("dnsName") ) {
                if( attr.hasChildNodes() ) {
                    String value = attr.getFirstChild().getNodeValue();

                    server.setPublicDnsAddress(value);
					if( server.getPublicIpAddresses() == null || server.getPublicIpAddresses().length < 1 ) {
					    server.setPublicIpAddresses(new String[] { resolve(value) });
					}
				}
			}
            else if( name.equals("privateIpAddress") ) {
                if( attr.hasChildNodes() ) {
                    String value = attr.getFirstChild().getNodeValue();

                    server.setPrivateIpAddresses(new String[] { value });
                }
            }
            else if( name.equals("ipAddress") ) {
                if( attr.hasChildNodes() ) {
                    String value = attr.getFirstChild().getNodeValue();

                    server.setPublicIpAddresses(new String[] { value });
                }
            }
            else if( name.equals("rootDeviceType") ) {
                if( attr.hasChildNodes() ) {
                    server.setPersistent(attr.getFirstChild().getNodeValue().equalsIgnoreCase("ebs"));
                }                
            }
            else if( name.equals("tagSet") ) {
                if( attr.hasChildNodes() ) {
                    NodeList tags = attr.getChildNodes();
                    
                    for( int j=0; j<tags.getLength(); j++ ) {
                        Node tag = tags.item(j);
                        
                        if( tag.getNodeName().equals("item") && tag.hasChildNodes() ) {
                            NodeList parts = tag.getChildNodes();
                            String key = null, value = null;
                            
                            for( int k=0; k<parts.getLength(); k++ ) {
                                Node part = parts.item(k);
                                
                                if( part.getNodeName().equalsIgnoreCase("key") ) {
                                    if( part.hasChildNodes() ) {
                                        key = part.getFirstChild().getNodeValue().trim();
                                    }
                                }
                                else if( part.getNodeName().equalsIgnoreCase("value") ) {
                                    if( part.hasChildNodes() ) {
                                        value = part.getFirstChild().getNodeValue().trim();
                                    }
                                }
                            }
                            if( key != null ) {
                                if( key.equalsIgnoreCase("name") ) {
                                    server.setName(value);
                                }
                                else if( key.equalsIgnoreCase("description") ) {
                                    server.setDescription(value);
                                }
                                else {
                                    server.addTag(key, value);
                                }
                            }
                        }
                    }
                }
            }
			else if( name.equals("instanceType") ) {
				String value = attr.getFirstChild().getNodeValue().trim();
				VirtualMachineProduct product = getProduct(value);
				
				server.setProduct(product);
			}
			else if( name.equals("launchTime") ) {
				SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
				String value = attr.getFirstChild().getNodeValue().trim();

				try {
					server.setLastBootTimestamp(fmt.parse(value).getTime());
					server.setCreationTimestamp(server.getLastBootTimestamp());
				} 
				catch( ParseException e ) {
					logger.error(e);
					e.printStackTrace();
					throw new CloudException(e);
				}
			}
			else if( name.equals("platform") ) {
			    if( attr.hasChildNodes() ) {
			        server.setPlatform(Platform.guess(attr.getFirstChild().getNodeValue()));
			    }
			}
			else if( name.equals("placement") ) {
				NodeList details = attr.getChildNodes();
				
				for( int j=0; j<details.getLength(); j++ ) {
					Node detail = details.item(j);
					
					name = detail.getNodeName();
					if( name.equals("availabilityZone") ) {
					    if( detail.hasChildNodes() ) {
					        String value = detail.getFirstChild().getNodeValue().trim();
						
					        server.setProviderDataCenterId(value);
					    }
					}
				}
			}
		}
		if( server.getPlatform() == null ) {
		    server.setPlatform(Platform.UNKNOWN);
		}
        server.setProviderRegionId(ctx.getRegionId());
        if( server.getName() == null ) {
            server.setName(server.getProviderVirtualMachineId());
        }
        if( server.getDescription() == null ) {
            server.setDescription(server.getName() + " (" + server.getProduct().getName() + ")");
        }
        if( server.getArchitecture() == null && server.getProduct() != null ) {
            server.setArchitecture(getArchitecture(server.getProduct().getProductId()));
        }
        else if( server.getArchitecture() == null ) {
            server.setArchitecture(Architecture.I64);
        }
		return server;
	}
	
	@Override
	public void disableAnalytics(String instanceId) throws InternalException, CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.UNMONITOR_INSTANCES);
		EC2Method method;
        
        parameters.put("InstanceId.1", instanceId);
        method = new EC2Method(provider, provider.getEc2Url(), parameters);
        try {
        	method.invoke();
        }
        catch( EC2Exception e ) {
        	logger.error(e.getSummary());
        	throw new CloudException(e);
        }
	}
}
