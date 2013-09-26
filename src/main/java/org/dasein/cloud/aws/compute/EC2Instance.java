/**
 * Copyright (C) 2009-2013 Dell, Inc.
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

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.compute.*;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.*;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Megabyte;
import org.dasein.util.uom.storage.Storage;
import org.dasein.util.uom.time.Day;
import org.dasein.util.uom.time.TimePeriod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;

public class EC2Instance extends AbstractVMSupport<AWSCloud> {
  static private final Logger logger = Logger.getLogger(EC2Instance.class);
  static private final Calendar UTC_CALENDAR = Calendar.getInstance(new SimpleTimeZone(0, "GMT"));

  EC2Instance(AWSCloud provider) {
    super(provider);
  }

  @Override
  public VirtualMachine alterVirtualMachine(@Nonnull String vmId, @Nonnull VMScalingOptions options) throws InternalException, CloudException {
    throw new OperationNotSupportedException("AWS does not support vertical scaling of instances");
  }

  @Override
  public void start(@Nonnull String instanceId) throws InternalException, CloudException {
    APITrace.begin(getProvider(), "startVM");
    try {
      VirtualMachine vm = getVirtualMachine(instanceId);

      if (vm == null) {
        throw new CloudException("No such instance: " + instanceId);
      }
      if (!vm.isPersistent()) {
        throw new OperationNotSupportedException("Instances backed by ephemeral drives are not start/stop capable");
      }
      Map<String, String> parameters = getProvider().getStandardParameters(getProvider().getContext(), EC2Method.START_INSTANCES);
      EC2Method method;

      parameters.put("InstanceId.1", instanceId);
      method = new EC2Method(getProvider(), getProvider().getEc2Url(), parameters);
      try {
        method.invoke();
      } catch (EC2Exception e) {
        logger.error(e.getSummary());
        throw new CloudException(e);
      }
    } finally {
      APITrace.end();
    }
  }

  static private class Metric implements Comparable<Metric> {
    int samples = 0;
    long timestamp = -1L;
    double minimum = -1.0;
    double maximum = 0.0;
    double average = 0.0;

    public int compareTo(Metric other) {
      if (other == this) {
        return 0;
      }
      return (new Long(timestamp)).compareTo(other.timestamp);
    }
  }

  private Set<Metric> calculate(String metric, String unit, String instanceId, long startTimestamp, long endTimestamp) throws CloudException, InternalException {
    APITrace.begin(getProvider(), "calculateVMAnalytics");
    try {
      if (!getProvider().getEC2Provider().isAWS()) {
        return new TreeSet<Metric>();
      }
      Map<String, String> parameters = getProvider().getStandardCloudWatchParameters(getContext(), EC2Method.GET_METRIC_STATISTICS);
      SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
      fmt.setCalendar(UTC_CALENDAR);
      EC2Method method;
      NodeList blocks;
      Document doc;

      parameters.put("EndTime", fmt.format(new Date(endTimestamp)));
      parameters.put("StartTime", fmt.format(new Date(startTimestamp)));
      parameters.put("MetricName", metric);
      parameters.put("Namespace", "AWS/EC2");
      parameters.put("Unit", unit);
      parameters.put("Dimensions.member.Name.1", "InstanceId");
      parameters.put("Dimensions.member.Value.1", instanceId);
      parameters.put("Statistics.member.1", "Average");
      parameters.put("Statistics.member.2", "Minimum");
      parameters.put("Statistics.member.3", "Maximum");
      parameters.put("Period", "60");
      method = new EC2Method(getProvider(), getCloudWatchUrl(getContext()), parameters);
      try {
        doc = method.invoke();
      } catch (EC2Exception e) {
        logger.error(e.getSummary());
        throw new CloudException(e);
      }
      TreeSet<Metric> metrics = new TreeSet<Metric>();
      fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
      fmt.setCalendar(UTC_CALENDAR);
      blocks = doc.getElementsByTagName("member");
      for (int i = 0; i < blocks.getLength(); i++) {
        NodeList items = blocks.item(i).getChildNodes();
        Metric m = new Metric();

        for (int j = 0; j < items.getLength(); j++) {
          Node item = items.item(j);

          if (item.getNodeName().equals("Timestamp")) {
            String dateString = item.getFirstChild().getNodeValue();

            try {
              m.timestamp = fmt.parse(dateString).getTime();
            } catch (ParseException e) {
              logger.error(e);
              e.printStackTrace();
              throw new InternalException(e);
            }
          } else if (item.getNodeName().equals("Average")) {
            m.average = Double.parseDouble(item.getFirstChild().getNodeValue());
          } else if (item.getNodeName().equals("Minimum")) {
            m.minimum = Double.parseDouble(item.getFirstChild().getNodeValue());
          } else if (item.getNodeName().equals("Maximum")) {
            m.maximum = Double.parseDouble(item.getFirstChild().getNodeValue());
          } else if (item.getNodeName().equals("Samples")) {
            m.samples = (int) Double.parseDouble(item.getFirstChild().getNodeValue());
          }
        }
        metrics.add(m);
      }
      return metrics;
    } finally {
      APITrace.end();
    }
  }

  private interface ApplyCalcs {
    public void apply(VmStatistics stats, long start, long end, int samples, double average, double minimum, double maximum);
  }

  private void calculate(VmStatistics stats, String metricName, String unit, String instanceId, long startTimestamp, long endTimestamp, ApplyCalcs apply) throws CloudException, InternalException {
    Set<Metric> metrics = calculate(metricName, unit, instanceId, startTimestamp, endTimestamp);
    double minimum = -1.0, maximum = 0.0, sum = 0.0;
    long start = -1L, end = 0L;
    int samples = 0;

    for (Metric metric : metrics) {
      if (start < 0L) {
        start = metric.timestamp;
      }
      if (metric.timestamp > end) {
        end = metric.timestamp;
      }
      samples++;
      if (metric.minimum < minimum || minimum < 0.0) {
        minimum = metric.minimum;
      }
      if (metric.maximum > maximum) {
        maximum = metric.maximum;
      }
      sum += metric.average;
    }
    if (start < 0L) {
      start = startTimestamp;
    }
    if (end < 0L) {
      end = endTimestamp;
      if (end < 1L) {
        end = System.currentTimeMillis();
      }
    }
    if (minimum < 0.0) {
      minimum = 0.0;
    }
    apply.apply(stats, start, end, samples, samples == 0 ? 0.0 : sum / samples, minimum, maximum);
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
  public
  @Nonnull
  VirtualMachine clone(@Nonnull String vmId, @Nonnull String intoDcId, @Nonnull String name, @Nonnull String description, boolean powerOn, @Nullable String... firewallIds) throws InternalException, CloudException {
    throw new OperationNotSupportedException("AWS instances cannot be cloned.");
  }

  @Override
  public
  @Nullable
  VMScalingCapabilities describeVerticalScalingCapabilities() throws CloudException, InternalException {
    return null;
  }

  @Override
  public void enableAnalytics(@Nonnull String instanceId) throws InternalException, CloudException {
    APITrace.begin(getProvider(), "enableVMAnalytics");
    try {
      if (getProvider().getEC2Provider().isAWS() || getProvider().getEC2Provider().isEnStratus()) {
        Map<String, String> parameters = getProvider().getStandardParameters(getProvider().getContext(), EC2Method.MONITOR_INSTANCES);
        EC2Method method;

        parameters.put("InstanceId.1", instanceId);
        method = new EC2Method(getProvider(), getProvider().getEc2Url(), parameters);
        try {
          method.invoke();
        } catch (EC2Exception e) {
          logger.error(e.getSummary());
          throw new CloudException(e);
        }
      }
    } finally {
      APITrace.end();
    }
  }

  private Architecture getArchitecture(String size) {
    if (size.equals("m1.small") || size.equals("c1.medium")) {
      return Architecture.I32;
    } else {
      return Architecture.I64;
    }
  }

  private
  @Nonnull
  String getCloudWatchUrl(@Nonnull ProviderContext ctx) {
    return ("https://monitoring." + ctx.getRegionId() + ".amazonaws.com");
  }

  @Override
  public
  @Nullable
  String getPassword(@Nonnull String instanceId) throws InternalException, CloudException {
    APITrace.begin(getProvider(), "getPassword");
    try {
      Callable<String> callable = new GetPassCallable(
        instanceId,
        getProvider().getStandardParameters(getProvider().getContext(), EC2Method.GET_PASSWORD_DATA),
        getProvider(),
        getProvider().getEc2Url()
      );
      return callable.call();
    } catch (EC2Exception e) {
      logger.error(e.getSummary());
      throw new CloudException(e);
    } catch (CloudException ce) {
      ce.printStackTrace();
      throw ce;
    } catch (Exception e) {
      e.printStackTrace();
      throw new InternalException(e);
    } finally {
      APITrace.end();
    }
  }

  public static class GetPassCallable implements Callable {
    private String instanceId;
    private Map<String, String> params;
    private AWSCloud awsProvider;
    private String ec2url;

    public GetPassCallable( String iId, Map<String, String> p, AWSCloud ap, String eUrl ) {
      instanceId = iId;
      params = p;
      awsProvider = ap;
      ec2url = eUrl;
    }

    public String call() throws CloudException {
      EC2Method method;
      NodeList blocks;
      Document doc;

      params.put("InstanceId", instanceId);
      try {
        method = new EC2Method(awsProvider, ec2url, params);
      } catch(InternalException e) {
        logger.error(e.getMessage());
        throw new CloudException(e);
      }
      try {
        doc = method.invoke();
      } catch (EC2Exception e) {
        logger.error(e.getSummary());
        throw new CloudException(e);
      } catch(InternalException e) {
        logger.error(e.getMessage());
        throw new CloudException(e);
      }
      blocks = doc.getElementsByTagName("passwordData");

      if (blocks.getLength() > 0) {
        Node pw = blocks.item(0);

        if (pw.hasChildNodes()) {
          return pw.getFirstChild().getNodeValue();
        }
        return null;
      }
      return null;
    }
  }

  @Override
  public
  @Nonnull
  String getConsoleOutput(@Nonnull String instanceId) throws InternalException, CloudException {
    APITrace.begin(getProvider(), "getConsoleOutput");
    try {
      Map<String, String> parameters = getProvider().getStandardParameters(getProvider().getContext(), EC2Method.GET_CONSOLE_OUTPUT);
      String output = null;
      EC2Method method;
      NodeList blocks;
      Document doc;

      parameters.put("InstanceId", instanceId);
      method = new EC2Method(getProvider(), getProvider().getEc2Url(), parameters);
      try {
        doc = method.invoke();
      } catch (EC2Exception e) {
        String code = e.getCode();

        if (code != null && code.startsWith("InvalidInstanceID")) {
          return "";
        }
        logger.error(e.getSummary());
        throw new CloudException(e);
      }
      blocks = doc.getElementsByTagName("timestamp");
      for (int i = 0; i < blocks.getLength(); i++) {
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        fmt.setCalendar(UTC_CALENDAR);
        String ts = blocks.item(i).getFirstChild().getNodeValue();
        long timestamp;

        try {
          timestamp = fmt.parse(ts).getTime();
        } catch (ParseException e) {
          logger.error(e);
          e.printStackTrace();
          throw new CloudException(e);
        }
        if (timestamp > -1L) {
          break;
        }
      }
      blocks = doc.getElementsByTagName("output");
      for (int i = 0; i < blocks.getLength(); i++) {
        Node item = blocks.item(i);

        if (item.hasChildNodes()) {
          output = item.getFirstChild().getNodeValue().trim();
          if (output != null) {
            break;
          }
        }
      }
      if (output != null) {
        try {
          return new String(Base64.decodeBase64(output.getBytes("utf-8")), "utf-8");
        } catch (UnsupportedEncodingException e) {
          logger.error(e);
          e.printStackTrace();
          throw new InternalException(e);
        }
      }
      return "";
    } finally {
      APITrace.end();
    }
  }

  @Override
  public int getCostFactor(@Nonnull VmState vmState) throws InternalException, CloudException {
    return (vmState.equals(VmState.STOPPED) ? 0 : 100);
  }

  @Override
  public int getMaximumVirtualMachineCount() throws CloudException, InternalException {
    return -2;
  }

  @Override
  public
  @Nonnull
  Iterable<String> listFirewalls(@Nonnull String instanceId) throws InternalException, CloudException {
    APITrace.begin(getProvider(), "listFirewallsForVM");
    try {
      Map<String, String> parameters = getProvider().getStandardParameters(getProvider().getContext(), EC2Method.DESCRIBE_INSTANCES);
      ArrayList<String> firewalls = new ArrayList<String>();
      EC2Method method;
      NodeList blocks;
      Document doc;

      parameters.put("InstanceId.1", instanceId);
      method = new EC2Method(getProvider(), getProvider().getEc2Url(), parameters);
      try {
        doc = method.invoke();
      } catch (EC2Exception e) {
        String code = e.getCode();

        if (code != null && code.startsWith("InvalidInstanceID")) {
          return firewalls;
        }
        logger.error(e.getSummary());
        throw new CloudException(e);
      }
      blocks = doc.getElementsByTagName("groupSet");
      for (int i = 0; i < blocks.getLength(); i++) {
        NodeList items = blocks.item(i).getChildNodes();

        for (int j = 0; j < items.getLength(); j++) {
          Node item = items.item(j);

          if (item.getNodeName().equals("item")) {
            NodeList sub = item.getChildNodes();

            for (int k = 0; k < sub.getLength(); k++) {
              Node id = sub.item(k);

              if (id.getNodeName().equalsIgnoreCase("groupId") && id.hasChildNodes()) {
                firewalls.add(id.getFirstChild().getNodeValue().trim());
                break;
              }
            }
          }
        }
      }
      return firewalls;
    } finally {
      APITrace.end();
    }
  }

  @Override
  public
  @Nonnull
  String getProviderTermForServer(@Nonnull Locale locale) {
    return "instance";
  }

  @Override
  public
  @Nullable
  VirtualMachine getVirtualMachine(@Nonnull String instanceId) throws InternalException, CloudException {
    APITrace.begin(getProvider(), "getVirtualMachine");
    try {
      ProviderContext ctx = getProvider().getContext();

      if (ctx == null) {
        throw new CloudException("No context was established for this request");
      }
      Map<String, String> parameters = getProvider().getStandardParameters(getProvider().getContext(), EC2Method.DESCRIBE_INSTANCES);
      EC2Method method;
      NodeList blocks;
      Document doc;

      parameters.put("InstanceId.1", instanceId);
      method = new EC2Method(getProvider(), getProvider().getEc2Url(), parameters);
      try {
        doc = method.invoke();
      } catch (EC2Exception e) {
        String code = e.getCode();

        if (code != null && code.startsWith("InvalidInstanceID")) {
          return null;
        }
        logger.error(e.getSummary());
        throw new CloudException(e);
      }
      blocks = doc.getElementsByTagName("instancesSet");
      for (int i = 0; i < blocks.getLength(); i++) {
        NodeList instances = blocks.item(i).getChildNodes();

        for (int j = 0; j < instances.getLength(); j++) {
          Node instance = instances.item(j);

          if (instance.getNodeName().equals("item")) {
            Iterable<IpAddress> addresses = Collections.emptyList();


            if (getProvider().hasNetworkServices()) {
              NetworkServices services = getProvider().getNetworkServices();

              if (services != null) {
                IpAddressSupport support = services.getIpAddressSupport();

                if (support != null) {
                  addresses = support.listIpPool(IPVersion.IPV4, false);
                }
              }
            }
            VirtualMachine server = toVirtualMachine(ctx, instance, addresses);

            if (server != null && server.getProviderVirtualMachineId().equals(instanceId)) {
              return server;
            }
          }
        }
      }
      return null;
    } finally {
      APITrace.end();
    }
  }

  @Override
  public
  @Nullable
  VirtualMachineProduct getProduct(@Nonnull String sizeId) throws CloudException, InternalException {
    for (Architecture a : listSupportedArchitectures()) {
      for (VirtualMachineProduct prd : listProducts(a)) {
        if (prd.getProviderProductId().equals(sizeId)) {
          return prd;
        }
      }
    }
    return null;
  }

  private VmState getServerState(String state) {
    if (state.equals("pending")) {
      return VmState.PENDING;
    } else if (state.equals("running")) {
      return VmState.RUNNING;
    } else if (state.equals("terminating") || state.equals("stopping")) {
      return VmState.STOPPING;
    } else if (state.equals("stopped")) {
      return VmState.STOPPED;
    } else if (state.equals("shutting-down")) {
      return VmState.STOPPING;
    } else if (state.equals("terminated")) {
      return VmState.TERMINATED;
    } else if (state.equals("rebooting")) {
      return VmState.REBOOTING;
    }
    logger.warn("Unknown server state: " + state);
    return VmState.PENDING;
  }

  @Override
  public
  @Nonnull
  VmStatistics getVMStatistics(@Nonnull String instanceId, @Nonnegative long startTimestamp, @Nonnegative long endTimestamp) throws InternalException, CloudException {
    APITrace.begin(getProvider(), "getVMStatistics");
    try {
      VmStatistics statistics = new VmStatistics();

      if (endTimestamp < 1L) {
        endTimestamp = System.currentTimeMillis() + 1000L;
      }
      if (startTimestamp > (endTimestamp - (2L * CalendarWrapper.MINUTE))) {
        startTimestamp = endTimestamp - (2L * CalendarWrapper.MINUTE);
      } else if (startTimestamp < (System.currentTimeMillis() - (2L * CalendarWrapper.DAY))) {
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
    } finally {
      APITrace.end();
    }
  }

  @Override
  public
  @Nonnull
  Iterable<VmStatistics> getVMStatisticsForPeriod(@Nonnull String instanceId, long startTimestamp, long endTimestamp) throws InternalException, CloudException {
    APITrace.begin(getProvider(), "getVMStatisticsForPeriod");
    try {
      if (endTimestamp < 1L) {
        endTimestamp = System.currentTimeMillis() + 1000L;
      }
      if (startTimestamp > (endTimestamp - (2L * CalendarWrapper.MINUTE))) {
        startTimestamp = endTimestamp - (2L * CalendarWrapper.MINUTE);
      } else if (startTimestamp < (System.currentTimeMillis() - CalendarWrapper.DAY)) {
        startTimestamp = System.currentTimeMillis() - CalendarWrapper.DAY;
      }
      TreeMap<Integer, VmStatistics> statMap = new TreeMap<Integer, VmStatistics>();
      int minutes = (int) ((endTimestamp - startTimestamp) / CalendarWrapper.MINUTE);

      for (int i = 1; i <= minutes; i++) {
        statMap.put(i, new VmStatistics());
      }
      Set<Metric> metrics = calculate("CPUUtilization", "Percent", instanceId, startTimestamp, endTimestamp);
      for (Metric m : metrics) {
        int minute = 1 + (int) ((m.timestamp - startTimestamp) / CalendarWrapper.MINUTE);
        VmStatistics stats = statMap.get(minute);

        if (stats == null) {
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
      for (Metric m : metrics) {
        int minute = 1 + (int) ((m.timestamp - startTimestamp) / CalendarWrapper.MINUTE);
        VmStatistics stats = statMap.get(minute);

        if (stats == null) {
          stats = new VmStatistics();
          statMap.put(minute, stats);
        }
        stats.setAverageDiskReadBytes(m.average);
        stats.setMinimumDiskReadBytes(m.minimum);
        stats.setMaximumDiskReadBytes(m.maximum);
        if (stats.getSamples() < 1) {
          stats.setSamples(m.samples);
        }
      }
      metrics = calculate("DiskReadOps", "Count", instanceId, startTimestamp, endTimestamp);
      for (Metric m : metrics) {
        int minute = 1 + (int) ((m.timestamp - startTimestamp) / CalendarWrapper.MINUTE);
        VmStatistics stats = statMap.get(minute);

        if (stats == null) {
          stats = new VmStatistics();
          statMap.put(minute, stats);
        }
        stats.setAverageDiskReadOperations(m.average);
        stats.setMinimumDiskReadOperations(m.minimum);
        stats.setMaximumDiskReadOperations(m.maximum);
        if (stats.getSamples() < 1) {
          stats.setSamples(m.samples);
        }
      }
      metrics = calculate("DiskWriteBytes", "Bytes", instanceId, startTimestamp, endTimestamp);
      for (Metric m : metrics) {
        int minute = 1 + (int) ((m.timestamp - startTimestamp) / CalendarWrapper.MINUTE);
        VmStatistics stats = statMap.get(minute);

        if (stats == null) {
          stats = new VmStatistics();
          statMap.put(minute, stats);
        }
        stats.setAverageDiskWriteBytes(m.average);
        stats.setMinimumDiskWriteBytes(m.minimum);
        stats.setMaximumDiskWriteBytes(m.maximum);
        if (stats.getSamples() < 1) {
          stats.setSamples(m.samples);
        }
      }
      metrics = calculate("DiskWriteOps", "Count", instanceId, startTimestamp, endTimestamp);
      for (Metric m : metrics) {
        int minute = 1 + (int) ((m.timestamp - startTimestamp) / CalendarWrapper.MINUTE);
        VmStatistics stats = statMap.get(minute);

        if (stats == null) {
          stats = new VmStatistics();
          statMap.put(minute, stats);
        }
        stats.setAverageDiskWriteOperations(m.average);
        stats.setMinimumDiskWriteOperations(m.minimum);
        stats.setMaximumDiskWriteOperations(m.maximum);
        if (stats.getSamples() < 1) {
          stats.setSamples(m.samples);
        }
      }
      metrics = calculate("NetworkIn", "Bytes", instanceId, startTimestamp, endTimestamp);
      for (Metric m : metrics) {
        int minute = 1 + (int) ((m.timestamp - startTimestamp) / CalendarWrapper.MINUTE);
        VmStatistics stats = statMap.get(minute);

        if (stats == null) {
          stats = new VmStatistics();
          statMap.put(minute, stats);
        }
        stats.setAverageNetworkIn(m.average);
        stats.setMinimumNetworkIn(m.minimum);
        stats.setMaximumNetworkIn(m.maximum);
        if (stats.getSamples() < 1) {
          stats.setSamples(m.samples);
        }
      }
      metrics = calculate("NetworkOut", "Bytes", instanceId, startTimestamp, endTimestamp);
      for (Metric m : metrics) {
        int minute = 1 + (int) ((m.timestamp - startTimestamp) / CalendarWrapper.MINUTE);
        VmStatistics stats = statMap.get(minute);

        if (stats == null) {
          stats = new VmStatistics();
          statMap.put(minute, stats);
        }
        stats.setAverageNetworkOut(m.average);
        stats.setMinimumNetworkOut(m.minimum);
        stats.setMaximumNetworkOut(m.maximum);
        if (stats.getSamples() < 1) {
          stats.setSamples(m.samples);
        }
      }
      ArrayList<VmStatistics> list = new ArrayList<VmStatistics>();
      for (Map.Entry<Integer, VmStatistics> entry : statMap.entrySet()) {
        VmStatistics stats = entry.getValue();

        if (stats != null && stats.getSamples() > 0) {
          list.add(stats);
        }
      }
      return list;
    } finally {
      APITrace.end();
    }
  }

  @Override
  public
  @Nonnull
  Requirement identifyImageRequirement(@Nonnull ImageClass cls) throws CloudException, InternalException {
    return (ImageClass.MACHINE.equals(cls) ? Requirement.REQUIRED : Requirement.OPTIONAL);
  }

  @Override
  public
  @Nonnull
  Requirement identifyPasswordRequirement(Platform platform) throws CloudException, InternalException {
    return Requirement.NONE;
  }

  @Override
  public
  @Nonnull
  Requirement identifyRootVolumeRequirement() {
    return Requirement.NONE;
  }

  @Override
  public
  @Nonnull
  Requirement identifyShellKeyRequirement(Platform platform) throws CloudException, InternalException {
    return Requirement.OPTIONAL;
  }

  @Override
  public
  @Nonnull
  Requirement identifyStaticIPRequirement() throws CloudException, InternalException {
    return Requirement.NONE; // TODO: make this optional and fake it during provisioning
  }

  @Override
  public
  @Nonnull
  Requirement identifyVlanRequirement() {
    return (getProvider().getEC2Provider().isEucalyptus() ? Requirement.NONE : Requirement.OPTIONAL);
  }

  @Override
  public boolean isAPITerminationPreventable() {
    return getProvider().getEC2Provider().isAWS();
  }

  @Override
  public boolean isBasicAnalyticsSupported() {
    return (getProvider().getEC2Provider().isAWS() || getProvider().getEC2Provider().isEnStratus());
  }

  @Override
  public boolean isExtendedAnalyticsSupported() {
    return (getProvider().getEC2Provider().isAWS() || getProvider().getEC2Provider().isEnStratus());
  }

  @Override
  public boolean isSubscribed() throws InternalException, CloudException {
    APITrace.begin(getProvider(), "isSubscribedVirtualMachine");
    try {
      Map<String, String> parameters = getProvider().getStandardParameters(getProvider().getContext(), EC2Method.DESCRIBE_INSTANCES);
      EC2Method method = new EC2Method(getProvider(), getProvider().getEc2Url(), parameters);

      try {
        method.invoke();
        return true;
      } catch (EC2Exception e) {
        if (e.getStatus() == HttpServletResponse.SC_UNAUTHORIZED || e.getStatus() == HttpServletResponse.SC_FORBIDDEN) {
          return false;
        }
        String code = e.getCode();

        if (code != null && code.equals("SignatureDoesNotMatch")) {
          return false;
        }
        logger.warn(e.getSummary());
        throw new CloudException(e);
      }
    } finally {
      APITrace.end();
    }
  }

  @Override
  public boolean isUserDataSupported() {
    return true;
  }

  @Override
  public
  @Nonnull
  Iterable<VirtualMachineProduct> listProducts(Architecture architecture) throws InternalException, CloudException {
    ProviderContext ctx = getProvider().getContext();

    if (ctx == null) {
      throw new CloudException("No context was set for this request");
    }
    Cache<VirtualMachineProduct> cache = Cache.getInstance(getProvider(), "products" + architecture.name(), VirtualMachineProduct.class, CacheLevel.REGION, new TimePeriod<Day>(1, TimePeriod.DAY));
    Iterable<VirtualMachineProduct> products = cache.get(ctx);

    if (products == null) {
      ArrayList<VirtualMachineProduct> list = new ArrayList<VirtualMachineProduct>();

      try {
        InputStream input = EC2Instance.class.getResourceAsStream("/org/dasein/cloud/aws/vmproducts.json");

        if (input != null) {
          BufferedReader reader = new BufferedReader(new InputStreamReader(input));
          StringBuilder json = new StringBuilder();
          String line;

          while ((line = reader.readLine()) != null) {
            json.append(line);
            json.append("\n");
          }
          JSONArray arr = new JSONArray(json.toString());
          JSONObject toCache = null;

          for (int i = 0; i < arr.length(); i++) {
            JSONObject productSet = arr.getJSONObject(i);
            String cloud, providerName;

            if (productSet.has("cloud")) {
              cloud = productSet.getString("cloud");
            } else {
              continue;
            }
            if (productSet.has("provider")) {
              providerName = productSet.getString("provider");
            } else {
              continue;
            }
            if (!productSet.has("products")) {
              continue;
            }
            if (toCache == null || (providerName.equals("AWS") && cloud.equals("AWS"))) {
              toCache = productSet;
            }
            if (providerName.equalsIgnoreCase(getProvider().getProviderName()) && cloud.equalsIgnoreCase(getProvider().getCloudName())) {
              toCache = productSet;
              break;
            }
          }
          if (toCache == null) {
            logger.warn("No products were defined");
            return Collections.emptyList();
          }
          JSONArray plist = toCache.getJSONArray("products");

          for (int i = 0; i < plist.length(); i++) {
            JSONObject product = plist.getJSONObject(i);
            boolean supported = false;

            if (product.has("architectures")) {
              JSONArray architectures = product.getJSONArray("architectures");

              for (int j = 0; j < architectures.length(); j++) {
                String a = architectures.getString(j);

                if (architecture.name().equals(a)) {
                  supported = true;
                  break;
                }
              }
            }
            if (!supported) {
              continue;
            }
            if (product.has("excludesRegions")) {
              JSONArray regions = product.getJSONArray("excludesRegions");

              for (int j = 0; j < regions.length(); j++) {
                String r = regions.getString(j);

                if (r.equals(ctx.getRegionId())) {
                  supported = false;
                  break;
                }
              }
            }
            if (!supported) {
              continue;
            }
            VirtualMachineProduct prd = toProduct(product);

            if (prd != null) {
              list.add(prd);
            }
          }

        } else {
          logger.warn("No standard products resource exists for /org/dasein/cloud/aws/vmproducts.json");
        }
        input = EC2Instance.class.getResourceAsStream("/org/dasein/cloud/aws/vmproducts-custom.json");
        if (input != null) {
          ArrayList<VirtualMachineProduct> customList = new ArrayList<VirtualMachineProduct>();
          TreeSet<String> discard = new TreeSet<String>();
          boolean discardAll = false;

          BufferedReader reader = new BufferedReader(new InputStreamReader(input));
          StringBuilder json = new StringBuilder();
          String line;

          while ((line = reader.readLine()) != null) {
            json.append(line);
            json.append("\n");
          }
          JSONArray arr = new JSONArray(json.toString());
          JSONObject toCache = null;

          for (int i = 0; i < arr.length(); i++) {
            JSONObject listing = arr.getJSONObject(i);
            String cloud, providerName, endpoint = null;

            if (listing.has("cloud")) {
              cloud = listing.getString("cloud");
            } else {
              continue;
            }
            if (listing.has("provider")) {
              providerName = listing.getString("provider");
            } else {
              continue;
            }
            if (listing.has("endpoint")) {
              endpoint = listing.getString("endpoint");
            }
            if (!cloud.equals(getProvider().getCloudName()) || !providerName.equals(getProvider().getProviderName())) {
              continue;
            }
            if (endpoint != null && endpoint.equals(ctx.getEndpoint())) {
              toCache = listing;
              break;
            }
            if (endpoint == null && toCache == null) {
              toCache = listing;
            }
          }
          if (toCache != null) {
            if (toCache.has("discardDefaults")) {
              discardAll = toCache.getBoolean("discardDefaults");
            }
            if (toCache.has("discard")) {
              JSONArray dlist = toCache.getJSONArray("discard");

              for (int i = 0; i < dlist.length(); i++) {
                discard.add(dlist.getString(i));
              }
            }
            if (toCache.has("products")) {
              JSONArray plist = toCache.getJSONArray("products");

              for (int i = 0; i < plist.length(); i++) {
                JSONObject product = plist.getJSONObject(i);
                boolean supported = false;

                if (product.has("architectures")) {
                  JSONArray architectures = product.getJSONArray("architectures");

                  for (int j = 0; j < architectures.length(); j++) {
                    String a = architectures.getString(j);

                    if (architecture.name().equals(a)) {
                      supported = true;
                      break;
                    }
                  }
                }
                if (!supported) {
                  continue;
                }
                if (product.has("excludesRegions")) {
                  JSONArray regions = product.getJSONArray("excludesRegions");

                  for (int j = 0; j < regions.length(); j++) {
                    String r = regions.getString(j);

                    if (r.equals(ctx.getRegionId())) {
                      supported = false;
                      break;
                    }
                  }
                }
                if (!supported) {
                  continue;
                }
                VirtualMachineProduct prd = toProduct(product);

                if (prd != null) {
                  customList.add(prd);
                }
              }
            }
            if (!discardAll) {
              for (VirtualMachineProduct product : list) {
                if (!discard.contains(product.getProviderProductId())) {
                  customList.add(product);
                }
              }
            }
            list = customList;
          }
        }
        products = list;
        cache.put(ctx, products);

      } catch (IOException e) {
        throw new InternalException(e);
      } catch (JSONException e) {
        throw new InternalException(e);
      }
    }
    return products;
  }

  static private volatile Collection<Architecture> architectures;

  @Override
  public Iterable<Architecture> listSupportedArchitectures() {
    if (architectures == null) {
      ArrayList<Architecture> list = new ArrayList<Architecture>();

      list.add(Architecture.I64);
      list.add(Architecture.I32);
      architectures = Collections.unmodifiableCollection(list);
    }
    return architectures;
  }

  private String guess(String privateDnsAddress) {
    String dnsAddress = privateDnsAddress;
    String[] parts = dnsAddress.split("\\.");

    if (parts != null && parts.length > 1) {
      dnsAddress = parts[0];
    }
    if (dnsAddress.startsWith("ip-")) {
      dnsAddress = dnsAddress.replace('-', '.');
      return dnsAddress.substring(3);
    }
    return null;
  }

  @Override
  public
  @Nonnull
  VirtualMachine launch(@Nonnull VMLaunchOptions cfg) throws CloudException, InternalException {
    APITrace.begin(getProvider(), "launchVM");
    try {
      ProviderContext ctx = getProvider().getContext();

      if (ctx == null) {
        throw new CloudException("No context was established for this request");
      }
      MachineImage img = getProvider().getComputeServices().getImageSupport().getMachineImage(cfg.getMachineImageId());

      if (img == null) {
        throw new InternalException("No such machine image: " + cfg.getMachineImageId());
      }
      Map<String, String> parameters = getProvider().getStandardParameters(getProvider().getContext(), EC2Method.RUN_INSTANCES);
      String ramdiskImage = (String) cfg.getMetaData().get("ramdiskImageId"), kernelImage = (String) cfg.getMetaData().get("kernelImageId");
      EC2Method method;
      NodeList blocks;
      Document doc;

      parameters.put("ImageId", cfg.getMachineImageId());
      parameters.put("MinCount", "1");
      parameters.put("MaxCount", "1");
      parameters.put("InstanceType", cfg.getStandardProductId());
      if (ramdiskImage != null) {
        parameters.put("ramdiskId", ramdiskImage);
      }
      if (kernelImage != null) {
        parameters.put("kernelId", kernelImage);
      }
      if (cfg.getUserData() != null) {
        try {
          parameters.put("UserData", Base64.encodeBase64String(cfg.getUserData().getBytes("utf-8")));
        } catch (UnsupportedEncodingException e) {
          throw new InternalException(e);
        }
      }
      if (cfg.isPreventApiTermination()) {
        parameters.put("DisableApiTermination", "true");
      }
      String[] ids = cfg.getFirewallIds();

      if (ids.length > 0) {
        int i = 1;

        for (String id : ids) {
          parameters.put("SecurityGroupId." + (i++), id);
        }
      }
      if (cfg.getDataCenterId() != null) {
        parameters.put("Placement.AvailabilityZone", cfg.getDataCenterId());
      } else if (cfg.getVolumes().length > 0) {
        String dc = null;

        for (VMLaunchOptions.VolumeAttachment a : cfg.getVolumes()) {
          if (a.volumeToCreate != null) {
            dc = a.volumeToCreate.getDataCenterId();
            if (dc != null) {
              break;
            }
          }
        }
        if (dc != null) {
          cfg.inDataCenter(dc);
        }
      }
      if (cfg.getBootstrapKey() != null) {
        parameters.put("KeyName", cfg.getBootstrapKey());
      }
      if (cfg.getVlanId() != null) {
        parameters.put("SubnetId", cfg.getVlanId());
      }
      if (cfg.getPrivateIp() != null) {
        parameters.put("PrivateIpAddress", cfg.getPrivateIp());
      }
      if (getProvider().getEC2Provider().isAWS()) {
        parameters.put("Monitoring.Enabled", String.valueOf(cfg.isExtendedAnalytics()));
      }
      final ArrayList<VMLaunchOptions.VolumeAttachment> existingVolumes = new ArrayList<VMLaunchOptions.VolumeAttachment>();
      TreeSet<String> deviceIds = new TreeSet<String>();

      if (cfg.isIoOptimized()) {
        parameters.put("EbsOptimized", "true");
      }

      if (cfg.getVolumes().length > 0) {
        Iterable<String> possibles = getProvider().getComputeServices().getVolumeSupport().listPossibleDeviceIds(img.getPlatform());
        int i = 1;

        for (VMLaunchOptions.VolumeAttachment a : cfg.getVolumes()) {
          if (a.deviceId != null) {
            deviceIds.add(a.deviceId);
          } else if (a.volumeToCreate != null && a.volumeToCreate.getDeviceId() != null) {
            deviceIds.add(a.volumeToCreate.getDeviceId());
            a.deviceId = a.volumeToCreate.getDeviceId();
          }
        }
        for (VMLaunchOptions.VolumeAttachment a : cfg.getVolumes()) {
          if (a.deviceId == null) {
            for (String id : possibles) {
              if (!deviceIds.contains(id)) {
                a.deviceId = id;
                deviceIds.add(id);
              }
            }
            if (a.deviceId == null) {
              throw new InternalException("Unable to identify a device ID for volume");
            }
          }
          if (a.existingVolumeId == null) {
            parameters.put("BlockDeviceMapping." + i + ".DeviceName", a.deviceId);

            VolumeProduct prd = getProvider().getComputeServices().getVolumeSupport().getVolumeProduct(a.volumeToCreate.getVolumeProductId());
            parameters.put("BlockDeviceMapping." + i + ".Ebs.VolumeType", prd.getProviderProductId());

            if (a.volumeToCreate.getIops() > 0) {
              parameters.put("BlockDeviceMapping." + i + ".Ebs.Iops", String.valueOf(a.volumeToCreate.getIops()));
            }

            if (a.volumeToCreate.getSnapshotId() != null) {
              parameters.put("BlockDeviceMapping." + i + ".Ebs.SnapshotId", a.volumeToCreate.getSnapshotId());
            } else {
              parameters.put("BlockDeviceMapping." + i + ".Ebs.VolumeSize", String.valueOf(a.volumeToCreate.getVolumeSize().getQuantity().intValue()));
            }
            i++;
          } else {
            existingVolumes.add(a);
          }
        }
      }
      VMLaunchOptions.NICConfig[] nics = cfg.getNetworkInterfaces();

      if (nics != null && nics.length > 0) {
        int i = 1;

        for (VMLaunchOptions.NICConfig c : nics) {
          parameters.put("NetworkInterface." + i + ".DeviceIndex", String.valueOf(i));
          if (c.nicId == null) {
            parameters.put("NetworkInterface." + i + ".SubnetId", c.nicToCreate.getSubnetId());
            parameters.put("NetworkInterface." + i + ".Description", c.nicToCreate.getDescription());
            if (c.nicToCreate.getIpAddress() != null) {
              parameters.put("NetworkInterface." + i + ".PrivateIpAddress", c.nicToCreate.getIpAddress());
            }
            if (c.nicToCreate.getFirewallIds().length > 0) {
              int j = 1;

              for (String id : c.nicToCreate.getFirewallIds()) {
                parameters.put("NetworkInterface." + i + ".SecurityGroupId." + j, id);
                j++;
              }
            }
          } else {
            parameters.put("NetworkInterface." + i + ".NetworkInterfaceId", c.nicId);
          }
          i++;
        }
      }
      method = new EC2Method(getProvider(), getProvider().getEc2Url(), parameters);
      try {
        doc = method.invoke();
      } catch (EC2Exception e) {
        String code = e.getCode();

        if (code != null && code.equals("InsufficientInstanceCapacity")) {
          return null;
        }
        logger.error(e.getSummary());
        throw new CloudException(e);
      }
      blocks = doc.getElementsByTagName("instancesSet");
      VirtualMachine server = null;
      for (int i = 0; i < blocks.getLength(); i++) {
        NodeList instances = blocks.item(i).getChildNodes();

        for (int j = 0; j < instances.getLength(); j++) {
          Node instance = instances.item(j);

          if (instance.getNodeName().equals("item")) {
            server = toVirtualMachine(ctx, instance, new ArrayList<IpAddress>() /* can't be an elastic IP */);
            if (server != null) {
              break;
            }
          }
        }
      }
      if (server != null) {
        // wait for EC2 to figure out the server exists
        VirtualMachine copy = getVirtualMachine(server.getProviderVirtualMachineId());

        if (copy == null) {
          long timeout = System.currentTimeMillis() + CalendarWrapper.MINUTE;

          while (timeout > System.currentTimeMillis()) {
            try {
              Thread.sleep(5000L);
            } catch (InterruptedException ignore) {
            }
            try {
              copy = getVirtualMachine(server.getProviderVirtualMachineId());
            } catch (Throwable ignore) {
            }
            if (copy != null) {
              break;
            }
          }
        }
      }
      if ( cfg.isIpForwardingAllowed() ) {
        enableIpForwarding( server.getProviderVirtualMachineId() );
      }
      if (server != null && cfg.getBootstrapKey() != null) {
        try {
          final String sid = server.getProviderVirtualMachineId();
          try {
            Callable<String> callable = new GetPassCallable(
              sid,
              getProvider().getStandardParameters(getProvider().getContext(), EC2Method.GET_PASSWORD_DATA),
              getProvider(),
              getProvider().getEc2Url()
            );
            String password = callable.call();

            if (password == null) {
              server.setRootPassword(null);
              server.setPasswordCallback(callable);
            } else {
              server.setRootPassword(password);
            }
            server.setPlatform(Platform.WINDOWS);
          } catch (CloudException e) {
            logger.warn(e.getMessage());
          }
        } catch (Throwable t) {
          logger.warn("Unable to retrieve password for " + server.getProviderVirtualMachineId() + ", Let's hope it's Unix: " + t.getMessage());
        }
      }
      Map<String, Object> meta = cfg.getMetaData();
      Tag[] toCreate;
      int i = 0;

      if (meta.isEmpty()) {
        toCreate = new Tag[2];
      } else {
        int count = 0;

        for (Map.Entry<String, Object> entry : meta.entrySet()) {
          if (entry.getKey().equalsIgnoreCase("name") || entry.getKey().equalsIgnoreCase("description")) {
            continue;
          }
          count++;
        }
        toCreate = new Tag[count + 2];
        for (Map.Entry<String, Object> entry : meta.entrySet()) {
          if (entry.getKey().equalsIgnoreCase("name") || entry.getKey().equalsIgnoreCase("description")) {
            continue;
          }
          toCreate[i++] = new Tag(entry.getKey(), entry.getValue().toString());
        }
      }
      Tag t = new Tag();

      t.setKey("Name");
      t.setValue(cfg.getFriendlyName());
      toCreate[i++] = t;
      t = new Tag();
      t.setKey("Description");
      t.setValue(cfg.getDescription());
      toCreate[i] = t;
      getProvider().createTags(server.getProviderVirtualMachineId(), toCreate);
      if (!existingVolumes.isEmpty()) {
        final VirtualMachine vm = server;

        getProvider().hold();
        Thread thread = new Thread() {
          public void run() {
            try {
              for (VMLaunchOptions.VolumeAttachment a : existingVolumes) {
                try {
                  getProvider().getComputeServices().getVolumeSupport().attach(a.existingVolumeId, vm.getProviderMachineImageId(), a.deviceId);
                } catch (Throwable t) {
                  t.printStackTrace();
                }
              }
            } finally {
              getProvider().release();
            }
          }
        };

        thread.setName("Volume Mounter for " + server);
        thread.start();
      }
      return server;
    } finally {
      APITrace.end();
    }
  }

  private void enableIpForwarding( final String instanceId ) throws CloudException {

    Thread t = new Thread() {
      public void run() {
        APITrace.begin( getProvider(), "enableIpForwarding" );

        long timeout = System.currentTimeMillis() + CalendarWrapper.MINUTE;

        while ( timeout > System.currentTimeMillis() ) {
          try {
            Map<String, String> params = getProvider().getStandardParameters( getProvider().getContext(), EC2Method.MODIFY_INSTANCE_ATTRIBUTE );
            EC2Method m;

            params.put( "InstanceId", instanceId );
            params.put( "SourceDestCheck.Value", "false" );
            m = new EC2Method( getProvider(), getProvider().getEc2Url(), params );

            m.invoke();
            return;
          }
          catch ( EC2Exception ex ) {
            if ( ex.getStatus() != 404 ) {
              logger.error( "Unable to modify instance attributes on " + instanceId + ".", ex );
              return;
            }
          }
          catch ( Throwable ex ) {
            logger.error( "Unable to modify instance attributes on " + instanceId + ".", ex );
            return;
          }
          finally {
            APITrace.end();
          }

          try {
            Thread.sleep( 5000L );
          }
          catch ( InterruptedException ignore ) {
          }
        }

      }
    };

    t.setName( EC2Method.MODIFY_INSTANCE_ATTRIBUTE + " thread" );
    t.setDaemon( true );
    t.start();

  }

  @Override
  public
  @Nonnull
  Iterable<ResourceStatus> listVirtualMachineStatus() throws InternalException, CloudException {
    APITrace.begin(getProvider(), "listVirtualMachineStatus");
    try {
      ProviderContext ctx = getProvider().getContext();

      if (ctx == null) {
        throw new CloudException("No context was established for this request");
      }
      Map<String, String> parameters = getProvider().getStandardParameters(getProvider().getContext(), EC2Method.DESCRIBE_INSTANCES);
      EC2Method method = new EC2Method(getProvider(), getProvider().getEc2Url(), parameters);
      ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();
      NodeList blocks;
      Document doc;

      try {
        doc = method.invoke();
      } catch (EC2Exception e) {
        logger.error(e.getSummary());
        throw new CloudException(e);
      }
      blocks = doc.getElementsByTagName("instancesSet");
      for (int i = 0; i < blocks.getLength(); i++) {
        NodeList instances = blocks.item(i).getChildNodes();

        for (int j = 0; j < instances.getLength(); j++) {
          Node instance = instances.item(j);

          if (instance.getNodeName().equals("item")) {
            ResourceStatus status = toStatus(instance);

            if (status != null) {
              list.add(status);
            }
          }
        }
      }
      return list;
    } finally {
      APITrace.end();
    }
  }

  @Override
  public
  @Nonnull
  Iterable<VirtualMachine> listVirtualMachines() throws InternalException, CloudException {
    return listVirtualMachinesWithParams(null, null);
  }

  @Override
  public
  @Nonnull
  Iterable<VirtualMachine> listVirtualMachines(@Nullable VMFilterOptions options) throws InternalException, CloudException {
    Map<String, String> tags = ((options == null || options.isMatchesAny()) ? null : options.getTags());

    if (tags != null) {
      // tag advantage of EC2-based filtering if we can...
      Map<String, String> extraParameters = new HashMap<String, String>();

      getProvider().putExtraParameters(extraParameters, getProvider().getTagFilterParams(options.getTags()));

      String regex = options.getRegex();

      if (regex != null) {
        // still have to match on regex
        options = VMFilterOptions.getInstance(false, regex);
      } else {
        // nothing else to match on
        options = null;
      }
      return listVirtualMachinesWithParams(extraParameters, options);
    } else {
      return listVirtualMachinesWithParams(null, options);
    }
  }

  private
  @Nonnull
  Iterable<VirtualMachine> listVirtualMachinesWithParams(Map<String, String> extraParameters, @Nullable VMFilterOptions options) throws InternalException, CloudException {
    APITrace.begin(getProvider(), "listVirtualMachines");
    try {
      ProviderContext ctx = getProvider().getContext();

      if (ctx == null) {
        throw new CloudException("No context was established for this request");
      }
      Iterable<IpAddress> addresses = Collections.emptyList();
      NetworkServices services = getProvider().getNetworkServices();

      if (services != null) {
        if (services.hasIpAddressSupport()) {
          IpAddressSupport support = services.getIpAddressSupport();

          if (support != null) {
            addresses = support.listIpPool(IPVersion.IPV4, false);
          }
        }
      }

      Map<String, String> parameters = getProvider().getStandardParameters(getProvider().getContext(), EC2Method.DESCRIBE_INSTANCES);

      getProvider().putExtraParameters(parameters, extraParameters);

      EC2Method method = new EC2Method(getProvider(), getProvider().getEc2Url(), parameters);
      ArrayList<VirtualMachine> list = new ArrayList<VirtualMachine>();
      NodeList blocks;
      Document doc;

      try {
        doc = method.invoke();
      } catch (EC2Exception e) {
        logger.error(e.getSummary());
        throw new CloudException(e);
      }
      blocks = doc.getElementsByTagName("instancesSet");
      for (int i = 0; i < blocks.getLength(); i++) {
        NodeList instances = blocks.item(i).getChildNodes();

        for (int j = 0; j < instances.getLength(); j++) {
          Node instance = instances.item(j);

          if (instance.getNodeName().equals("item")) {
            VirtualMachine vm = toVirtualMachine(ctx, instance, addresses);

            if (options == null || options.matches(vm)) {
              list.add(vm);
            }
          }
        }
      }
      return list;
    } finally {
      APITrace.end();
    }
  }

  @Override
  public void pause(@Nonnull String vmId) throws InternalException, CloudException {
    throw new OperationNotSupportedException("Pause/unpause not supported by the EC2 API");
  }

  @Override
  public
  @Nonnull
  String[] mapServiceAction(@Nonnull ServiceAction action) {
    if (action.equals(VirtualMachineSupport.ANY)) {
      return new String[]{EC2Method.EC2_PREFIX + "*"};
    } else if (action.equals(VirtualMachineSupport.BOOT)) {
      return new String[]{EC2Method.EC2_PREFIX + EC2Method.START_INSTANCES};
    } else if (action.equals(VirtualMachineSupport.CLONE)) {
      return new String[0];
    } else if (action.equals(VirtualMachineSupport.CREATE_VM)) {
      return new String[]{EC2Method.EC2_PREFIX + EC2Method.RUN_INSTANCES};
    } else if (action.equals(VirtualMachineSupport.GET_VM) || action.equals(VirtualMachineSupport.LIST_VM)) {
      return new String[]{EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_INSTANCES};
    } else if (action.equals(VirtualMachineSupport.PAUSE)) {
      return new String[]{EC2Method.EC2_PREFIX + EC2Method.STOP_INSTANCES};
    } else if (action.equals(VirtualMachineSupport.REBOOT)) {
      return new String[]{EC2Method.EC2_PREFIX + EC2Method.REBOOT_INSTANCES};
    } else if (action.equals(VirtualMachineSupport.REMOVE_VM)) {
      return new String[]{EC2Method.EC2_PREFIX + EC2Method.TERMINATE_INSTANCES};
    } else if (action.equals(VirtualMachineSupport.TOGGLE_ANALYTICS)) {
      return new String[]{EC2Method.EC2_PREFIX + EC2Method.MONITOR_INSTANCES};
    } else if (action.equals(VirtualMachineSupport.VIEW_ANALYTICS)) {
      return new String[]{EC2Method.EC2_PREFIX + EC2Method.GET_METRIC_STATISTICS};
    } else if (action.equals(VirtualMachineSupport.VIEW_CONSOLE)) {
      return new String[]{EC2Method.EC2_PREFIX + EC2Method.GET_CONSOLE_OUTPUT};
    }
    return new String[0];
  }

  @Override
  public void stop(@Nonnull String instanceId, boolean force) throws InternalException, CloudException {
    APITrace.begin(getProvider(), "stopVM");
    try {
      VirtualMachine vm = getVirtualMachine(instanceId);

      if (vm == null) {
        throw new CloudException("No such instance: " + instanceId);
      }
      if (!vm.isPersistent()) {
        throw new OperationNotSupportedException("Instances backed by ephemeral drives are not start/stop capable");
      }
      Map<String, String> parameters = getProvider().getStandardParameters(getProvider().getContext(), EC2Method.STOP_INSTANCES);
      EC2Method method;

      parameters.put("InstanceId.1", instanceId);
      if (force) {
        parameters.put("Force", "true");
      }
      method = new EC2Method(getProvider(), getProvider().getEc2Url(), parameters);
      try {
        method.invoke();
      } catch (EC2Exception e) {
        logger.error(e.getSummary());
        throw new CloudException(e);
      }
    } finally {
      APITrace.end();
    }
  }

  @Override
  public void reboot(@Nonnull String instanceId) throws CloudException, InternalException {
    APITrace.begin(getProvider(), "rebootVM");
    try {
      Map<String, String> parameters = getProvider().getStandardParameters(getProvider().getContext(), EC2Method.REBOOT_INSTANCES);
      EC2Method method;

      parameters.put("InstanceId.1", instanceId);
      method = new EC2Method(getProvider(), getProvider().getEc2Url(), parameters);
      try {
        method.invoke();
      } catch (EC2Exception e) {
        logger.error(e.getSummary());
        throw new CloudException(e);
      }
    } finally {
      APITrace.end();
    }
  }

  @Override
  public void resume(@Nonnull String vmId) throws CloudException, InternalException {
    throw new OperationNotSupportedException("Suspend/resume not supported by the EC2 API");
  }

  private String resolve(String dnsName) {
    if (dnsName != null && dnsName.length() > 0) {
      InetAddress[] addresses;

      try {
        addresses = InetAddress.getAllByName(dnsName);
      } catch (UnknownHostException e) {
        addresses = null;
      }
      if (addresses != null && addresses.length > 0) {
        dnsName = addresses[0].getHostAddress();
      } else {
        dnsName = dnsName.split("\\.")[0];
        dnsName = dnsName.replaceAll("-", "\\.");
        dnsName = dnsName.substring(4);
      }
    }
    return dnsName;
  }

  @Override
  public boolean supportsPauseUnpause(@Nonnull VirtualMachine vm) {
    return false;
  }

  @Override
  public boolean supportsStartStop(@Nonnull VirtualMachine vm) {
    return vm.isPersistent();
  }

  @Override
  public boolean supportsSuspendResume(@Nonnull VirtualMachine vm) {
    return false;
  }

  @Override
  public void suspend(@Nonnull String vmId) throws CloudException, InternalException {
    throw new OperationNotSupportedException("Suspend/resume not supported by the EC2 API");
  }

  @Override
  public void terminate(@Nonnull String instanceId, @Nullable String explanation) throws InternalException, CloudException {
    APITrace.begin(getProvider(), "terminateVM");
    try {
      Map<String, String> parameters = getProvider().getStandardParameters(getProvider().getContext(), EC2Method.TERMINATE_INSTANCES);
      EC2Method method;

      parameters.put("InstanceId.1", instanceId);
      method = new EC2Method(getProvider(), getProvider().getEc2Url(), parameters);
      try {
        method.invoke();
      } catch (EC2Exception e) {
        logger.error(e.getSummary());
        throw new CloudException(e);
      }
    } finally {
      APITrace.end();
    }
  }

  @Override
  public void unpause(@Nonnull String vmId) throws CloudException, InternalException {
    throw new OperationNotSupportedException("Pause/unpause not supported by the EC2 API");
  }

  private
  @Nullable
  ResourceStatus toStatus(@Nullable Node instance) throws CloudException {
    if (instance == null) {
      return null;
    }
    NodeList attrs = instance.getChildNodes();
    VmState state = VmState.PENDING;
    String vmId = null;

    for (int i = 0; i < attrs.getLength(); i++) {
      Node attr = attrs.item(i);
      String name;

      name = attr.getNodeName();
      if (name.equals("instanceId")) {
        vmId = attr.getFirstChild().getNodeValue().trim();
      } else if (name.equals("instanceState")) {
        NodeList details = attr.getChildNodes();

        for (int j = 0; j < details.getLength(); j++) {
          Node detail = details.item(j);

          name = detail.getNodeName();
          if (name.equals("name")) {
            String value = detail.getFirstChild().getNodeValue().trim();

            state = getServerState(value);
          }
        }
      }
    }
    if (vmId == null) {
      return null;
    }
    return new ResourceStatus(vmId, state);
  }

  private
  @Nullable
  VirtualMachine toVirtualMachine(@Nonnull ProviderContext ctx, @Nullable Node instance, @Nonnull Iterable<IpAddress> addresses) throws CloudException {
    if (instance == null) {
      return null;
    }
    String rootDeviceName = null;
    NodeList attrs = instance.getChildNodes();
    VirtualMachine server = new VirtualMachine();

    server.setPersistent(false);
    server.setProviderOwnerId(ctx.getAccountNumber());
    server.setCurrentState(VmState.PENDING);
    server.setName(null);
    server.setDescription(null);
    for (int i = 0; i < attrs.getLength(); i++) {
      Node attr = attrs.item(i);
      String name;

      name = attr.getNodeName();
      if (name.equals("instanceId")) {
        String value = attr.getFirstChild().getNodeValue().trim();

        server.setProviderVirtualMachineId(value);
      } else if (name.equals("architecture")) {
        String value = attr.getFirstChild().getNodeValue().trim();
        Architecture architecture;

        if (value.equalsIgnoreCase("i386")) {
          architecture = Architecture.I32;
        } else {
          architecture = Architecture.I64;
        }
        server.setArchitecture(architecture);
      } else if (name.equals("imageId")) {
        String value = attr.getFirstChild().getNodeValue().trim();

        server.setProviderMachineImageId(value);
      } else if (name.equals("kernelId")) {
        String value = attr.getFirstChild().getNodeValue().trim();

        server.setTag("kernelImageId", value);
        server.setProviderKernelImageId(value);
      } else if (name.equals("ramdiskId")) {
        String value = attr.getFirstChild().getNodeValue().trim();

        server.setTag("ramdiskImageId", value);
        server.setProviderRamdiskImageId(value);
      } else if (name.equalsIgnoreCase("subnetId")) {
        server.setProviderSubnetId(attr.getFirstChild().getNodeValue().trim());
      } else if (name.equalsIgnoreCase("vpcId")) {
        server.setProviderVlanId(attr.getFirstChild().getNodeValue().trim());
      } else if (name.equals("instanceState")) {
        NodeList details = attr.getChildNodes();

        for (int j = 0; j < details.getLength(); j++) {
          Node detail = details.item(j);

          name = detail.getNodeName();
          if (name.equals("name")) {
            String value = detail.getFirstChild().getNodeValue().trim();

            server.setCurrentState(getServerState(value));
          }
        }
      } else if (name.equals("privateDnsName")) {
        if (attr.hasChildNodes()) {
          String value = attr.getFirstChild().getNodeValue();
          RawAddress[] addrs = server.getPrivateAddresses();

          server.setPrivateDnsAddress(value);
          if (addrs == null || addrs.length < 1) {
            value = guess(value);
            if (value != null) {
              server.setPrivateAddresses(new RawAddress(value));
            }
          }
        }
      } else if (name.equals("dnsName")) {
        if (attr.hasChildNodes()) {
          String value = attr.getFirstChild().getNodeValue();
          RawAddress[] addrs = server.getPublicAddresses();

          server.setPublicDnsAddress(value);
          if (addrs == null || addrs.length < 1) {
            server.setPublicAddresses(new RawAddress(resolve(value)));
          }
        }
      } else if (name.equals("privateIpAddress")) {
        if (attr.hasChildNodes()) {
          String value = attr.getFirstChild().getNodeValue();

          server.setPrivateAddresses(new RawAddress(value));
        }
      } else if (name.equals("ipAddress")) {
        if (attr.hasChildNodes()) {
          String value = attr.getFirstChild().getNodeValue();

          server.setPublicAddresses(new RawAddress(value));
          for (IpAddress addr : addresses) {
            if (value.equals(addr.getRawAddress().getIpAddress())) {
              server.setProviderAssignedIpAddressId(addr.getProviderIpAddressId());
              break;
            }
          }
        }
      } else if (name.equals("rootDeviceType")) {
        if (attr.hasChildNodes()) {
          server.setPersistent(attr.getFirstChild().getNodeValue().equalsIgnoreCase("ebs"));
        }
      } else if (name.equals("tagSet")) {

        Map<String, String> tags = getProvider().getTagsFromTagSet(attr);
        if (tags != null && tags.size() > 0) {
          server.setTags(tags);
          for (Map.Entry<String, String> entry : tags.entrySet()) {
            if (entry.getKey().equalsIgnoreCase("name")) {
              server.setName(entry.getValue());
            } else if (entry.getKey().equalsIgnoreCase("description")) {
              server.setDescription(entry.getValue());
            }
          }
        }
      } else if (name.equals("instanceType")) {
        String value = attr.getFirstChild().getNodeValue().trim();

        server.setProductId(value);
      } else if (name.equals("launchTime")) {
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        fmt.setCalendar(UTC_CALENDAR);
        String value = attr.getFirstChild().getNodeValue().trim();

        try {
          server.setLastBootTimestamp(fmt.parse(value).getTime());
          server.setCreationTimestamp(server.getLastBootTimestamp());
        } catch (ParseException e) {
          logger.error(e);
          e.printStackTrace();
          throw new CloudException(e);
        }
      } else if (name.equals("platform")) {
        if (attr.hasChildNodes()) {
          Platform platform = Platform.guess(attr.getFirstChild().getNodeValue());
          if (platform.equals(Platform.UNKNOWN)) {
            platform = Platform.UNIX;
          }
          server.setPlatform(platform);
        }
      } else if (name.equals("placement")) {
        NodeList details = attr.getChildNodes();

        for (int j = 0; j < details.getLength(); j++) {
          Node detail = details.item(j);

          name = detail.getNodeName();
          if (name.equals("availabilityZone")) {
            if (detail.hasChildNodes()) {
              String value = detail.getFirstChild().getNodeValue().trim();

              server.setProviderDataCenterId(value);
            }
          }
        }
      } else if (name.equals("networkInterfaceSet")) {
        ArrayList<String> networkInterfaceIds = new ArrayList<String>();
        if (attr.hasChildNodes()) {
          NodeList items = attr.getChildNodes();
          for (int j = 0; j < items.getLength(); j++) {
            Node item = items.item(j);

            if (item.getNodeName().equals("item") && item.hasChildNodes()) {
              NodeList parts = item.getChildNodes();
              String networkInterfaceId = null;

              for (int k = 0; k < parts.getLength(); k++) {
                Node part = parts.item(k);

                if (part.getNodeName().equalsIgnoreCase("networkInterfaceId")) {
                  if (part.hasChildNodes()) {
                    networkInterfaceId = part.getFirstChild().getNodeValue().trim();
                  }
                }
              }
              if (networkInterfaceId != null) {
                networkInterfaceIds.add(networkInterfaceId);
              }
            }
          }
        }
        if (networkInterfaceIds.size() > 0) {
          server.setProviderNetworkInterfaceIds(networkInterfaceIds.toArray(new String[networkInterfaceIds.size()]));
        }
        /*
          [FIXME?] TODO: Really networkInterfaceSet needs to be own type/resource
          Example:
          <networkInterfaceSet>
            <item>
              <networkInterfaceId>eni-1a2b3c4d</networkInterfaceId>
              <subnetId>subnet-1a2b3c4d</subnetId>
              <vpcId>vpc-1a2b3c4d</vpcId>
              <description>Primary network interface</description>
              <ownerId>111122223333</ownerId>
              <status>in-use</status>
              <macAddress>1b:2b:3c:4d:5e:6f</macAddress>
              <privateIpAddress>10.0.0.12</privateIpAddress>
              <sourceDestCheck>true</sourceDestCheck>
              <groupSet>
                <item>
                  <groupId>sg-1a2b3c4d</groupId>
                  <groupName>my-security-group</groupName>
                </item>
              </groupSet>
              <attachment>
                <attachmentId>eni-attach-1a2b3c4d</attachmentId>
                <deviceIndex>0</deviceIndex>
                <status>attached</status>
                <attachTime>YYYY-MM-DDTHH:MM:SS+0000</attachTime>
                <deleteOnTermination>true</deleteOnTermination>
              </attachment>
              <association>
                <publicIp>198.51.100.63</publicIp>
                <ipOwnerId>111122223333</ipOwnerId>
              </association>
              <privateIpAddressesSet>
                <item>
                  <privateIpAddress>10.0.0.12</privateIpAddress>
                  <primary>true</primary>
                  <association>
                    <publicIp>198.51.100.63</publicIp>
                    <ipOwnerId>111122223333</ipOwnerId>
                  </association>
                </item>
                <item>
                  <privateIpAddress>10.0.0.14</privateIpAddress>
                  <primary>false</primary>
                  <association>
                    <publicIp>198.51.100.177</publicIp>
                    <ipOwnerId>111122223333</ipOwnerId>
                  </association>
                </item>
              </privateIpAddressesSet>
            </item>
          </networkInterfaceSet>
         */
      } else if (name.equals("keyName")) {
        String value = attr.getFirstChild().getNodeValue().trim();
        server.setProviderKeypairId(value);
      } else if (name.equals("groupSet")) {
        ArrayList<String> firewalls = new ArrayList<String>();
        if (attr.hasChildNodes()) {
          NodeList tags = attr.getChildNodes();

          for (int j = 0; j < tags.getLength(); j++) {
            Node tag = tags.item(j);

            if (tag.getNodeName().equals("item") && tag.hasChildNodes()) {
              NodeList parts = tag.getChildNodes();
              String groupId = null;

              for (int k = 0; k < parts.getLength(); k++) {
                Node part = parts.item(k);

                if (part.getNodeName().equalsIgnoreCase("groupId")) {
                  if (part.hasChildNodes()) {
                    groupId = part.getFirstChild().getNodeValue().trim();
                  }
                }
              }
              if (groupId != null) {
                firewalls.add(groupId);
              }
            }
          }
        }
        if (firewalls.size() > 0) {
          server.setProviderFirewallIds(firewalls.toArray(new String[firewalls.size()]));
        }
      } else if ("blockDeviceMapping".equals(name) && attr.hasChildNodes()) {
        List<Volume> volumes = new ArrayList<Volume>();
        if (attr.hasChildNodes()) {
          NodeList blockDeviceMapping = attr.getChildNodes();

          for (int j = 0; j < blockDeviceMapping.getLength(); j++) {
            Node bdmItems = blockDeviceMapping.item(j);

            if (bdmItems.getNodeName().equals("item") && bdmItems.hasChildNodes()) {
              NodeList items = bdmItems.getChildNodes();
              Volume volume = new Volume();

              for (int k = 0; k < items.getLength(); k++) {
                Node item = items.item(k);
                String itemNodeName = item.getNodeName();

                if ("deviceName".equals(itemNodeName)) {
                  volume.setDeviceId(AWSCloud.getTextValue(item));
                } else if ("ebs".equals(itemNodeName)) {
                  NodeList ebsNodeList = item.getChildNodes();

                  for (int l = 0; l < ebsNodeList.getLength(); l++) {
                    Node ebsNode = ebsNodeList.item(l);
                    String ebsNodeName = ebsNode.getNodeName();

                    if ("volumeId".equals(ebsNodeName)) {
                      volume.setProviderVolumeId(AWSCloud.getTextValue(ebsNode));
                    } else if ("status".equals(ebsNodeName)) {
                      volume.setCurrentState(EBSVolume.toVolumeState(ebsNode));
                    } else if ("deleteOnTermination".equals(ebsNodeName)) {
                      volume.setDeleteOnVirtualMachineTermination(AWSCloud.getBooleanValue(ebsNode));
                    }
                  }

                }
              }

              if (volume.getDeviceId() != null) {
                volumes.add(volume);
              }
            }
          }
        }
        if (volumes.size() > 0) {
          server.setVolumes(volumes.toArray(new Volume[volumes.size()]));
        }
      } else if ("rootDeviceName".equals(name) && attr.hasChildNodes()) {
        rootDeviceName = AWSCloud.getTextValue(attr);
      } else if ("ebsOptimized".equals(name) && attr.hasChildNodes()) {
        if (attr.hasChildNodes()) {
          server.setIoOptimized(Boolean.valueOf(attr.getFirstChild().getNodeValue()));
        }
      } else if ( "sourceDestCheck".equals( name ) && attr.hasChildNodes() ) {
        if ( attr.hasChildNodes() ) {
          /**
           * note: a value of <sourceDestCheck>true</sourceDestCheck> means this instance cannot
           * function as a NAT instance, so we negate the value to indicate if it is allowed
           */
          server.setIpForwardingAllowed( !Boolean.valueOf( attr.getFirstChild().getNodeValue() ) );
        }
      }
    }
    if (server.getPlatform() == null) {
      server.setPlatform(Platform.UNKNOWN);
    }
    server.setProviderRegionId(ctx.getRegionId());
    if (server.getName() == null) {
      server.setName(server.getProviderVirtualMachineId());
    }
    if (server.getDescription() == null) {
      server.setDescription(server.getName() + " (" + server.getProductId() + ")");
    }
    if (server.getArchitecture() == null && server.getProductId() != null) {
      server.setArchitecture(getArchitecture(server.getProductId()));
    } else if (server.getArchitecture() == null) {
      server.setArchitecture(Architecture.I64);
    }

    // find the root device in the volumes list and set boolean value
    if (rootDeviceName != null && server.getVolumes() != null) {
      for (Volume volume : server.getVolumes()) {
        if (rootDeviceName.equals(volume.getDeviceId())) {
          volume.setRootVolume(true);
          break;
        }
      }
    }

    return server;
  }

  @Override
  public void disableAnalytics(@Nonnull String instanceId) throws InternalException, CloudException {
    APITrace.begin(getProvider(), "disableVMAnalytics");
    try {
      if (getProvider().getEC2Provider().isAWS() || getProvider().getEC2Provider().isEnStratus()) {
        Map<String, String> parameters = getProvider().getStandardParameters(getProvider().getContext(), EC2Method.UNMONITOR_INSTANCES);
        EC2Method method;

        parameters.put("InstanceId.1", instanceId);
        method = new EC2Method(getProvider(), getProvider().getEc2Url(), parameters);
        try {
          method.invoke();
        } catch (EC2Exception e) {
          logger.error(e.getSummary());
          throw new CloudException(e);
        }
      }
    } finally {
      APITrace.end();
    }
  }

  private
  @Nullable
  VirtualMachineProduct toProduct(@Nonnull JSONObject json) throws InternalException {
        /*
                    {
                "architectures":["I32"],
                "id":"m1.small",
                "name":"Small Instance (m1.small)",
                "description":"Small Instance (m1.small)",
                "cpuCount":1,
                "rootVolumeSizeInGb":160,
                "ramSizeInMb": 1700
            },
         */
    VirtualMachineProduct prd = new VirtualMachineProduct();

    try {
      if (json.has("id")) {
        prd.setProviderProductId(json.getString("id"));
      } else {
        return null;
      }
      if (json.has("name")) {
        prd.setName(json.getString("name"));
      } else {
        prd.setName(prd.getProviderProductId());
      }
      if (json.has("description")) {
        prd.setDescription(json.getString("description"));
      } else {
        prd.setDescription(prd.getName());
      }
      if (json.has("cpuCount")) {
        prd.setCpuCount(json.getInt("cpuCount"));
      } else {
        prd.setCpuCount(1);
      }
      if (json.has("rootVolumeSizeInGb")) {
        prd.setRootVolumeSize(new Storage<Gigabyte>(json.getInt("rootVolumeSizeInGb"), Storage.GIGABYTE));
      } else {
        prd.setRootVolumeSize(new Storage<Gigabyte>(1, Storage.GIGABYTE));
      }
      if (json.has("ramSizeInMb")) {
        prd.setRamSize(new Storage<Megabyte>(json.getInt("ramSizeInMb"), Storage.MEGABYTE));
      } else {
        prd.setRamSize(new Storage<Megabyte>(512, Storage.MEGABYTE));
      }
      if (json.has("standardHourlyRates")) {
        JSONArray rates = json.getJSONArray("standardHourlyRates");

        for (int i = 0; i < rates.length(); i++) {
          JSONObject rate = rates.getJSONObject(i);

          if (rate.has("rate")) {
            prd.setStandardHourlyRate((float) rate.getDouble("rate"));
          }
        }
      }
    } catch (JSONException e) {
      throw new InternalException(e);
    }
    return prd;
  }

  @Override
  public void updateTags(@Nonnull String vmId, @Nonnull Tag... tags) throws CloudException, InternalException {
    getProvider().createTags(vmId, tags);
  }

  @Override
  public void updateTags(@Nonnull String[] vmIds, @Nonnull Tag... tags) throws CloudException, InternalException {
    getProvider().createTags(vmIds, tags);
  }

  @Override
  public void removeTags(@Nonnull String vmId, @Nonnull Tag... tags) throws CloudException, InternalException {
    getProvider().removeTags(vmId, tags);
  }

  @Override
  public void removeTags(@Nonnull String[] vmIds, @Nonnull Tag... tags) throws CloudException, InternalException {
    getProvider().removeTags(vmIds, tags);
  }

}
