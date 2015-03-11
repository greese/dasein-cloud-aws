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

package org.dasein.cloud.aws.admin;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.admin.Offering;
import org.dasein.cloud.admin.Prepayment;
import org.dasein.cloud.admin.PrepaymentState;
import org.dasein.cloud.admin.PrepaymentSupport;
import org.dasein.cloud.aws.AWSCloud;
import org.dasein.cloud.aws.compute.EC2Exception;
import org.dasein.cloud.aws.compute.EC2Method;
import org.dasein.cloud.compute.ComputeServices;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.util.APITrace;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ReservedInstance implements PrepaymentSupport {
    static private final Logger logger = AWSCloud.getLogger(ReservedInstance.class);

    static private final int SECONDS_IN_DAY = (60 * 60 * 24);

    private AWSCloud provider = null;
    
    ReservedInstance(@Nonnull AWSCloud provider) {
    	this.provider = provider;
    }
    
	@Override
	public @Nullable Offering getOffering(@Nonnull String offeringId) throws InternalException, CloudException {
        APITrace.begin(provider, "Prepayment.getOffering");
        try {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_RESERVED_INSTANCES_OFFERINGS);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("ReservedInstancesOfferingId", offeringId);
            method = new EC2Method(provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && code.startsWith("InvalidReservedInstancesOfferingId") ) {
                    return null;
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("reservedInstancesOfferingsSet");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("item") ) {
                        Offering offering = toOffering(item);

                        if( offering != null && offering.getProviderOfferingId().equals(offeringId) ) {
                            return offering;
                        }
                    }
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
	}

	@Override
	public @Nullable Prepayment getPrepayment(@Nonnull String prepaymentId) throws InternalException, CloudException {
        APITrace.begin(provider, "Prepayment.getPrepayment");
        try {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_RESERVED_INSTANCES);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("ReservedInstancesId.1", prepaymentId);
            method = new EC2Method(provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                String code = e.getCode();

                if( code != null && code.startsWith("InvalidReservedInstancesId") ) {
                    return null;
                }
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("reservedInstancesSet");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("item") ) {
                        Prepayment prepayment = toPrepayment(item);

                        if( prepayment != null && prepayment.getProviderPrepaymentId().equals(prepaymentId) ) {
                            return prepayment;
                        }
                    }
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
	}

	@Override
	public @Nonnull String getProviderTermForOffering(@Nonnull Locale locale) {
		return "offering";
	}

	@Override
	public @Nonnull String getProviderTermForPrepayment(@Nonnull Locale locale) {
		return "reserved instance";
	}

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(provider, "Prepayment.isSubscribed");
        try {
            ComputeServices svc = provider.getComputeServices();

            if( svc == null ) {
                return false;
            }
            VirtualMachineSupport support = svc.getVirtualMachineSupport();

            return (support != null && support.isSubscribed());
        }
        finally {
            APITrace.end();
        }
    }

	@Override
	public @Nonnull Collection<Offering> listOfferings() throws InternalException, CloudException {
        APITrace.begin(provider, "Prepayment.listOfferings");
        try {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_RESERVED_INSTANCES_OFFERINGS);
            List<Offering> list = new ArrayList<Offering>();
            EC2Method method;

            NodeList blocks;
            Document doc;

            method = new EC2Method(provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("reservedInstancesOfferingsSet");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("item") ) {
                        Offering offering = toOffering(item);

                        if( offering != null ) {
                            list.add(offering);
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
	public @Nonnull Collection<Prepayment> listPrepayments() throws InternalException, CloudException {
        APITrace.begin(provider, "Prepayment.listPrepayments");
        try {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.DESCRIBE_RESERVED_INSTANCES);
            List<Prepayment> list = new ArrayList<Prepayment>();
            EC2Method method;
            NodeList blocks;
            Document doc;

            method = new EC2Method(provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("reservedInstancesSet");
            for( int i=0; i<blocks.getLength(); i++ ) {
                NodeList items = blocks.item(i).getChildNodes();

                for( int j=0; j<items.getLength(); j++ ) {
                    Node item = items.item(j);

                    if( item.getNodeName().equals("item") ) {
                        Prepayment prepayment = toPrepayment(item);

                        if( prepayment != null ) {
                            list.add(prepayment);
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
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        if( action.equals(PrepaymentSupport.ANY) ) {
            return new String[] { EC2Method.EC2_PREFIX + "*" };
        }
        else if( action.equals(PrepaymentSupport.GET_OFFERING) || action.equals(PrepaymentSupport.LIST_OFFERING) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_RESERVED_INSTANCES_OFFERINGS };
        }
        else if( action.equals(PrepaymentSupport.GET_PREPAYMENT) || action.equals(PrepaymentSupport.LIST_PREPAYMENT) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.DESCRIBE_RESERVED_INSTANCES };
        }
        else if( action.equals(PrepaymentSupport.PREPAY) ) {
            return new String[] { EC2Method.EC2_PREFIX + EC2Method.PURCHASE_RESERVED_INSTANCES_OFFERING };
        }
        return new String[0];
    }

	@Override
	public @Nonnull String prepay(@Nonnull String offeringId, @Nonnegative int count) throws InternalException, CloudException {
        APITrace.begin(provider, "Prepayment.prepay");
        try {
            Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), EC2Method.PURCHASE_RESERVED_INSTANCES_OFFERING);
            EC2Method method;
            NodeList blocks;
            Document doc;

            parameters.put("ReservedInstanceOfferingId.1", offeringId);
            parameters.put("InstanceCount.1", String.valueOf(count));
            method = new EC2Method(provider, parameters);
            try {
                doc = method.invoke();
            }
            catch( EC2Exception e ) {
                logger.error(e.getSummary());
                throw new CloudException(e);
            }
            blocks = doc.getElementsByTagName("reservedInstancesId");
            if( blocks.getLength() > 0 ) {
                return blocks.item(0).getFirstChild().getNodeValue().trim();
            }
            throw new CloudException("Unable to identify newly reserved instance");
        }
        finally {
            APITrace.end();
        }
	}
	
	private @Nullable Offering toOffering(@Nullable Node node) throws CloudException {
        if( node == null ) {
            return null;
        }
		Offering offering = new Offering();
		NodeList attrs = node.getChildNodes();
		
		for( int i=0; i<attrs.getLength(); i++ ) {
			Node attr = attrs.item(i);
			String name;
	
			name = attr.getNodeName();
			if( name.equals("reservedInstancesOfferingId") ) {
				offering.setOfferingId(attr.getFirstChild().getNodeValue().trim());
			}
			else if( name.equals("instanceType") ) {
				offering.setSize(attr.getFirstChild().getNodeValue().trim());
			}
			else if( name.equals("availabilityZone") ) {
				offering.setDataCenterId(attr.getFirstChild().getNodeValue().trim());
			}
			else if( name.equals("fixedPrice") ) {
				double fixedFee = Double.parseDouble(attr.getFirstChild().getNodeValue().trim());
				
				offering.setFixedFee(fixedFee);				
			}
			else if( name.equals("usagePrice") ) {
				double usageFee = Double.parseDouble(attr.getFirstChild().getNodeValue().trim());
				
				offering.setUsageFee(usageFee);
			}
			else if( name.equals("duration") ) {
				int seconds = Integer.parseInt(attr.getFirstChild().getNodeValue().trim());

				offering.setPeriodInDays(seconds/SECONDS_IN_DAY);				
			}
		}
		offering.setCurrencyCode("USD");
		offering.setPlatform(Platform.UNIX);
		offering.setSoftware(null);
		return offering;
	}
	
	private @Nullable Prepayment toPrepayment(@Nullable Node node) throws CloudException {
        if( node == null ) {
            return null;
        }
		Prepayment prepayment = new Prepayment();
		NodeList attrs = node.getChildNodes();
		
		for( int i=0; i<attrs.getLength(); i++ ) {
			Node attr = attrs.item(i);
			String name;
			
			name = attr.getNodeName();
			if( name.equals("reservedInstancesId") ) {
				prepayment.setPrepaymentId(attr.getFirstChild().getNodeValue().trim());
			}
			else if( name.equals("instanceType") ) {
				prepayment.setSize(attr.getFirstChild().getNodeValue().trim());
			}
      else if( name.equals("offeringType") ) {
        prepayment.setUtilization(attr.getFirstChild().getNodeValue().trim());
      }
			else if( name.equals("availabilityZone") ) {
				prepayment.setDataCenterId(attr.getFirstChild().getNodeValue().trim());
			}
			else if( name.equals("instanceCount") ) {
				int count = Integer.parseInt(attr.getFirstChild().getNodeValue().trim());
				
				prepayment.setCount(count);
			}
			else if( name.equals("fixedPrice") ) {
				double fixedFee = Double.parseDouble(attr.getFirstChild().getNodeValue().trim());
				
				prepayment.setFixedFee(fixedFee);				
			}
			else if( name.equals("usagePrice") ) {
				double usageFee = Double.parseDouble(attr.getFirstChild().getNodeValue().trim());
				
				prepayment.setUsageFee(usageFee);
			}
			else if( name.equals("duration") ) {
				int seconds = Integer.parseInt(attr.getFirstChild().getNodeValue().trim());

				prepayment.setPeriodInDays(seconds/SECONDS_IN_DAY);				
			}
			else if( name.equals("start") ) {
				SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
				String start = attr.getFirstChild().getNodeValue().trim();

				try {
					prepayment.setPeriodStartTimestamp(fmt.parse(start).getTime());
				} 
				catch( ParseException e ) {
					logger.error(e);
					e.printStackTrace();
					throw new CloudException(e);
				}
			}
			else if( name.equals("state") ) {
				String state = attr.getFirstChild().getNodeValue().trim();
				
				if( state.equalsIgnoreCase("active") ) {
					prepayment.setPrepaymentState(PrepaymentState.PAID);
				}
				else if( state.equalsIgnoreCase("payment-failed") ) {
					prepayment.setPrepaymentState(PrepaymentState.REJECTED);
				}
				else if( state.equalsIgnoreCase("retired") ) {
					prepayment.setPrepaymentState(PrepaymentState.RETIRED);
				}
				else {
					prepayment.setPrepaymentState(PrepaymentState.PENDING);
				}
			}
      // Valid values: Linux/UNIX | Linux/UNIX (Amazon VPC) | Windows | Windows (Amazon VPC)
      else if( name.equals("productDescription") ) {
        String productDesc = attr.getFirstChild().getNodeValue().trim();
        if( productDesc.toLowerCase().contains( "linux" ) ) {
          prepayment.setPlatform( Platform.UNIX );
        } else if ( productDesc.toLowerCase().contains( "windows" ) ) {
          prepayment.setPlatform( Platform.WINDOWS );
        }
        if ( productDesc.toLowerCase().contains( "vpc" ) ) {
          prepayment.setForVlan( true );
        }
      }
      else if( name.equals("recurringCharges") ) {
        NodeList chargeNodes = attr.getChildNodes();
        boolean hrly = false;
        for( int ff=0; ff<chargeNodes.getLength(); ff++ ) {
          Node chargeAttr = chargeNodes.item(ff);
          String subName = chargeAttr.getNodeName();
          if( subName.equals("item") ) {
            NodeList itemNodes = chargeAttr.getChildNodes();
            for( int xx=0; xx<itemNodes.getLength(); xx++ ) {
              Node itemAttr = itemNodes.item(xx);
              String itemName = itemAttr.getNodeName();
              if( itemName.equals("frequency") ) {
                String frequency = itemAttr.getFirstChild().getNodeValue().trim();
                if( frequency.equalsIgnoreCase( "hourly" ) ) {
                  hrly = true;
                }
              }
              if( itemName.equals("amount") ) {
                String amount = itemAttr.getFirstChild().getNodeValue().trim();
                if( hrly ) {
                  prepayment.setHourlyFee( Double.valueOf( amount ) );
                }
              }
            }
          }
        }
      }
		}
		prepayment.setCurrencyCode("USD");
		prepayment.setSoftware(null);
		return prepayment;
	}
}
