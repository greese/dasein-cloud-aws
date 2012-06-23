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

package org.dasein.cloud.aws.network;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.VPN;
import org.dasein.cloud.network.VPNGateway;
import org.dasein.cloud.network.VPNProtocol;
import org.dasein.cloud.network.VPNSupport;

import javax.annotation.Nonnull;

public class VPCGateway implements VPNSupport {

    @Override
    public void attachToVLAN(String providerVpnId, String providerVlanId) throws CloudException, InternalException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void connectToGateway(String providerVpnId, String toGatewayId) throws CloudException, InternalException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public VPN createVPN(String inProviderDataCenterId, String name, String description, VPNProtocol protocol) throws CloudException, InternalException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public VPNGateway createVPNGateway(String endpoint, String name, String description, VPNProtocol protocol, String bgpAsn) throws CloudException, InternalException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void deleteVPN(String providerVpnId) throws CloudException, InternalException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void deleteVPNGateway(String providerGatewayI) throws CloudException, InternalException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void detachFromVLAN(String providerVpnId, String providerVlanId) throws CloudException, InternalException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void disconnectFromGateway(String providerGatewayId) throws CloudException, InternalException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public VPN getVPN(String providerVpnId) throws CloudException, InternalException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Iterable<VPN> listVPNs() throws CloudException, InternalException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];     // TODO: implement me
    }
}
