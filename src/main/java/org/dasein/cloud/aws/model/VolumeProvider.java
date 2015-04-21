/*
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

package org.dasein.cloud.aws.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.dasein.cloud.InternalException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Description
 * <p>Created by Stas Maksimov: 14/07/2014 16:19</p>
 *

 * @author Stas Maksimov
 * @version 2014.07 initial version
 * @since 2014.07
 */
public class VolumeProvider {
    private String cloud;
    private String provider;
    private Date created = new Date();
    private List<VolumeProduct> products;
    private List<VolumeRegion> regions;

    public static VolumeProvider fromFile(String filename, String providerId) throws InternalException {
        try {
            ObjectMapper om = new ObjectMapper();
            URL url = om.getClass().getResource(filename);
            VolumeProvider[] providers = om.readValue(url, VolumeProvider[].class);
            for( VolumeProvider provider : providers ) {
                if( provider.provider.equalsIgnoreCase(providerId) ) {
                    return provider;
                }
            }
        } catch( IOException e ) {
            throw new InternalException("Unable to read stream", e);
        }
        throw new InternalException("Unable to find "+providerId+" provider configuration in "+filename);
    }

    public VolumeProvider() {

    }

    public VolumeProvider(String cloud, String provider) {
        this.cloud = cloud;
        this.provider = provider;
    }

    public String getProvider() {
        return provider;
    }

    public String getCloud() {
        return cloud;
    }

    public List<VolumeProduct> getProducts() {
        if( products == null ) {
            products = new ArrayList<VolumeProduct>();
        }
        return products;
    }

    public void setProducts( List<VolumeProduct> products ) {
        this.products = products;
    }

    public Date getCreated() {
        return created;
    }

    public void setRegions( List<VolumeRegion> regions ) {
        this.regions = regions;
    }

    public List<VolumeRegion> getRegions() {
        if( regions == null ) {
            regions = new ArrayList<VolumeRegion>();
        }
        return regions;
    }

    public @Nullable VolumeRegion findRegion(String regionName) {
        for(VolumeRegion region : regions ) {
            if( region.getName().equalsIgnoreCase( regionName ) ) {
                return region;
            }
        }
        return null;
    }

    public @Nullable VolumePrice findProductPrice(String regionId, String productId) {
        VolumeRegion region = findRegion(regionId);
        if( region != null ) {
            for( VolumePrice price : region.getPrices() ) {
                if( productId.equalsIgnoreCase(price.getId()) ) {
                    return price;
                }
            }
        }
        return null;
    }

    public @Nullable VolumeProduct findProduct(String name) {
        for(VolumeProduct product : products ) {
            if( product.getName().equalsIgnoreCase(name)) {
                return product;
            }
        }
        return null;
    }

}
