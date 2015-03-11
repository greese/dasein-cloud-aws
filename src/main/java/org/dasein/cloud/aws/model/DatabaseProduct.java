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

package org.dasein.cloud.aws.model;

/**
 * Description
 * <p>Created by Stas Maksimov: 14/07/2014 17:03</p>
 *
 * @author Stas Maksimov
 * @version 2014.07 initial version
 * @since 2014.07
 */
public class DatabaseProduct {
    String  name;
    boolean highAvailability;
    float   hourlyRate;
    float   ioRate;
    float   storageRate;
    int     minStorage;
    String  license;
    String  currency;

    public String getLicense() {
        return license;
    }

    public void setLicense( String license ) {
        this.license = license;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency( String currency ) {
        this.currency = currency;
    }

    public String getName() {
        return name;
    }

    public void setName( String name ) {
        this.name = name;
    }

    public boolean isHighAvailability() {
        return highAvailability;
    }

    public void setHighAvailability( boolean highAvailability ) {
        this.highAvailability = highAvailability;
    }

    public float getHourlyRate() {
        return hourlyRate;
    }

    public void setHourlyRate( float hourlyRate ) {
        this.hourlyRate = hourlyRate;
    }

    public float getIoRate() {
        return ioRate;
    }

    public void setIoRate( float ioRate ) {
        this.ioRate = ioRate;
    }

    public float getStorageRate() {
        return storageRate;
    }

    public void setStorageRate( float storageRate ) {
        this.storageRate = storageRate;
    }

    public int getMinStorage() {
        return minStorage;
    }

    public void setMinStorage( int minStorage ) {
        this.minStorage = minStorage;
    }
}
