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
 * <p>Created by Stas Maksimov: 16/07/2014 18:25</p>
 *
 * @author Stas Maksimov
 * @version 2014.03 initial version
 * @since 2014.03
 */
public class DatabaseProductDefinition {
    String name;
    int vCpus;
    double memory;
    boolean piopsOptimized;
    String networkPerformance;

    public String getName() {
        return name;
    }

    public void setName( String name ) {
        this.name = name;
    }

    public int getvCpus() {
        return vCpus;
    }

    public void setvCpus( int vCpus ) {
        this.vCpus = vCpus;
    }

    public double getMemory() {
        return memory;
    }

    public void setMemory( double memory ) {
        this.memory = memory;
    }

    public boolean isPiopsOptimized() {
        return piopsOptimized;
    }

    public void setPiopsOptimized( boolean piopsOptimized ) {
        this.piopsOptimized = piopsOptimized;
    }

    public String getNetworkPerformance() {
        return networkPerformance;
    }

    public void setNetworkPerformance( String networkPerformance ) {
        this.networkPerformance = networkPerformance;
    }
}
