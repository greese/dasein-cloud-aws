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

/**
 * Created by stas on 14/04/2015.
 */
public class VolumeProduct {
    private String id;
    private String name;
    private String description;
    private String type;
    private long minSize;
    private long maxSize;
    private int minIops;
    private int maxIops;
    private int iopsToGb;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getMinSize() {
        return minSize;
    }

    public void setMinSize(long minSize) {
        this.minSize = minSize;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    public int getMinIops() {
        return minIops;
    }

    public void setMinIops(int minIops) {
        this.minIops = minIops;
    }

    public int getMaxIops() {
        return maxIops;
    }

    public void setMaxIops(int maxIops) {
        this.maxIops = maxIops;
    }

    public int getIopsToGb() {
        return iopsToGb;
    }

    public void setIopsToGb(int iopsToGb) {
        this.iopsToGb = iopsToGb;
    }
}
