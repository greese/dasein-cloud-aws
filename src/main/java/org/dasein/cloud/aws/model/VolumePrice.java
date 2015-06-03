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
public class VolumePrice {
    private String id;
    private float monthly;
    private float iops;

    public float getIops() {
        return iops;
    }

    public void setIops(float iops) {
        this.iops = iops;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public float getMonthly() {
        return monthly;
    }

    public void setMonthly(float monthly) {
        this.monthly = monthly;
    }

}
