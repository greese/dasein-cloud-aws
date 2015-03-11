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

package org.dasein.cloud.aws;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/**
 * An enumeration-like object for different implementations of the EC2 API. This cannot be an enum as it allows
 * for arbitrary values. Furthermore, values are matched in an case-insensitive fashion.
 * <p>Created by George Reese: 9/17/12 12:29 PM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class EC2Provider implements Comparable<EC2Provider> {
    static private final String AWS_NAME          = "AWS";
    static private final String ENSTRATUS_NAME    = "Enstratius";
    static private final String EUCALYPTUS_NAME   = "Eucalyptus";
    static private final String OPENSTACK_NAME    = "OpenStack";
    static private final String OTHER_NAME        = "Other";
    static private final String STORAGE_NAME      = "Storage";

    static private final String[] OPENSTACK_PROVIDERS = { "CloudScaling "};
    static private final String[] STORAGE_PROVIDERS   = { "Google", "Riak" };

    static public final EC2Provider AWS          = new EC2Provider(AWS_NAME);
    static public final EC2Provider ENSTRATUS    = new EC2Provider(ENSTRATUS_NAME);
    static public final EC2Provider OPENSTACK    = new EC2Provider(OPENSTACK_NAME);
    static public final EC2Provider EUCALYPTUS   = new EC2Provider(EUCALYPTUS_NAME);
    static public final EC2Provider OTHER        = new EC2Provider(OTHER_NAME);
    static public final EC2Provider STORAGE      = new EC2Provider(STORAGE_NAME);

    static private Set<EC2Provider> providers;

    static public @Nonnull EC2Provider valueOf(@Nonnull String providerName) {
        if( providerName.equalsIgnoreCase("Amazon") ) {
            return AWS;
        }
        // cannot statically initialize the list of providers
        if( providers == null ) {
            TreeSet<EC2Provider> tmp = new TreeSet<EC2Provider>();

            Collections.addAll(tmp, AWS, ENSTRATUS, EUCALYPTUS, OPENSTACK, STORAGE);
            providers = Collections.unmodifiableSet(tmp);
        }
        for( EC2Provider provider : providers ) {
            if( provider.providerName.equalsIgnoreCase(providerName) ) {
                return provider;
            }
        }
        for( String name : OPENSTACK_PROVIDERS ) {
            if( name.equalsIgnoreCase(providerName) ) {
                return OPENSTACK;
            }
        }
        for( String name : STORAGE_PROVIDERS ) {
            if( name.equalsIgnoreCase(providerName) ) {
                return STORAGE;
            }
        }
        return OTHER;
    }

    static public @Nonnull Set<EC2Provider> values() {
        return providers;
    }

    private String providerName;

    private EC2Provider(String provider) {
        this.providerName = provider;
    }

    @Override
    public int compareTo(@Nullable EC2Provider other) {
        if( other == null ) {
            return -1;
        }
        return providerName.compareTo(other.providerName);
    }

    @Override
    public boolean equals(Object ob) {
        return ob != null && (ob == this || getClass().getName().equals(ob.getClass().getName()) && providerName.equals(((EC2Provider) ob).providerName));
    }

    public @Nonnull String getName() {
        return providerName;
    }

    @Override
    public @Nonnull String toString() {
        return providerName;
    }

    public boolean isAWS() {
        return providerName.equals(AWS_NAME);
    }

    public boolean isEnStratus() {
        return providerName.equals(ENSTRATUS_NAME);
    }

    public boolean isEucalyptus() {
        return providerName.equals(EUCALYPTUS_NAME);
    }

    public boolean isOpenStack() {
        return providerName.equals(OPENSTACK_NAME);
    }

    public boolean isOther() {
        return providerName.equals(OTHER_NAME);
    }

    public boolean isStorage() {
        return providerName.equals(STORAGE_NAME);
    }
}
