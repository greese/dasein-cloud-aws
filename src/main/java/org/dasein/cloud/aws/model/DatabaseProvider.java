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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.dasein.cloud.InternalException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
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
public class DatabaseProvider {
    String cloud;
    String provider;
    Date created = new Date();
    List<DatabaseEngine> engines;
    private List<DatabaseProductDefinition> productDefinitions;

    public static DatabaseProvider fromFile(String filename, String providerId) throws InternalException {
        try {
            ObjectMapper om = new ObjectMapper();
            URL url = om.getClass().getResource(filename);
            DatabaseProvider[] providers = om.readValue(url, DatabaseProvider[].class);
            for( DatabaseProvider provider : providers ) {
                if( provider.provider.equalsIgnoreCase(providerId) ) {
                    return provider;
                }
            }
        } catch( IOException e ) {
            throw new InternalException("Unable to read stream", e);
        }
        throw new InternalException("Unable to find "+providerId+" provider configuration in "+filename);
    }

    public DatabaseProvider() {

    }

    public DatabaseProvider( String cloud, String provider ) {
        this.cloud = cloud;
        this.provider = provider;
    }

    public String getProvider() {
        return provider;
    }

    public String getCloud() {
        return cloud;
    }

    public List<DatabaseEngine> getEngines() {
        return engines;
    }

    public void setEngines( List<DatabaseEngine> engines ) {
        this.engines = engines;
    }

    public Date getCreated() {
        return created;
    }

    public void setProductDefinitions( List<DatabaseProductDefinition> productDefinitions ) {
        this.productDefinitions = productDefinitions;
    }

    public List<DatabaseProductDefinition> getProductDefinitions() {
        return productDefinitions;
    }

    public @Nullable DatabaseProductDefinition findProductDefinition(String productName) {
        for(DatabaseProductDefinition def : productDefinitions ) {
            if( def.getName().equalsIgnoreCase( productName ) ) {
                return def;
            }
        }
        return null;
    }

    public @Nullable DatabaseEngine findEngine(String name) {
        for(DatabaseEngine engine : engines ) {
            if( engine.getName().equalsIgnoreCase(name)) {
                return engine;
            }
        }
        return null;
    }

}
