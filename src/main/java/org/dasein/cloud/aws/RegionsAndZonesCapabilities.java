package org.dasein.cloud.aws;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.dc.DataCenterCapabilities;

import javax.annotation.Nonnull;
import java.util.Locale;

/**
 * User: daniellemayne
 * Date: 07/07/2014
 * Time: 09:10
 */
public class RegionsAndZonesCapabilities extends AbstractCapabilities<AWSCloud> implements DataCenterCapabilities{
    public RegionsAndZonesCapabilities(@Nonnull AWSCloud provider) {
        super(provider);
    }
    @Override
    public String getProviderTermForDataCenter(Locale locale) {
        return "zone";
    }

    @Override
    public String getProviderTermForRegion(Locale locale) {
        return "region";
    }

    @Override
    public boolean supportsResourcePools() {
        return false;
    }
}
