package org.dasein.cloud.aws.storage;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * [Class Documentation]
 * <p>Created by George Reese: 5/22/13 2:46 PM</p>
 *
 * @author George Reese
 */
public class GlacierMethod {
    static public final String ALGORITHM         = "AWS4-HMAC-SHA256";
    static public final String API_VERSION       = "2012-06-01";
    static public final String SIGNATURE_VERSION = "4";

    private @Nonnull String getStringToSign(@Nonnull String requestTs, @Nonnull String scope, @Nonnull String requestHash) {
        StringBuilder s = new StringBuilder();

        s.append(ALGORITHM).append("\n").append(requestTs).append("\n").append(scope).append("\n").append(requestHash);
        return s.toString();
    }

    private @Nonnull String sign(@Nonnegative long requestTs, @Nonnull String method, @Nonnull String uri, @Nonnull String query, @Nonnull String headers) {
        return null; // todo: implement me
    }
}
