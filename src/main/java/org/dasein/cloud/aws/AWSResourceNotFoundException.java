package org.dasein.cloud.aws;

import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;

import javax.annotation.Nonnull;
import java.net.HttpURLConnection;

/**
 * Exception to be used when some AWS resource should exist, but cannot be found for some reason
 *
 * @author igoonich
 * @since 19.03.2014
 */
public class AWSResourceNotFoundException extends CloudException {
    private static final long serialVersionUID = 950720238875342406L;

    public AWSResourceNotFoundException(@Nonnull String msg) {
        super(CloudErrorType.GENERAL, HttpURLConnection.HTTP_NOT_FOUND, "none", msg);
    }

    public AWSResourceNotFoundException(@Nonnull String msg, @Nonnull Throwable cause) {
        super(CloudErrorType.GENERAL, HttpURLConnection.HTTP_NOT_FOUND, "none", msg, cause);
    }
}
