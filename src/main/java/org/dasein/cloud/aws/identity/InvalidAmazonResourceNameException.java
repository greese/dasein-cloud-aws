package org.dasein.cloud.aws.identity;

/**
 * Exception is thrown in case invalid ARN is provided
 *
 * @author igoonich
 * @since 13.05.2014
 */
public class InvalidAmazonResourceNameException extends Exception {
    private static final long serialVersionUID = -6333761728160596784L;

    private static final String DEFAULT_MESSAGE = "Invalid amazon resource name [%s]";

    private String invalidResourceName;

    public InvalidAmazonResourceNameException(String invalidResourceName) {
        super(String.format(DEFAULT_MESSAGE, invalidResourceName));
        this.invalidResourceName = invalidResourceName;
    }

    public InvalidAmazonResourceNameException(String customMessage, String invalidResourceName) {
        super(customMessage);
        this.invalidResourceName = invalidResourceName;
    }

    public String getInvalidResourceName() {
        return invalidResourceName;
    }
}
