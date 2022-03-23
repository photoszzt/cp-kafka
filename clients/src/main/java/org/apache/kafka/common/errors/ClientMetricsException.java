package org.apache.kafka.common.errors;

import org.apache.kafka.common.protocol.Errors;

public class ClientMetricsException  extends ApiException {
    private static final long serialVersionUID = 1;
    private Errors errorCode;
    public Errors getErrorCode() { return this.errorCode; }
    public ClientMetricsException(String s, Errors error) {
        super(s);
        this.errorCode = error;
    }
}
