package com.raysurf.client.consumer.response;

public class UnregisterResponse {
    private UnregisterResponse.Status status;
    private String errMsg;

    public UnregisterResponse(UnregisterResponse.Status status) {
        this.status = status;
    }

    public UnregisterResponse(UnregisterResponse.Status status, String errMsg) {
        this.status = status;
        this.errMsg = errMsg;
    }

    public UnregisterResponse.Status getStatus() {
        return this.status;
    }

    public void setStatus(UnregisterResponse.Status status) {
        this.status = status;
    }

    public String getErrMsg() {
        return this.errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public boolean isOK() {
        return this.status == UnregisterResponse.Status.OK;
    }

    public String toString() {
        return "UnregisterResponse [status=" + this.status + ", errMsg=" + this.errMsg + "]";
    }

    public static enum Status {
        SERVER_ERROR,
        REQUEST_PARAM_ILLEGAL,
        SERVER_BUSY,
        OK;

        private Status() {
        }
    }
}