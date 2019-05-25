package com.raysurf.client.consumer.response;

public class SyncResponse {
    private SyncResponse.Status status;
    private String errMsg;

    public SyncResponse(SyncResponse.Status status) {
        this.status = status;
    }

    public SyncResponse(SyncResponse.Status status, String errMsg) {
        this.status = status;
        this.errMsg = errMsg;
    }

    public SyncResponse.Status getStatus() {
        return this.status;
    }

    public void setStatus(SyncResponse.Status status) {
        this.status = status;
    }

    public String getErrMsg() {
        return this.errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public String toString() {
        return "SyncResponse [status=" + this.status + ", errMsg=" + this.errMsg + "]";
    }

    public static enum Status {
        SERVER_ERROR,
        REQUEST_PARAM_ILLEGAL,
        OK,
        ALREADY_EXISTS;

        private Status() {
        }
    }
}
