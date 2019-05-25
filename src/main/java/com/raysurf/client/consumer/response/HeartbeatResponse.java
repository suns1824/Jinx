package com.raysurf.client.consumer.response;

public class HeartbeatResponse {
    private HeartbeatResponse.Status status;
    private String errMsg;

    public HeartbeatResponse(HeartbeatResponse.Status status) {
        this.status = status;
    }

    public HeartbeatResponse(HeartbeatResponse.Status status, String errMsg) {
        this.status = status;
        this.errMsg = errMsg;
    }

    public HeartbeatResponse.Status getStatus() {
        return this.status;
    }

    public void setStatus(HeartbeatResponse.Status status) {
        this.status = status;
    }

    public String getErrMsg() {
        return this.errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public boolean isOK() {
        return this.status == HeartbeatResponse.Status.OK;
    }

    public String toString() {
        return "HeartbeatResponse [status=" + this.status + ", errMsg=" + this.errMsg + "]";
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
