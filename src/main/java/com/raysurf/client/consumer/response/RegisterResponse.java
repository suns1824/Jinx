package com.raysurf.client.consumer.response;

public class RegisterResponse {
    private RegisterResponse.Status status;
    private String errMsg;
    private long sessionTimeout;
    private boolean needSyncOldOffset;

    public RegisterResponse(RegisterResponse.Status status) {
        this.status = status;
    }

    public RegisterResponse(RegisterResponse.Status status, String errMsg) {
        this.status = status;
        this.errMsg = errMsg;
    }

    public RegisterResponse(long sessionTimeout, boolean needSyncOldOffset) {
        this.status = RegisterResponse.Status.OK;
        this.sessionTimeout = sessionTimeout;
        this.needSyncOldOffset = needSyncOldOffset;
    }

    public long getSessionTimeout() {
        return this.sessionTimeout;
    }

    public void setSessionTimeout(long sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public RegisterResponse.Status getStatus() {
        return this.status;
    }

    public void setStatus(RegisterResponse.Status status) {
        this.status = status;
    }

    public String getErrMsg() {
        return this.errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public boolean isNeedSyncOldOffset() {
        return this.needSyncOldOffset;
    }

    public void setNeedSyncOldOffset(boolean needSyncOldOffset) {
        this.needSyncOldOffset = needSyncOldOffset;
    }

    public boolean isOK() {
        return this.status == RegisterResponse.Status.OK;
    }

    public String toString() {
        return "RegisterResponse [status=" + this.status + ", errMsg=" + this.errMsg + ", sessionTimeout=" + this.sessionTimeout + ", needSyncOldOffset=" + this.needSyncOldOffset + "]";
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
