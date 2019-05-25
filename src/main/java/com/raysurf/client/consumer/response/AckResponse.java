package com.raysurf.client.consumer.response;

import java.util.Map;

public class AckResponse {
    private AckResponse.Status status;
    private String errMsg;
    private int invokeTimes;
    private Map<Integer, Long> invokeElapsed;

    public AckResponse(AckResponse.Status status) {
        this.status = status;
    }

    public AckResponse(AckResponse.Status status, String errMsg) {
        this.status = status;
        this.errMsg = errMsg;
    }

    public AckResponse.Status getStatus() {
        return this.status;
    }

    public void setStatus(AckResponse.Status status) {
        this.status = status;
    }

    public String getErrMsg() {
        return this.errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public boolean isOK() {
        return this.status == AckResponse.Status.OK;
    }

    public int getInvokeTimes() {
        return this.invokeTimes;
    }

    public void setInvokeTimes(int invokeTimes) {
        this.invokeTimes = invokeTimes;
    }

    public Map<Integer, Long> getInvokeElapsed() {
        return this.invokeElapsed;
    }

    public void setInvokeElapsed(Map<Integer, Long> invokeElapsed) {
        this.invokeElapsed = invokeElapsed;
    }

    public String toString() {
        return "AckResponse [status=" + this.status + ", errMsg=" + this.errMsg + "]";
    }

    public static enum Status {
        SERVER_ERROR,
        REQUEST_PARAM_ILLEGAL,
        SERVER_BUSY,
        OK,
        ALREADY_ACKED,
        DISALLOWED,
        NOT_FOUND,
        RECYCLED;

        private Status() {
        }
    }
}

