package com.raysurf.client.consumer.response;

import com.raysurf.client.consumer.KafkaMessage;
import java.util.Map;

public class FetchResponse {
    private KafkaMessage message;
    private FetchResponse.Status status;
    private String errMsg;
    private int invokeTimes;
    private Map<Integer, Long> invokeElapsed;

    public FetchResponse(FetchResponse.Status status) {
        this.status = status;
    }

    public FetchResponse(FetchResponse.Status status, String errMsg) {
        this.status = status;
        this.errMsg = errMsg;
    }

    public FetchResponse(KafkaMessage message) {
        this.status = FetchResponse.Status.OK;
        this.message = message;
    }

    public FetchResponse.Status getStatus() {
        return this.status;
    }

    public void setStatus(FetchResponse.Status status) {
        this.status = status;
    }

    public String getErrMsg() {
        return this.errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public KafkaMessage getMessage() {
        return this.message;
    }

    public void setMessage(KafkaMessage message) {
        this.message = message;
    }

    public boolean isOK() {
        return this.status == FetchResponse.Status.OK;
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
        return "FetchResponse [message=" + this.message + ", status=" + this.status + ", errMsg=" + this.errMsg + "]";
    }

    public static enum Status {
        SERVER_ERROR,
        REQUEST_PARAM_ILLEGAL,
        OK,
        DEFER,
        LOCK_CONFLICT;

        private Status() {
        }
    }
}

