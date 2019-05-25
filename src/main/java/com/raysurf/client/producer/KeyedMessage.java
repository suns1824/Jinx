package com.raysurf.client.producer;

import java.io.Serializable;

public class KeyedMessage implements Serializable {
    private static final long serialVersionUID = -6811131784871323023L;
    private String key;
    private Object message;

    public KeyedMessage() {

    }

    public KeyedMessage(String key, Object message) {
        this.key = key;
        this.message = message;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Object getMessage() {
        return message;
    }

    public void setMessage(Object message) {
        this.message = message;
    }

    public int hashCode() {
        int prime = 1;
        int result = 31 * prime + (this.key == null ? 0 : this.key.hashCode());
        result = 31 * result + (this.message == null ? 0 : this.message.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (this.getClass() != obj.getClass()) {
            return false;
        } else {
            KeyedMessage other = (KeyedMessage) obj;
            if (this.key == null) {
                if (other.key != null) {
                    return false;
                }
            } else if (!this.key.equals(other.key)) {
                return false;
            }
            if (this.message == null) {
                if (other.message != null) {
                    return false;
                }
            } else if (!this.message.equals(other.message)) {
                return false;
            }
            return true;
        }
    }
}
