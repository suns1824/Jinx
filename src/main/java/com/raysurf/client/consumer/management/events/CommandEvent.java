package com.raysurf.client.consumer.management.events;

import java.io.Serializable;

public class CommandEvent implements Serializable {
    private static final long serialVersionUID = -1827705107610584942L;
    public final String topic;
    public final int partition;
    public final String command;
    public final long elapsed;
    public final boolean isFail;

    public CommandEvent(String topic, int partition, String command, long elapsed, boolean isFail) {
        this.topic = topic;
        this.partition = partition;
        this.command = command;
        this.elapsed = elapsed;
        this.isFail = isFail;
    }

    public String toString() {
        return "CommandEvent [topic=" + this.topic + ", partition=" + this.partition + ", command=" + this.command + ", elapsed=" + this.elapsed + ", isFail=" + this.isFail + "]";
    }

    public int hashCode() {
        int result = 1;
        result = 31 * result + (this.command == null ? 0 : this.command.hashCode());
        result = 31 * result + (int)(this.elapsed ^ this.elapsed >>> 32);
        result = 31 * result + (this.isFail ? 1231 : 1237);
        result = 31 * result + this.partition;
        result = 31 * result + (this.topic == null ? 0 : this.topic.hashCode());
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
            CommandEvent other = (CommandEvent)obj;
            if (this.command == null) {
                if (other.command != null) {
                    return false;
                }
            } else if (!this.command.equals(other.command)) {
                return false;
            }

            if (this.elapsed != other.elapsed) {
                return false;
            } else if (this.isFail != other.isFail) {
                return false;
            } else if (this.partition != other.partition) {
                return false;
            } else {
                if (this.topic == null) {
                    if (other.topic != null) {
                        return false;
                    }
                } else if (!this.topic.equals(other.topic)) {
                    return false;
                }

                return true;
            }
        }
    }
}

