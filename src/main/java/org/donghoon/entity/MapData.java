package org.donghoon.entity;

import lombok.Builder;
import org.joda.time.DateTime;

import java.util.Date;

@Builder
public class MapData {

    private Date logtime;
    private String timezone;
    private Integer logid;
    private Integer seq;
    private String info;
    private String plogdate;

    public MapData(Date logtime, String timezone, Integer logid, Integer seq, String info, String plogdate) {
        this.logtime = logtime;
        this.timezone = timezone;
        this.logid = logid;
        this.seq = seq;
        this.info = info;
        this.plogdate = plogdate;
    }

    public Date getLogtime() {
        return logtime;
    }

    public void setLogtime(Date logtime) {
        this.logtime = logtime;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public Integer getLogid() {
        return logid;
    }

    public void setLogid(Integer logid) {
        this.logid = logid;
    }

    public Integer getSeq() {
        return seq;
    }

    public void setSeq(Integer seq) {
        this.seq = seq;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public String getPlogdate() {
        return plogdate;
    }

    public void setPlogdate(String plogdate) {
        this.plogdate = plogdate;
    }

    @Override
    public String toString() {
        return "MapData{" +
                "logtime=" + logtime +
                ", timezone='" + timezone + '\'' +
                ", logid=" + logid +
                ", seq=" + seq +
                ", info='" + info + '\'' +
                ", plogdate='" + plogdate + '\'' +
                '}';
    }
}
