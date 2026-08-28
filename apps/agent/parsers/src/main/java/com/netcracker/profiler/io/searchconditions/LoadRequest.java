package com.netcracker.profiler.io.searchconditions;

import java.util.Date;

public class LoadRequest {
    private String podName;
    private Date dateFrom;
    private Date dateTo;

    public LoadRequest(String podName, Date dateFrom, Date dateTo) {
        this.podName = podName;
        this.dateFrom = dateFrom;
        this.dateTo = dateTo;
    }

    public String getPodName() {
        return podName;
    }

    public void setPodName(String podName) {
        this.podName = podName;
    }

    public Date getDateFrom() {
        return dateFrom;
    }

    public void setDateFrom(Date dateFrom) {
        this.dateFrom = dateFrom;
    }

    public Date getDateTo() {
        return dateTo;
    }

    public void setDateTo(Date dateTo) {
        this.dateTo = dateTo;
    }
}
