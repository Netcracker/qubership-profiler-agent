package org.qubership.profiler.configuration;

import org.qubership.profiler.agent.ParamTypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

public class ParameterInfoDto {
    private static final Logger logger = LoggerFactory.getLogger(ParameterInfoDto.class);
    public final String name;
    public boolean big;
    public boolean deduplicate;
    public boolean index;
    public boolean list;
    public String signatureFunction;
    public int order = 1000;
    public int combined;

    public ParameterInfoDto(String name) {
        this.name = name;

        String lowerName = name.toLowerCase();
        if (lowerName.indexOf("sql") > -1 || lowerName.indexOf("xpath") > -1)
            big = deduplicate = true;
        else if (lowerName.indexOf("xml") > -1)
            big = true;

        update();
    }

    public ParameterInfoDto(Element e) {
        this(e.getAttribute("name"));
    }

    public void parse(Element e) {
        final String isBig = e.getAttribute("big");
        if (isBig.length() > 0)
            big = Boolean.valueOf(isBig);

        final String shouldIndex = e.getAttribute("index");
        if (shouldIndex.length() > 0)
            index = Boolean.valueOf(shouldIndex);

        final String shouldList = e.getAttribute("list");
        list = shouldList.length() == 0 || Boolean.valueOf(shouldList);

        final String dedup = e.getAttribute("deduplicate");
        if (dedup.length() > 0) {
            deduplicate = Boolean.valueOf(dedup);
            if (deduplicate)
                big = true;
        }

        final String orderNumber = e.getAttribute("order");
        if (orderNumber.length() == 0)
            order = 100;
        else
            try {
                order = Integer.valueOf(orderNumber);
            } catch (NumberFormatException nfe) {
                logger.error("[PROFILER] Unable to parse order attribute " + orderNumber, nfe);
            }

        final String signature = e.getAttribute("signature");
        if (signature.length() > 0)
            signatureFunction = signature;

        update();
    }

    public ParameterInfoDto index(boolean shouldIndex) {
        index = shouldIndex;
        update();
        return this;
    }

    public ParameterInfoDto big(boolean isBig) {
        big = isBig;
        update();
        return this;
    }

    public ParameterInfoDto deduplicate(boolean dedup) {
        deduplicate = dedup;
        if (deduplicate)
            big = true;
        update();
        return this;
    }

    public ParameterInfoDto list(boolean shouldList) {
        list = shouldList;
        update();
        return this;
    }

    public ParameterInfoDto order(int orderNumber) {
        order = orderNumber;
        update();
        return this;
    }

    public ParameterInfoDto signature(String signature) {
        signatureFunction = signature;
        return this;
    }

    public void update() {
        if (big)
            combined = deduplicate ? ParamTypes.PARAM_BIG_DEDUP : ParamTypes.PARAM_BIG;
        else
            combined = index ? ParamTypes.PARAM_INDEX : ParamTypes.PARAM_INLINE;
    }

    public ParameterInfoDto paramType(int type) {
        switch(type){
            case ParamTypes.PARAM_BIG:
                big = true;
                deduplicate = false;
                index = false;
                break;
            case ParamTypes.PARAM_BIG_DEDUP:
                big = true;
                deduplicate = true;
                index = false;
                break;
            case ParamTypes.PARAM_INDEX:
                big = false;
                deduplicate = false;
                index = true;
                break;
            case ParamTypes.PARAM_INLINE:
                big = false;
                deduplicate = false;
                index = true;
                break;
        }
        combined = type;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ParameterInfoDto)) return false;

        ParameterInfoDto that = (ParameterInfoDto) o;

        if (big != that.big) return false;
        if (combined != that.combined) return false;
        if (deduplicate != that.deduplicate) return false;
        if (index != that.index) return false;
        if (list != that.list) return false;
        if (order != that.order) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (signatureFunction != null ? !signatureFunction.equals(that.signatureFunction) : that.signatureFunction != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (big ? 1 : 0);
        result = 31 * result + (deduplicate ? 1 : 0);
        result = 31 * result + (index ? 1 : 0);
        result = 31 * result + (list ? 1 : 0);
        result = 31 * result + order;
        result = 31 * result + combined;
        result = 31 * result + (signatureFunction != null? signatureFunction.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ParameterInfoDto{" +
                "name='" + name + '\'' +
                ", big=" + big +
                ", deduplicate=" + deduplicate +
                ", index=" + index +
                ", list=" + list +
                ", signatureFunction='" + signatureFunction + '\'' +
                ", order=" + order +
                ", combined=" + combined +
                '}';
    }
}
