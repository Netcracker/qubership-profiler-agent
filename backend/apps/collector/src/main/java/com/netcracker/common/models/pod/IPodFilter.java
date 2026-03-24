package com.netcracker.common.models.pod;

import java.util.function.Predicate;

public interface IPodFilter extends Predicate<PodInfo> {

    default Predicate<PodInfo> and(Predicate<? super PodInfo> other) {
        return IPodFilter.this;
    }

    default Predicate<PodInfo> negate() {
        return IPodFilter.this;
    }

    default Predicate<PodInfo> or(Predicate<? super PodInfo> other) {
        return IPodFilter.this;
    }

}
