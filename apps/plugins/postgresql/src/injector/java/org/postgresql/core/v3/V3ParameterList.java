package org.postgresql.core.v3;

import org.postgresql.core.ParameterList;

public interface V3ParameterList extends ParameterList {
    SimpleParameterList[] getSubparams();
}
