package com.netcracker.profiler.fetch;

import com.netcracker.profiler.dom.ProfiledTreeStreamVisitor;
import com.netcracker.profiler.threaddump.parser.MethodThreadLineInfo;
import com.netcracker.profiler.threaddump.parser.ThreadInfo;

import org.openjdk.jmc.common.IMCType;
import org.openjdk.jmc.common.item.IItem;
import org.openjdk.jmc.common.item.IItemFilter;
import org.openjdk.jmc.common.item.IMemberAccessor;
import org.openjdk.jmc.common.item.IType;
import org.openjdk.jmc.common.unit.IQuantity;
import org.openjdk.jmc.flightrecorder.jdk.JdkAttributes;
import org.openjdk.jmc.flightrecorder.jdk.JdkFilters;
import org.openjdk.jmc.flightrecorder.jdk.JdkTypeIDs;

public class FetchJFRAllocations extends FetchJFRDump {
    protected IMemberAccessor<IQuantity, IItem> allocationSize;
    protected IMemberAccessor<IQuantity, IItem> allocationSizeInTlab;
    protected IMemberAccessor<IMCType, IItem> allocationClass;

    public FetchJFRAllocations(ProfiledTreeStreamVisitor sv, String jfrFileName) {
        super(sv, jfrFileName);
    }

    @Override
    protected IItemFilter getIItemFilter() {
        return JdkFilters.ALLOC_ALL;
    }

    @Override
    protected void onNextItemType(IType<IItem> itemType) {
        super.onNextItemType(itemType);
        allocationSize = JdkAttributes.ALLOCATION_SIZE.getAccessor(itemType);
        allocationSizeInTlab = JdkAttributes.TLAB_SIZE.getAccessor(itemType);
        allocationClass = JdkAttributes.ALLOCATION_CLASS.getAccessor(itemType);
    }

    @Override
    protected void addStackTrace(IItem event, ThreadInfo threadinfo) {
        MethodThreadLineInfo allocatedObject = new MethodThreadLineInfo();
        boolean inNewTlab = JdkTypeIDs.ALLOC_INSIDE_TLAB.equals(event.getType().getIdentifier());

        threadinfo.value = inNewTlab ? allocationSizeInTlab.getMember(event).longValue()
                                        : allocationSize.getMember(event).longValue();

        allocatedObject.methodName = inNewTlab ? "<allocate>" : "<allocate outside tlab>";
        allocatedObject.setClassName(allocationClass.getMember(event).getFullName());
        threadinfo.addThreadLine(allocatedObject);

        super.addStackTrace(event, threadinfo);
    }
}
