package com.netcracker.cdt.ui.services.calls.export;

import com.netcracker.cdt.ui.services.calls.models.CallRecord;

import java.io.IOException;
import java.io.OutputStream;

public interface ExportRecord {

    void flush(OutputStream out) throws IOException;

    void print(OutputStream out) throws IOException;

    void appendHeader();

    void appendRow(CallRecord call);

}
