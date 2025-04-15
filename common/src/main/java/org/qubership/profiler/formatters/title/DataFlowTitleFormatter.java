package org.qubership.profiler.formatters.title;

import static org.qubership.profiler.formatters.title.TitleCommonTools.getParameterValues;

import org.qubership.profiler.agent.ParameterInfo;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class DataFlowTitleFormatter extends AbstractTitleFormatter {
    @Override
    public ProfilerTitle formatTitle(String classMethod, Map<String, Integer> tagToIdMap, Object params, List<ParameterInfo> defaultListParams) {
        ProfilerTitleBuilder title = new ProfilerTitleBuilder();
        Collection<String> dataflowSessionParameters = getParameterValues(tagToIdMap, params, "dataflow.session");
        if(dataflowSessionParameters.isEmpty()) {
            title.append(classMethod).setDefault(true);
            return title;
        }
        title.append("DataFlow: ");
        for(String s : dataflowSessionParameters) {
            formatDataFlowSession(s, title);
            title.append(", ");
        }
        title.deleteLastChars(2);
        return title;
    }

    @Override
    public ProfilerTitle formatCommonTitle(String classMethod, Map<String, Integer> tagToIdMap, Map<Integer, List<String>> params, Map<String, Object> formatContext) {
        ProfilerTitleBuilder title = new ProfilerTitleBuilder();
        Collection<String> dataflowSessionParameters = getParameterValues(tagToIdMap, params, "dataflow.session");
        if(dataflowSessionParameters.isEmpty()) {
            title.append(classMethod).setDefault(true);
            return title;
        }
        title.append("DataFlow: ");
        for(String s : dataflowSessionParameters) {
            try {
                DFSession session = DFSession.parse(s);
                if (session == null) continue;
                if (session.n != null) {
                    title.append(session.n);
                }
            } catch (Exception e) {
                //DoNothing
            }
            title.append(", ");
        }
        title.deleteLastChars(2);
        return title;
    }

    private static void formatDataFlowSession(String sessionString, ProfilerTitleBuilder title) {
        try {
            DFSession session = DFSession.parse(sessionString);
            if (session == null) return;
            boolean stealthMode = session.i == null || session.i.length() != 19;
            if (session.n != null) {
                title.appendHtml("<b>").append(session.n).appendHtml("</b>").append(" ");
            }
            if (!stealthMode) {
                title.append("(instance=").append(session.i);
                if (session.c != null) {
                    title.append(", configuration=").append(session.c);
                }
                title.append(")");
            } else if (session.c != null) {
                title.append("(configuration=").append(session.c).append(")");
            }
        } catch (Exception e) {
            //DoNothing
        }
    }

    private static class DFSession {
        String t;
        String i;
        String c;
        String n;

        // Lazy initialization
        private static final JsonFactory jsonFactory = new JsonFactory();

        private static DFSession parse(String params) throws Exception {
            JsonParser parser = jsonFactory.createParser("{" + params + "}");
            DFSession session = new DFSession();
            for (JsonToken token = parser.nextToken(); token != null; token = parser.nextToken()) {
                if (token == JsonToken.VALUE_STRING) {
                    switch (parser.getCurrentName()) {
                        case "t":
                            session.t = parser.getText();
                            break;
                        case "i":
                            session.i = parser.getText();
                            break;
                        case "c":
                            session.c = parser.getText();
                            break;
                        case "n":
                            session.n = parser.getText();
                            break;
                    }
                }
            }
            return session;
        }
    }
}
