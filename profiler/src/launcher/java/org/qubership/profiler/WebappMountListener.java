package org.qubership.profiler;

import org.apache.catalina.*;
import org.apache.catalina.webresources.JarResourceSet;
import org.apache.catalina.webresources.StandardRoot;

import java.util.Arrays;
import java.util.List;

public class WebappMountListener implements LifecycleListener {

    private static final List<String> WEB_RESOURCES =Arrays.asList(
            "/WEB-INF/web.xml",
            "/index.html",
            "/login.html",
            "/tree.html",
            "/single-page/tree.html",
            "/css",
            "/js"
    );

    @Override
    public void lifecycleEvent(LifecycleEvent event) {
        if (event.getType().equals(Lifecycle.BEFORE_START_EVENT)) {
            Context context = (Context) event.getLifecycle();
            WebResourceRoot resources = context.getResources();
            if (resources == null) {
                resources = new StandardRoot(context);
                context.setResources(resources);
            }

            for(String webResource : WEB_RESOURCES) {
                JarResourceSet jrs = new JarResourceSet(resources, webResource, WARLauncher.PATH_TO_WAR_FILE, webResource);
                resources.addJarResources(jrs);
            }
            resources.addJarResources(new JarResourceSet(resources, "/assets", WARLauncher.PATH_TO_WAR_FILE, "/assets"));
            // Tomcat registers JSP handler by default; however, we do not use JSPs
            Container jsp = context.findChild("jsp");
            if (jsp != null) {
                context.removeChild(jsp);
            }
        }
    }

}
