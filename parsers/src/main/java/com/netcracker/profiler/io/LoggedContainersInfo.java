package com.netcracker.profiler.io;

import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Scope("prototype")
@Profile("filestorage")
public class LoggedContainersInfo {
    public List<String[]> listPodDetails(){
        return null;
    }
}
