package com.liferay.portal.theme;

import com.netcracker.profiler.agent.CallInfo;
import com.netcracker.profiler.agent.Profiler;

import com.liferay.portal.model.User;

public class ThemeDisplay {
    private void logUser$profiler(User user) {
        if (user == null) {
            return;
        }
        String userName = user.getScreenName();
        if (userName == null) {
            return;
        }

        final CallInfo callInfo = Profiler.getState().callInfo;
        if (userName.equals(callInfo.getNcUser())) {
            return;
        }
        callInfo.setNcUser(userName);
        Profiler.event(userName, "liferay.user");
    }
}
