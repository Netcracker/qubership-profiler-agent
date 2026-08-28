package com.netcracker.profiler.formatters.title;

public class ProfilerTitleBuilder implements ProfilerTitle {
    private StringBuilder htmlTitle = new StringBuilder();
    private StringBuilder textTitle = new StringBuilder();
    private boolean isDefault;

    public ProfilerTitleBuilder() {}

    public ProfilerTitleBuilder(String s) {
        htmlTitle.append(s);
        textTitle.append(s);
    }

    public String getHtml() {
        return htmlTitle.toString().trim();
    }

    public String getText() {
        return textTitle.toString().trim();
    }

    public boolean isDefault() {
        return isDefault;
    }

    public void setDefault(boolean isDefault) {
        this.isDefault = isDefault;
    }

    public ProfilerTitleBuilder append(Object text) {
        textTitle.append(text);
        htmlTitle.append(text);
        return this;
    }

    public ProfilerTitleBuilder appendHtml(String html) {
        htmlTitle.append(html);
        return this;
    }

    public ProfilerTitleBuilder deleteLastChars(int cnt) {
        textTitle.delete(textTitle.length()-cnt, textTitle.length());
        htmlTitle.delete(htmlTitle.length()-cnt, htmlTitle.length());
        return this;
    }

}
