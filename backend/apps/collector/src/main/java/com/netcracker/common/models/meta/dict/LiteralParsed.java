package com.netcracker.common.models.meta.dict;

public record LiteralParsed(String full,
                            String type, // tag, code
                            // for methods
                            String clazz, String method, String file, int line, String signature,
                            // lib
                            String library, String version
                              ) {
    // see tags.small.u.txt

    // TODO: parsing method names
//    var METHOD_REGEX = /^(\S+) ((?:[^(.]+\.)*)([^(.]+)\.([^(.]+)(\([^)]*\)) (\([^)]*\))(?: (\[[^\]]*]))?/
//            return [tag, m[1], m[2], m[3], m[4], m[5], m[6], m[7]];

    @Override
    public String toString() {
        return switch (type) {
            case "code" -> clazz + '.' + method + ':' + line;
            case "tag" -> full;
            default -> full;
        };
    }
}
