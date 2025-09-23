package com.netcracker.profiler.tools;

import java.util.ArrayList;
import java.util.regex.Pattern;

public class ListOfRegExps {

    public static boolean equals(ArrayList<Pattern> a, ArrayList<Pattern> b){
        if (a == null)
            return b == null;
        if (b == null) return false;
        if (a.size() != b.size()) return false;
        for (int i = 0, aSize = a.size(); i < aSize; i++)
            if (!a.get(i).pattern().equals(b.get(i).pattern()))
                return false;
        return true;
    }

    public static int hashCode(ArrayList<Pattern> a){
        if (a == null)
            return 0;
        int result = 0;
        for (int i = 0, aSize = a.size(); i < aSize; i++)
            result = result*31 + a.get(i).hashCode();
        return result;
    }
}
