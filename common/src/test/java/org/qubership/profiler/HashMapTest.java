package org.qubership.profiler;


import java.lang.reflect.Field;
import java.util.BitSet;
import java.util.HashMap;

public class HashMapTest {

    private static int oldHash(int h) {
        h += ~(h << 9);
        h ^=  (h >>> 14);
        h +=  (h << 4);
        h ^=  (h >>> 10);
        return h;
    }

    private static int newHash(int h) {
        // This function ensures that hashCodes that differ only by
        // constant multiples at each bit position have a bounded
        // number of collisions (approximately 8 at default load factor).
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }

    public void test(int size) throws NoSuchFieldException, IllegalAccessException {
        final Class<? extends HashMap> aClass = HashMap.class;
        final Field field = aClass.getDeclaredField("table");
        field.setAccessible(true);

        HashMap<String, Boolean> hs = new HashMap<String, Boolean>((int) (size/0.75f),0.75f);
        int resizes = 0;
        int oldSize = ((Object[])field.get(hs)).length;
        BitSet bits = new BitSet(oldSize);
        int collisions = 0;

        for(int i=0; i<size; i++){
            final String s = String.valueOf( i);
            final int idx = (int) (newHash(s.hashCode())&(oldSize-1));
            if (bits.get(idx))
                collisions++;
            else
                bits.set(idx);
            hs.put(s, true);
            int newSize = ((Object[])field.get(hs)).length;
            if (newSize==oldSize) continue;
            resizes++;
            oldSize=newSize;
        }
//        if (resizes==0) return;
        System.out.println(size + " -> " + oldSize+" (resizes: "+resizes+", collisions: "+collisions+", card: "+bits.cardinality()+", "+(size*1f/bits.cardinality())+")");
    }

    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
        HashMapTest t = new HashMapTest();
        for(int i=0; i<1000; i++){
        t.test(i);
        }
    }
}
