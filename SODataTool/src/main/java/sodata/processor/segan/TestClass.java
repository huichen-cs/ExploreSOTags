package sodata.processor.segan;

import sodata.database.DbTagSelectorUtils;
import sodata.database.DbUtilsBase;

public class TestClass {
    public static void main(String[] args) {
        doTest(DbTagSelectorUtils.class);
    }
    
    private static void doTest(Class<? extends DbUtilsBase> DbUtilsClass) {
        System.out.println(DbUtilsClass.getName());
        System.out.println(DbUtilsClass.getSimpleName());
        System.out.println(DbUtilsClass.getCanonicalName());
        System.out.println(DbUtilsClass.getTypeName());
    }
}
