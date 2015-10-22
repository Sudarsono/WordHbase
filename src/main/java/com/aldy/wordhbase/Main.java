
package com.aldy.wordhbase;

import com.aldy.wordhbase.db.DbHelper;
import java.io.IOException;

/**
 *
 * @author sudarsono
 */
public class Main {

    public static void main(String[] args) throws IOException {
        resetTablesAndDumb();
    }
    
    public static void resetTablesAndDumb() throws IOException {
        System.out.println("droping tables...");
        DbHelper.getInstance().dropTables();
        System.out.println("creating tables...");
        DbHelper.getInstance().createTables();
    }
}
