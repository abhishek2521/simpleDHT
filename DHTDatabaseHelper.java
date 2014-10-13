
package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

public class DHTDatabaseHelper extends SQLiteOpenHelper {

    // Database table
    public static final String TABLE_DHT = "dht";
    public static final String COLUMN_KEY = "key";
    public static final String COLUMN_VALUE = "value";
    private static final String DATABASE_NAME = "DHTTable.db";
    private static final int DATABASE_VERSION = 1;

    public DHTDatabaseHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    // Database creation SQL statement
    private static final String DATABASE_CREATE = "create table "
            + TABLE_DHT
            + "("
            + COLUMN_KEY + " text not null, "
            + COLUMN_VALUE + " text not null"
            + ");";

    // Method is called during creation of the database
    @Override
    public void onCreate(SQLiteDatabase database) {
        database.execSQL(DATABASE_CREATE);
    }

    public void onUpgrade(SQLiteDatabase database, int oldVersion,
            int newVersion) {
        Log.w(DHTDatabaseHelper.class.getName(), "Upgrading database from version "
                + oldVersion + " to " + newVersion
                + ", which will destroy all old data");
        database.execSQL("DROP TABLE IF EXISTS " + TABLE_DHT);
        onCreate(database);
    }
}
