
package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.EditText;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtActivity.class.getSimpleName();

    static final String REMOTE_PORT0 = "11108";

    static final String REMOTE_PORT1 = "11112";

    static final String REMOTE_PORT2 = "11116";

    static final String REMOTE_PORT3 = "11120";

    static final String REMOTE_PORT4 = "11124";

    static final int SERVER_PORT = 10000;

    String myPort;

    String myID;

    String mySuccessorID;

    String mySuccessorPort;

    String myPredecessorID;

    String myPredecessorPort;

    EditText editText1;

    ContentResolver cr;

    static Uri mUri;

    static ContentValues cv;

    SimpleDhtProvider dht;

    String joinMessage;

    DHTDatabaseHelper dbHelper;

    SQLiteDatabase sqlDB;

    SQLiteQueryBuilder queryBuilder;

    boolean deleteFlag;

    boolean predecessorDeleteFlag;

    boolean intermediateDeleteEmulatorFlag;

    int globalRowsDeleted;

    boolean deleteRequestFlag;

    boolean queryFlag;

    boolean predecessorQueryFlag;

    boolean intermediateQueryEmulatorFlag;

    Cursor globalCursor;

    HashMap<String, String> globalMap;

    boolean queryRequestFlag;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        int i = 0;
        String hashKey = "";
        String key = selection;
        String msg;
        intermediateDeleteEmulatorFlag = false;
        int localNumberOfRowsDeleted;
        globalRowsDeleted = 0;
        if (selectionArgs == null) {
            selectionArgs = new String[] {
                    myPort
            };
            deleteFlag = true;
            predecessorDeleteFlag = true;
        }

        // Step 1 : wait for query request flag and set it true
        Log.e(TAG, "Checking for Delete Request flag");
        while (deleteRequestFlag) {
            if (i == 0) {
                Log.e(TAG, "Waiting for Delete Request flag");
                i++;
            }
        }
        i = 0;
        deleteRequestFlag = true;

        // Step 1 : Find the hash code of the key to be deleted
        try {
            hashKey = genHash(key);
        } catch (Exception e) {
        }

        if (mySuccessorID.compareTo(myID) == 0 || selection.equals("@")) {
            globalRowsDeleted = localDelete(uri, selection, selectionArgs);
            printLogCat("Delete is being performed locally  at port: " + myPort
                    + "Total no. of rows deleted " + globalRowsDeleted + "\n", key, "", hashKey);
        }
        // Find the node where the key has to be inserted
        // Step 2 : Check if successor id = self id || select parameter is @
        else if (selection.equals("*")) {
            globalRowsDeleted = localDelete(uri, "@", selectionArgs);
            printLogCat("Delete is being performed locally  at port: " + myPort
                    + "Total no. of rows deleted " + globalRowsDeleted + "\n", key, "", hashKey);

            if (!mySuccessorPort.equals(myPort)) {
                printLogCat("Delete is being broadcasted .Source Port: " + selectionArgs[0]
                        + "My Port " + myPort + "\n", key, "", hashKey);
                msg = Constants.BROADCASTDELETE + "|" + selectionArgs[0] + "|" + mySuccessorPort
                        + "|" + key;
                new ClientThread(msg);
                while (predecessorDeleteFlag)
                    ;
            }
        }
        // Step 3 : Check if the hash key is greater or less than my ID
        else if (hashKey.compareTo(myID) > 0) {

            // Step 6 : Check if my successorID is less than me or hashKey is
            // less than successor ID
            // the key has to be deleted at the successor. Send the delete
            // command to the successor
            if (myPredecessorID.compareTo(myID) > 0 && hashKey.compareTo(myPredecessorID) > 0) {
                globalRowsDeleted = localDelete(uri, "@", selectionArgs);
                printLogCat("Delete is being performed locally  at port: " + myPort
                        + "Total no. of rows deleted " + globalRowsDeleted + "\n", key, "", hashKey);
            } else if (mySuccessorID.compareTo(myID) < 0 || hashKey.compareTo(mySuccessorID) < 0) {
                printLogCat("Delete would be executed at the successor\n", key, "", hashKey);
                intermediateDeleteEmulatorFlag = true;
                msg = Constants.DELETEPROTOCOL + "|" + selectionArgs[0] + "|" + mySuccessorPort
                        + "|" + key;
                new ClientThread(msg);
                while (deleteFlag)
                    ;

            }
            // Step 7 : If hashKey is greater than my successor ID , send the
            // QUERYPROPAGATE message to the successor
            else if (hashKey.compareTo(mySuccessorID) > 0) {
                printLogCat("Delete is being propagated to the successor\n", key, "", hashKey);
                intermediateDeleteEmulatorFlag = true;
                msg = Constants.PROPAGATEDELETE + "|" + selectionArgs[0] + "|" + mySuccessorPort
                        + "|" + key;
                new ClientThread(msg);
                while (deleteFlag)
                    ;
            }
        } else {
            // Step 8 :Check if predecessor is greater than my id or the hash
            // Key is greater than predecessor id
            if (myPredecessorID.compareTo(myID) > 0 || hashKey.compareTo(myPredecessorID) > 0) {
                globalRowsDeleted = localDelete(uri, selection, selectionArgs);
                printLogCat("Delete is being performed locally  at port: " + myPort
                        + "Total no. of rows deleted " + globalRowsDeleted + "\n", key, "", hashKey);
            }
            // Step 9 : If hashKey is less than my predecessor ID , send the
            // QUERYPROPAGATE message to the predecessor
            else if (hashKey.compareTo(myPredecessorID) < 0) {
                printLogCat("Delete is being propagated to the Predecessor\n", key, "", hashKey);
                intermediateDeleteEmulatorFlag = true;
                msg = Constants.PROPAGATEDELETE + "|" + selectionArgs[0] + "|" + myPredecessorPort
                        + "|" + key;
                new ClientThread(msg);
                while (deleteFlag)
                    ;
            }
        }

        localNumberOfRowsDeleted = globalRowsDeleted;
        globalRowsDeleted = 0;
        predecessorDeleteFlag = true;
        deleteFlag = true;
        deleteRequestFlag = false;
        return localNumberOfRowsDeleted;

    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        String key;
        String value;
        String hashKey = "";
        String msg;

        // Step 1 : Get the key to be inserted
        key = values.get(DHTDatabaseHelper.COLUMN_KEY).toString();

        // Step 2: Get the value to be inserted
        value = values.get(DHTDatabaseHelper.COLUMN_VALUE).toString();

        // Step 3 : Find the hash code of the key to be inserted
        try {
            hashKey = genHash(key);
        } catch (Exception e) {

        }

        // Find the node where the key has to be inserted

        // Step 4 : Check if successor id = self id
        if (mySuccessorID.compareTo(myID) == 0) {
            Log.e(TAG, " Local Insert is being performed at port : " + myPort + " Key:" + key
                    + " value:" + value + " hashKey: " + hashKey + "\n");
            localInsert(uri, values);
        }
        // Step 5 : Check if the hash key is greater or less than my ID
        else if (hashKey.compareTo(myID) > 0) {

            // Step 6 : Check if my successorID is less than me or hashKey is
            // less than successor ID
            // the key has to be inserted at the successor. Send the insert
            // command to the successor
            if (myPredecessorID.compareTo(myID) > 0 && hashKey.compareTo(myPredecessorID) > 0) {
                Log.e(TAG, " Local Insert is being performed at port : " + myPort + " Key:" + key
                        + " value:" + value + " hashKey: " + hashKey + "\n");
                localInsert(uri, values);
            } else if (mySuccessorID.compareTo(myID) < 0 || hashKey.compareTo(mySuccessorID) < 0) {
                Log.e(TAG, " Insert would be performed at the successor" + "Key:" + key + " value:"
                        + value + " hashKey: " + hashKey + "\n");
                msg = Constants.INSERTPROTOCOL + "|" + myPort + "|" + mySuccessorPort + "|" + key
                        + "|" + value;
                new ClientThread(msg);
                // new
                // ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,msg,mySuccessorPort);
            }
            // Step 7 : If hashKey is greater than my successor ID , send the
            // INSERTPROPAGATE message to the successor
            else if (hashKey.compareTo(mySuccessorID) > 0) {
                Log.e(TAG, " Insert is being propagated to the successor" + "Key:" + key
                        + " value:" + value + " hashKey: " + hashKey + "\n");
                msg = Constants.PROPAGATEINSERT + "|" + myPort + "|" + mySuccessorPort + "|" + key
                        + "|" + value;
                new ClientThread(msg);
                // new
                // ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,msg,mySuccessorPort);
            }
        } else {
            // Step 8 :Check if predecessor is greater than my id or the hash
            // Key is greater than predecessor id
            if (myPredecessorID.compareTo(myID) > 0 || hashKey.compareTo(myPredecessorID) > 0) {
                Log.e(TAG, " Local Insert is being performed at port : " + myPort + " Key:" + key
                        + " value:" + value + " hashKey: " + hashKey + "\n");
                localInsert(uri, values);
            }
            // Step 9 : If hashKey is less than my predecessor ID , send the
            // INSERTPROPAGATE message to the predecessor
            else if (hashKey.compareTo(myPredecessorID) < 0) {
                Log.e(TAG, " Insert is being propagated to the predecessor " + "Key:" + key
                        + " value:" + value + " hashKey: " + hashKey + "\n");
                msg = Constants.PROPAGATEINSERT + "|" + myPort + "|" + myPredecessorPort + "|"
                        + key + "|" + value;
                new ClientThread(msg);
                // new
                // ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,msg,myPredecessorPort);
            }
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        // Step 1 : Get the databaseHelper instance
        dbHelper = new DHTDatabaseHelper(getContext());
        sqlDB = dbHelper.getWritableDatabase();
        queryBuilder = new SQLiteQueryBuilder();

        // Initialize delete flags
        deleteFlag = true;
        predecessorDeleteFlag = true;
        intermediateDeleteEmulatorFlag = false;
        globalRowsDeleted = 0;
        deleteRequestFlag = false;

        // Initialize query flags
        queryFlag = true;
        predecessorQueryFlag = true;
        intermediateQueryEmulatorFlag = false;
        globalCursor = null;
        globalMap = new HashMap<String, String>();
        queryRequestFlag = false;

        // Step 2 :Get my port
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(
                Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.e(TAG, "My Port:" + myPort + "\n");

        // Step 3 : Get the hash value of my port
        try {
            myID = genHash(portStr);
            Log.e(TAG, "My ID:" + myID + "\n");
        } catch (Exception e) {
        }

        // Step 4 : Initialize the variables
        mySuccessorID = myID;
        mySuccessorPort = myPort;
        myPredecessorID = myID;
        myPredecessorPort = myPort;

        // Step 5 : Invoke the server Task
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {

        }

        // Step 6 : If I am not emulator emulator-5554 then send join request to
        // 5554
        if (!myPort.equals(REMOTE_PORT0)) {
            joinMessage = Constants.JOINPROTOCOL + "|" + myPort;
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, joinMessage,
                    REMOTE_PORT0);

        }

        return false;
    }

    private class ClientThread implements Runnable {

        Thread t;

        String msg;

        ClientThread(String msg) {
            t = new Thread(this);
            this.msg = msg;
            t.start();
        }

        public void run() {
            String msgArray[];
            try {
                // Step 1 : Split the message
                msgArray = msg.split("\\|");

                // Step 2 : create a socket
                Socket socket = new Socket(InetAddress.getByAddress(new byte[] {
                        10, 0, 2, 2
                }), Integer.parseInt(msgArray[2]));

                // Step 2: Create an output stream and send the message
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                out.println(msg);
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                Log.e(TAG, e.toString());
            } catch (Exception e) {
                Log.e(TAG, "Emulator 5554 is not available. Cannot join \n");

            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            try {

                // Step 1 : Create a socket to send the message
                Socket socket = new Socket(InetAddress.getByAddress(new byte[] {
                        10, 0, 2, 2
                }), Integer.parseInt(msgs[1]));

                // Step 2: Create an output stream and send the message
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                out.println(msgs[0]);

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                Log.e(TAG, e.toString());
            } catch (Exception e) {
                Log.e(TAG, "Emulator 5554 is not available. Cannot join \n");

            }

            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            Socket socket;
            BufferedReader in;
            String messageRead;
            String[] messageReadArray;
            String emulator;
            String key;
            String value;
            String msg;
            int numberOfRowsDeleted;
            mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
            cv = new ContentValues();
            String responseMessage;
            Cursor localCursor;

            try {
                while (true) {

                    // Step 1: accept connections at the socket
                    socket = serverSocket.accept();

                    // Step 2 : Get the reader
                    in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    // Step 3 : Read the message
                    messageRead = in.readLine();
                    messageReadArray = messageRead.split("\\|");
                    if (messageReadArray[0].equalsIgnoreCase(Constants.JOINPROTOCOL)) {
                        joinNode(messageReadArray[1]);
                    } else if (messageReadArray[0].equalsIgnoreCase(Constants.REASSIGNSUCCESSOR)) {
                        mySuccessorPort = messageReadArray[1];
                        emulator = new Integer((Integer.parseInt(mySuccessorPort)) / 2).toString();
                        mySuccessorID = genHash(emulator);
                        // printLogCat("Successor Reassigned \n","","","");
                    } else if (messageReadArray[0].equalsIgnoreCase(Constants.REASSIGNPREDECESSOR)) {
                        myPredecessorPort = messageReadArray[1];
                        emulator = new Integer((Integer.parseInt(myPredecessorPort)) / 2)
                                .toString();
                        myPredecessorID = genHash(emulator);
                        // printLogCat("Predecessor Reassigned \n","","","");
                    } else if (messageReadArray[0].equalsIgnoreCase(Constants.INSERTPROTOCOL)) {
                        key = messageReadArray[3];
                        value = messageReadArray[4];
                        cv.put(DHTDatabaseHelper.COLUMN_KEY, key);
                        cv.put(DHTDatabaseHelper.COLUMN_VALUE, value);
                        // printLogCat("Data Inserted Locally \n","","","");
                        localInsert(mUri, cv);
                    } else if (messageReadArray[0].equalsIgnoreCase(Constants.PROPAGATEINSERT)) {
                        key = messageReadArray[3];
                        value = messageReadArray[4];
                        cv.put(DHTDatabaseHelper.COLUMN_KEY, key);
                        cv.put(DHTDatabaseHelper.COLUMN_VALUE, value);
                        // printLogCat("Insert Method Invoked \n","","","");
                        insert(mUri, cv);
                    } else if (messageReadArray[0].equalsIgnoreCase(Constants.DELETEPROTOCOL)) {
                        // call local delete
                        while (deleteRequestFlag && !(messageReadArray[1].equals(myPort)))
                            ;
                        numberOfRowsDeleted = localDelete(mUri, messageReadArray[3], null);
                        printLogCat("Delete is being executed locally  at port : " + myPort
                                + "rows deleted" + numberOfRowsDeleted + "\n", messageReadArray[3],
                                "", genHash(messageReadArray[3]));
                        responseMessage = Constants.DELETERESPONSE + "|" + myPort + "|"
                                + messageReadArray[1] + "|" + numberOfRowsDeleted;
                        // Send the response back
                        new ClientThread(responseMessage);
                    } else if (messageReadArray[0].equalsIgnoreCase(Constants.DELETERESPONSE)) {
                        globalRowsDeleted += Integer.parseInt(messageReadArray[3]);
                        if (messageReadArray.length > 3) {
                            printLogCat("Delete Response received : " + myPort + "rows deleted"
                                    + globalRowsDeleted + "\n", messageReadArray[3], "",
                                    genHash(messageReadArray[3]));
                        } else {
                            Log.e(TAG, "Delete Response received : . Nut no key as output" + "\n");
                        }
                        deleteFlag = false;
                        if (messageReadArray[1].equals(myPredecessorPort)) {
                            predecessorDeleteFlag = false;
                        }
                    } else if (messageReadArray[0].equalsIgnoreCase(Constants.PROPAGATEDELETE)) {
                        while (deleteRequestFlag)
                            ;
                        deleteFlag = false;
                        predecessorDeleteFlag = false;
                        intermediateDeleteEmulatorFlag = false;
                        numberOfRowsDeleted = delete(mUri, messageReadArray[3], new String[] {
                                messageReadArray[1]
                        });
                        if (!intermediateDeleteEmulatorFlag) {
                            responseMessage = Constants.DELETERESPONSE + "|" + myPort + "|"
                                    + messageReadArray[1] + "|" + numberOfRowsDeleted;
                            // Send the response back
                            new ClientThread(responseMessage);
                        }
                        printLogCat("PROPAGATEDELETE request received at  : " + myPort
                                + "rows deleted" + globalRowsDeleted + "\n", messageReadArray[3],
                                "", genHash(messageReadArray[3]));
                    } else if (messageReadArray[0].equalsIgnoreCase(Constants.BROADCASTDELETE)) {
                        while (deleteRequestFlag)
                            ;
                        numberOfRowsDeleted = localDelete(mUri, "@", null);
                        responseMessage = Constants.DELETERESPONSE + "|" + myPort + "|"
                                + messageReadArray[1] + "|" + numberOfRowsDeleted;
                        // Send the response back
                        new ClientThread(responseMessage);
                        if (!mySuccessorPort.equals(messageReadArray[1])) {
                            msg = Constants.BROADCASTDELETE + "|" + messageReadArray[1] + "|"
                                    + mySuccessorPort + "|" + messageReadArray[3];
                            new ClientThread(msg);
                        }
                        printLogCat(
                                "BROADCASTDELETE request received at  : " + myPort + "rows deleted"
                                        + numberOfRowsDeleted + "message broadcasted further"
                                        + !mySuccessorPort.equals(messageReadArray[1]) + "\n",
                                messageReadArray[3], "", genHash(messageReadArray[3]));
                    } else if (messageReadArray[0].equalsIgnoreCase(Constants.QUERYPROTOCOL)) {
                        // call local query
                        while (queryRequestFlag && !(messageReadArray[1].equals(myPort)))
                            ;
                        localCursor = localQuery(mUri, null, messageReadArray[3], null, null);
                        Log.e(TAG, "Query has been  executed locally  at port : " + myPort
                                + " key:" + messageReadArray[3] + " hashKey: "
                                + genHash(messageReadArray[3]) + "\n");
                        responseMessage = Constants.QUERYRESPONSE + "|" + myPort + "|"
                                + messageReadArray[1] + "|" + cursorToString(localCursor);
                        // Send the response back
                        new ClientThread(responseMessage);
                    } else if (messageReadArray[0].equalsIgnoreCase(Constants.QUERYRESPONSE)) {
                        insertIntoGlobalMap(messageRead);
                        globalCursor = mapToCursor(globalMap);
                        if (messageReadArray.length > 3) {
                            Log.e(TAG, "Query Response received : " + myPort + " key:"
                                    + messageReadArray[3] + " hashKey: "
                                    + genHash(messageReadArray[3]) + "\n");
                        } else {
                            Log.e(TAG, "Query Response received : . Nut no key as output" + "\n");
                        }
                        queryFlag = false;
                        if (messageReadArray[1].equals(myPredecessorPort)) {
                            predecessorQueryFlag = false;
                        }
                    } else if (messageReadArray[0].equalsIgnoreCase(Constants.PROPAGATEQUERY)) {
                        while (queryRequestFlag)
                            ;
                        queryFlag = false;
                        predecessorQueryFlag = false;
                        intermediateQueryEmulatorFlag = false;
                        localCursor = query(mUri, null, messageReadArray[3], new String[] {
                                messageReadArray[1]
                        }, null);
                        if (!intermediateQueryEmulatorFlag) {
                            responseMessage = Constants.QUERYRESPONSE + "|" + myPort + "|"
                                    + messageReadArray[1] + "|" + cursorToString(localCursor);
                            // Send the response back
                            new ClientThread(responseMessage);
                        }
                        Log.e(TAG, "PROPAGATEQUERY request received at  : " + myPort + " key:"
                                + messageReadArray[3] + " hashKey: " + genHash(messageReadArray[3])
                                + "\n");
                    } else if (messageReadArray[0].equalsIgnoreCase(Constants.BROADCASTQUERY)) {

                        while (queryRequestFlag)
                            ;
                        localCursor = localQuery(mUri, null, "@", null, null);
                        responseMessage = Constants.QUERYRESPONSE + "|" + myPort + "|"
                                + messageReadArray[1] + "|" + cursorToString(localCursor);
                        // Send the response back
                        new ClientThread(responseMessage);
                        if (!mySuccessorPort.equals(messageReadArray[1])) {
                            msg = Constants.BROADCASTQUERY + "|" + messageReadArray[1] + "|"
                                    + mySuccessorPort + "|" + messageReadArray[3];
                            new ClientThread(msg);
                        }
                        Log.e(TAG,
                                "BROADCASTQUERY request received at  : " + myPort
                                        + "message broadcasted further"
                                        + !mySuccessorPort.equals(messageReadArray[1])
                                        + "hashKey: " + genHash(messageReadArray[3]) + "\n");

                    }

                }
            } catch (Exception e) {
                Log.e(TAG, e.getMessage());
            }
            return null;
        }

        /*
         * void storeMessage(String msg){ String messageReadArray[] =
         * msg.split("\\|"); cr = getContentResolver(); cv = new
         * ContentValues(); mUri = buildUri("content",
         * "edu.buffalo.cse.cse486586.groupmessenger.provider");
         * cv.put(MessengerBean.COLUMN_KEY,messageReadArray[1] );
         * cv.put(MessengerBean.COLUMN_VALUE,messageReadArray[3]);
         * cr.insert(mUri, cv); }
         */

        void joinNode(String port) {

            String portID = null;
            String msg;
            String emulator;

            // Step 1 : Generate the hash id of the given port
            try {
                emulator = new Integer((Integer.parseInt(port)) / 2).toString();
                portID = genHash(emulator);
            } catch (Exception e) {

            }

            // Step 2 : Check if successor id = self id
            if (mySuccessorID.compareTo(myID) == 0) {
                mySuccessorID = portID;
                mySuccessorPort = port;
                myPredecessorID = portID;
                myPredecessorPort = port;
                printLogCat("Predecessor and Successor Reassigned", "", "", "");
                msg = Constants.REASSIGNPREDECESSOR + "|" + myPort;
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg, port);
                msg = Constants.REASSIGNSUCCESSOR + "|" + myPort;
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg, port);
            }
            // Step 3 : Check if the hash code is greater or less than my ID
            else if (portID.compareTo(myID) > 0) {

                // Step 4 : Check if my successorID is less than me or new node
                // id is less than successor ID
                if (mySuccessorID.compareTo(myID) < 0 || portID.compareTo(mySuccessorID) < 0) {

                    // Step 5 : If the port lies between me and my successor set
                    // predecessor and successor of the new avd
                    msg = Constants.REASSIGNPREDECESSOR + "|" + myPort;
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg, port);
                    msg = Constants.REASSIGNSUCCESSOR + "|" + mySuccessorPort;
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg, port);

                    // Step 6 : Set the predecessor of your successor
                    msg = Constants.REASSIGNPREDECESSOR + "|" + port;
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg,
                            mySuccessorPort);

                    // Step 7 : Set my successor
                    mySuccessorID = portID;
                    mySuccessorPort = port;
                    printLogCat("Predecessor and Successor Reassigned", "", "", "");
                }
                // Step 8 : If portID is greater than my successor ID , send the
                // JOIN message to the successor
                else if (portID.compareTo(mySuccessorID) > 0) {
                    msg = Constants.JOINPROTOCOL + "|" + port;
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg,
                            mySuccessorPort);
                }
            } else {

                // Step 9 :Check if predecessor is greater than my id or the new
                // port has id greater than predecessor id
                if (myPredecessorID.compareTo(myID) > 0 || portID.compareTo(myPredecessorID) > 0) {

                    // Step 10 : If the port lies between me and my predecessor
                    // set predecessor and successor of the new avd
                    msg = Constants.REASSIGNPREDECESSOR + "|" + myPredecessorPort;
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg, port);
                    msg = Constants.REASSIGNSUCCESSOR + "|" + myPort;
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg, port);

                    // Step 11 : Set the Successor of your predecessor
                    msg = Constants.REASSIGNSUCCESSOR + "|" + port;
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg,
                            myPredecessorPort);

                    // Step 12 : Set my Predecessor
                    myPredecessorID = portID;
                    myPredecessorPort = port;
                    printLogCat("Predecessor Reassigned", "", "", "");
                }

                // Step 13 : If portID is less than my predecessor ID , send the
                // JOIN message to the predecessor
                else if (portID.compareTo(myPredecessorID) < 0) {
                    msg = Constants.JOINPROTOCOL + "|" + port;
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg,
                            myPredecessorPort);
                }

            }

        }

        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        /*
         * protected void onProgressUpdate(String...strings) { The following
         * code displays what is received in doInBackground(). String
         * strReceived = strings[0].trim(); String stringArray[] =
         * strReceived.split("\\|"); TextView localTextView = (TextView)
         * findViewById(R.id.textView1); localTextView.append(stringArray[3] +
         * "\t\n"); The following code creates a file in the AVD's internal
         * storage and stores a file. For more information on file I/O on
         * Android, please take a look at
         * http://developer.android.com/training/basics/data-storage/files.html
         * return; }
         */

    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub

        int i = 0;
        String hashKey = "";
        String key = selection;
        String msg;
        intermediateQueryEmulatorFlag = false;
        Cursor localCursor;
        if (selectionArgs == null) {
            selectionArgs = new String[] {
                    myPort
            };
            queryFlag = true;
            predecessorQueryFlag = true;
        }

        // Step 1 : wait for query request flag and set it true
        Log.e(TAG, "Checking for Query Request flag");
        while (queryRequestFlag) {
            if (i == 0) {
                Log.e(TAG, "Waiting for Query Request flag");
                i++;
            }
        }
        i = 0;
        queryRequestFlag = true;

        // Step 2 : Find the hash code of the key to be searched
        try {
            hashKey = genHash(key);
        } catch (Exception e) {
        }

        // Find the node where the key has to be searched
        // Step 3 : Check if successor id = self id || select parameter is @

        if (mySuccessorID.compareTo(myID) == 0 || selection.equals("@")) {
            globalCursor = localQuery(uri, null, selection, null, null);
            Log.e(TAG, "Query has been performed locally  at port: " + myPort + " key:" + key
                    + "hashkey: " + hashKey);

        } else if (selection.equals("*")) {
            globalCursor = localQuery(uri, null, "@", null, null);
            globalMap = cursorToMap(globalCursor);
            Log.e(TAG, "Query has been performed locally  at port: " + myPort + " key:" + key
                    + "hashkey: " + hashKey);
            if (!mySuccessorPort.equals(myPort)) {
                Log.e(TAG, "Query is being broadcasted .Source Port: " + selectionArgs[0]
                        + " My Port: " + myPort + " SuccessorPort:" + mySuccessorPort);
                msg = Constants.BROADCASTQUERY + "|" + selectionArgs[0] + "|" + mySuccessorPort
                        + "|" + key;
                new ClientThread(msg);
                while (predecessorQueryFlag)
                    ;

            }
        }
        // Step 3 : Check if the hash key is greater or less than my ID
        else if (hashKey.compareTo(myID) > 0) {
            // Step 6 : Check if my successorID is less than me or hashKey is
            // less than successor ID
            // the key has to be deleted at the successor. Send the delete
            // command to the successor
            if (myPredecessorID.compareTo(myID) > 0 && hashKey.compareTo(myPredecessorID) > 0) {
                globalCursor = localQuery(uri, null, selection, null, null);
                Log.e(TAG, "Query has been performed locally  at port: " + " Myport: " + myPort
                        + " key: " + key + " hashKey: " + hashKey + "\n");
            } else if (mySuccessorID.compareTo(myID) < 0 || hashKey.compareTo(mySuccessorID) < 0) {
                Log.e(TAG, "Query would be executed at the successor .Source Port: "
                        + selectionArgs[0] + " My Port: " + myPort + " SuccessorPort:"
                        + mySuccessorPort + " key: " + key + " hashKey: " + hashKey);
                intermediateQueryEmulatorFlag = true;
                msg = Constants.QUERYPROTOCOL + "|" + selectionArgs[0] + "|" + mySuccessorPort
                        + "|" + key;
                new ClientThread(msg);
                while (queryFlag)
                    ;
            }
            // Step 7 : If hashKey is greater than my successor ID , send the
            // QUERYPROPAGATE message to the successor
            else if (hashKey.compareTo(mySuccessorID) > 0) {
                Log.e(TAG, "Query is being propagated to the successor " + " key: " + key
                        + " hashKey: " + hashKey);
                intermediateQueryEmulatorFlag = true;
                msg = Constants.PROPAGATEQUERY + "|" + selectionArgs[0] + "|" + mySuccessorPort
                        + "|" + key;
                new ClientThread(msg);
                while (queryFlag)
                    ;

            }
        } else {
            // Step 8 :Check if predecessor is greater than my id or the hash
            // Key is greater than predecessor id
            if (myPredecessorID.compareTo(myID) > 0 || hashKey.compareTo(myPredecessorID) > 0) {
                globalCursor = localQuery(uri, null, selection, null, null);
                Log.e(TAG, "Query has been performed locally  at port: " + " Myport: " + myPort
                        + " key: " + key + " hashKey: " + hashKey + "\n");
            }
            // Step 9 : If hashKey is less than my predecessor ID , send the
            // QUERYPROPAGATE message to the predecessor
            else if (hashKey.compareTo(myPredecessorID) < 0) {
                Log.e(TAG, "Query is being propagated to the Predecessor " + " Myport: " + myPort
                        + " key: " + key + " hashKey: " + hashKey + "\n");
                intermediateQueryEmulatorFlag = true;
                msg = Constants.PROPAGATEQUERY + "|" + selectionArgs[0] + "|" + myPredecessorPort
                        + "|" + key;
                new ClientThread(msg);
                while (queryFlag)
                    ;
            }
        }

        localCursor = globalCursor;
        globalCursor = null;
        predecessorQueryFlag = true;
        queryFlag = true;
        queryRequestFlag = false;
        globalMap.clear();
        return localCursor;
    }

    public Cursor localQuery(Uri uri, String[] projection, String selection,
            String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        Cursor cursor;
        queryBuilder.setTables(DHTDatabaseHelper.TABLE_DHT);
        String argument[] = {
                selection
        };
        if (selection.equals("@") || selection.equals("*")) {
            cursor = queryBuilder.query(sqlDB, projection, null, null, null, null, sortOrder);
        } else {
            cursor = queryBuilder.query(sqlDB, projection, DHTDatabaseHelper.COLUMN_KEY + "=?",
                    argument, null, null, sortOrder);
        }
        // make sure that potential listeners are getting notified
        cursor.setNotificationUri(getContext().getContentResolver(), uri);
        // Log.e(TAG ," Local query successful \n");
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        int rowsUpdated;
        rowsUpdated = sqlDB.update(DHTDatabaseHelper.TABLE_DHT, values,
                DHTDatabaseHelper.COLUMN_KEY + "=?", selectionArgs);
        getContext().getContentResolver().notifyChange(uri, null);
        return rowsUpdated;
    }

    public Uri localInsert(Uri uri, ContentValues values) {

        String key;
        String keyArray[];
        Cursor queryResult;
        int keyIndex;
        int valueIndex;
        String returnKey;
        String returnValue;

        // Step 1: Get the key and value to be inserted
        key = values.get(DHTDatabaseHelper.COLUMN_KEY).toString();
        keyArray = new String[] {
                key
        };

        // Step 2 : find if already a record exists with the given key
        queryResult = localQuery(uri, null, key, null, null);
        if (queryResult.getCount() > 0) {
            update(uri, values, DHTDatabaseHelper.COLUMN_KEY, keyArray);
        } else {
            sqlDB.insert(DHTDatabaseHelper.TABLE_DHT, null, values);
            queryResult = localQuery(uri, null, key, null, null);
            if (queryResult != null && queryResult.getCount() > 0 && queryResult.moveToFirst()) {
                keyIndex = queryResult.getColumnIndex(DHTDatabaseHelper.COLUMN_KEY);
                valueIndex = queryResult.getColumnIndex(DHTDatabaseHelper.COLUMN_VALUE);
                try {
                    returnKey = queryResult.getString(keyIndex);
                    returnValue = queryResult.getString(valueIndex);
                    Log.e(TAG, " Inside Insert .Query output is : key : " + returnKey + " value: "
                            + returnValue + "\n");
                    // Log.e("Insert successful", values.toString());
                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                }
            }
        }

        // Step 3 : Notify the changes
        getContext().getContentResolver().notifyChange(uri, null);
        return uri;

    }

    public int localDelete(Uri uri, String selection, String[] selectionArgs) {

        int numberOfRowsDeleted;
        String args[] = {
                selection
        };
        if (selection.equals("@") || selection.equals("*")) {
            numberOfRowsDeleted = sqlDB.delete(DHTDatabaseHelper.TABLE_DHT, null, null);
        } else {
            numberOfRowsDeleted = sqlDB.delete(DHTDatabaseHelper.TABLE_DHT,
                    DHTDatabaseHelper.COLUMN_KEY + "=?", args);
        }
        getContext().getContentResolver().notifyChange(uri, null);
        Log.e(TAG, "Delete successful \n");
        return numberOfRowsDeleted;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public void printLogCat(String message, String key, String value, String hashKey) {
        Log.e(TAG, message);
        Log.e(TAG, "My Port:" + myPort + " MyPredecessorPort:" + myPredecessorPort
                + " MySuccessorPort:" + mySuccessorPort + "\n");
        Log.e(TAG, "My ID:" + myID + " MyPredecessorID:" + myPredecessorID + " MySuccessorID:"
                + mySuccessorID + "\n");
        Log.e(TAG, "key: " + key + " value: " + value + " myId: " + myID + " hashKey: " + hashKey
                + "\n");

    }

    public MatrixCursor mapToCursor(Map<String, String> cursorMap) {
        Map.Entry<String, String> mapEntry;
        MatrixCursor cursor = new MatrixCursor(new String[] {
                DHTDatabaseHelper.COLUMN_KEY, DHTDatabaseHelper.COLUMN_VALUE
        });
        Iterator<Map.Entry<String, String>> iterator = cursorMap.entrySet().iterator();
        while (iterator.hasNext()) {
            mapEntry = (Map.Entry<String, String>) iterator.next();
            cursor.addRow(new String[] {
                    (String) mapEntry.getKey(), (String) mapEntry.getValue()
            });
        }
        return cursor;
    }

    public String cursorToString(Cursor cursor) {
        String output;
        HashMap<String, String> cursorMap = cursorToMap(cursor);
        output = mapToString(cursorMap);
        return output;
    }

    public HashMap<String, String> cursorToMap(Cursor cursor) {
        int count;
        HashMap<String, String> cursorMap = new HashMap<String, String>();
        cursor.moveToFirst();
        for (count = 0; count < cursor.getCount(); count++) {
            cursorMap.put(cursor.getString(cursor.getColumnIndex(DHTDatabaseHelper.COLUMN_KEY)),
                    cursor.getString(cursor.getColumnIndex(DHTDatabaseHelper.COLUMN_VALUE)));
            cursor.moveToNext();
        }
        return cursorMap;
    }

    public String mapToString(HashMap<String, String> cursorMap) {
        String output = "";
        Map.Entry<String, String> mapEntry;
        Iterator<Map.Entry<String, String>> iterator = cursorMap.entrySet().iterator();
        while (iterator.hasNext()) {
            mapEntry = (Map.Entry<String, String>) iterator.next();
            output = output + (String) mapEntry.getKey();
            output = output + "|";
            output = output + (String) mapEntry.getValue();
            output = output + "|";
        }

        return output;

    }

    public HashMap<String, String> stringToMap(String input) {

        int count = 0;
        int temp;
        HashMap<String, String> cursorMap = new HashMap<String, String>();
        String[] stringArray = input.split("\\|");
        for (count = 0; count < stringArray.length; count += 2) {
            temp = count + 1;
            cursorMap.put(stringArray[count], stringArray[temp]);
        }

        return cursorMap;

    }

    public void insertIntoGlobalMap(String input) {
        int count;
        int temp;
        String[] stringArray = input.split("\\|");
        for (count = 3; count < stringArray.length; count += 2) {
            temp = count + 1;
            globalMap.put(stringArray[count], stringArray[temp]);
        }
    }
}
