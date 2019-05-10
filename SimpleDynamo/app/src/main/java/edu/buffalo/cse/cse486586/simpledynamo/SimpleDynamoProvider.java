package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final String EMULATOR1 = "5554";
    static final String EMULATOR2 = "5556";
    static final String EMULATOR3 = "5558";
    static final String EMULATOR4 = "5560";
    static final String EMULATOR5 = "5562";
    private String myPort;
    private String nodeAttachPort = "5554";
    private String previousPort = "0";
    private String successorPort = "0";
    private String myHashValue =  "";
    private String previousPortHash = "";
    private String successorPortHash = "";
    static final int SERVER_PORT = 10000;
    private ServerSocket serverSocket;
    private SQLiteDatabase sqLiteDatabase;
    private Uri cpUri;
    String[] remotePorts = new String[]{REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    static ArrayList<String> REMOTE_PORTS = new ArrayList<String>(Arrays.asList(REMOTE_PORT0,
            REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4));
    static ArrayList<String> EMULATORS = new ArrayList<String>(Arrays.asList(EMULATOR1, EMULATOR2, EMULATOR3, EMULATOR4, EMULATOR5));
    private List<String> chordMembers = new ArrayList<String>();
    private Map<String, String> hashPortMapping = new HashMap<String, String>();
    private KVStorageSqliteOpenHelper storageSqliteOpenHelper = new KVStorageSqliteOpenHelper(this.getContext());
    private Map<String, String> hm = new HashMap<String, String>();
    //emulator to remote port mapping
    String TABLE_NAME = KVStorageSqliteOpenHelper.TABLE_KVSTORAGE;
    String COLUMN_KEY = KVStorageSqliteOpenHelper.COLUMN_KEY;
    String COLUMN_VALUE = KVStorageSqliteOpenHelper.COLUMN_VALUE;
    String totalResult = "";
    int resultCount = 0;
    String queryValAvd = "";
    public int numberOfLiveDevice = 4;
    public String failedNode = "";
    public Map<String, List<ContentValues>> mp = new HashMap<String, List<ContentValues>>();
    boolean dataFound = false;
    boolean nodeRecovered = false;

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    /**
     5562,
     5556,
     5554,
     5558,
     5560
     **/

    public void deleteOwnDataWithKey(String selection) {
        sqLiteDatabase = storageSqliteOpenHelper.getWritableDatabase();
        String[] args = {selection};
        sqLiteDatabase.delete(TABLE_NAME, COLUMN_KEY + "=?", args);
    }

    public void deleteOwnDateBeforeRecovery() {
        sqLiteDatabase = storageSqliteOpenHelper.getWritableDatabase();
        sqLiteDatabase.delete(TABLE_NAME, null, null);
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Log.e("inside delete", "function");

        while (!nodeRecovered) {
            Log.e("waiting for recovery", "waiting delete");
        }

        sqLiteDatabase = storageSqliteOpenHelper.getWritableDatabase();
        String keyHash = "";

        try {
            keyHash = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


//        if (previousPort.equals("0") && successorPort.equals("0")) {
//            if (selection.equals("*") || selection.equals("@")) {
//                return sqLiteDatabase.delete(TABLE_NAME, null, null);
//            } else {
//                String[] args = {selection};
//                return sqLiteDatabase.delete(TABLE_NAME, COLUMN_KEY + "=?", args);
//            }
//        } else
            if (selection.equals("*")) {
            //delete in all avds

            String message = "*";
            //tell all avd to delete, so in loop lunch client tasks
            // Log.e("in delete 1", "sending to all");
            for (int i = 0; i < remotePorts.length; i++) {
                if (!remotePorts.equals(hm.get(myPort))) {
                    //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, remotePorts[i], message, "Delete");
                    try {
                        String res = new connectToAvdTask().execute(remotePorts[i], "Delete", message).get();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            try {
                Thread.sleep(600);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return sqLiteDatabase.delete(TABLE_NAME, null, null);


        } else if (selection.equals("@")) {
            return sqLiteDatabase.delete(TABLE_NAME, null, null);
//        } else if (checkCondition(keyHash)) {
//            String[] args = {selection};
//            return sqLiteDatabase.delete(TABLE_NAME, COLUMN_KEY + "=?", args);
        } else {
            //key exists at some other avd.
            //Log.e("in delete 2 sending to ", hm.get(successorPort));
            String[] arr = getPortsForInsertion(keyHash);
            String message = selection;

            for (int i=0;i<arr.length;i++) {
                if (arr[i].equals(myPort)) {
                    String[] args = {selection};
                    return sqLiteDatabase.delete(TABLE_NAME, COLUMN_KEY + "=?", args);
                } else {
                    String remote_port = hm.get(arr[i]);
                    //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, remote_port, message, "Delete");
                    try {
                        String res = new connectToAvdTask().execute(remote_port, "Delete", message).get();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    public void directInsert(ContentValues values) {
        //Log.e("in direct", "insert");
        //Log.e("inserting in "+myPort, values.getAsString("key")+" "+values.getAsString("value"));
        storageSqliteOpenHelper.add(values);
    }

    public void directInsertMultiple(String message) {
        //Log.e("in direct multiple", "insert");
        String[] keyVals = message.split("\\-");
       // Log.e("keyVals length", String.valueOf(keyVals.length));

        for (int i=0;i<keyVals.length;i++) {
            String[] parts = keyVals[i].split("\\#");
            ContentValues values = new ContentValues();
            values.put("key", parts[0]);
            values.put("value", parts[1]);
            //Log.e("adding "+parts[0], parts[1]);
            storageSqliteOpenHelper.add(values);
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        //Log.e("in insert", "key is "+values.getAsString("key"));
        // TODO Auto-generated method stub

        while (!nodeRecovered) {
            Log.e("waiting for recovery", "waiting insert");
        }

        //Log.e("pred in insert "+previousPort, "succ in "+successorPort);
        //Log.e("key is",values.getAsString("key"));

        String keyHash = "";
        try {
            keyHash = genHash(values.getAsString("key"));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        //Log.e("gettig ports", "for insertion");
        String[] arr = getPortsForInsertion(keyHash);
        //Log.e("store 1", arr[0]);
        //Log.e("store 2", arr[1]);
        //Log.e("store 3", arr[2]);

        //Log.e("got ports", String.valueOf(arr.length));

        for (int i=0; i<arr.length; i++) {
            //Log.e("my port is "+myPort, "sending to "+arr[i]);
            if (arr[i].equals(myPort)) {
                //Log.e("storing in self "+myPort, values.getAsString("key")+" "+values.getAsString("value"));
                //Log.e("inside insert", "fourth");
                //now this key needs to get inserted in some other avd
                //Log.e("in insert 2 sending to ", "val is "+hm.get(successorPort));
                storageSqliteOpenHelper.add(values);
            } else {
                String message = values.getAsString("key") + "#" + values.getAsString("value");
                String remote_port = hm.get(arr[i]);
               // Log.e("insert sending to port "+remote_port, "Direct Insert "+message);
                //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, remote_port, message, "DirectInsert");

                try {
                    String res = new connectToAvdTask().execute(remote_port, "DirectInsert", message).get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    public String[] getPortsForInsertion(String hashKey) {
        //Log.e("inside", "get ports");
        int i;
        int index1;
        int index2 = -1;
        //Log.e("key hash is", hashKey);

        //Log.e("chordmembers size", String.valueOf(chordMembers.size()));

        for (i=0; i<chordMembers.size(); i++) {
            //Log.e("getPortforInsertion", "i is "+i);
            //Log.e("second", "second");
            index1 = i%5;
            index2 = (i+1)%5;

            //Log.e("index 1 is "+index1, "index 2 is "+index2);

            if (hashKey.compareTo(chordMembers.get(index1)) > 0 && hashKey.compareTo(chordMembers.get(index2)) <= 0) {
                Log.e("i is "+i, "breaking");
                break;
            }
        }

        //Log.e("key belongs to ", String.valueOf(index2));

        //here i+1 is the node partition where this key belongs, we need to return next 2 ports also where insertion will happen for replicas
        int next = (index2+1)%5;
        int next_to_next = (index2+2)%5;
        //Log.e("key next to ", String.valueOf(next));
        //Log.e("key next to next to ", String.valueOf(next_to_next));

        String port = hashPortMapping.get(chordMembers.get(index2));
        String port1 = hashPortMapping.get(chordMembers.get(next));
        String port2 = hashPortMapping.get(chordMembers.get(next_to_next));
        String[] arr = new String[3];
        arr[0] = port;
        arr[1] = port1;
        arr[2] = port2;
        return arr;
    }


    @Override
    public boolean onCreate() {
        Log.e("in on create", "starting");
        // TODO Auto-generated method stub
        hm.put("5554", REMOTE_PORT0);
        hm.put("5556", REMOTE_PORT1);
        hm.put("5558", REMOTE_PORT2);
        hm.put("5560", REMOTE_PORT3);
        hm.put("5562", REMOTE_PORT4);

        storageSqliteOpenHelper = new KVStorageSqliteOpenHelper(getContext());
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        myPort = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        cpUri  = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo");

        Log.e("MY PORT: "+myPort, ".");

        try {
            myHashValue = genHash(myPort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        String hashval = "";

        for (int i=0; i<EMULATORS.size(); i++) {
            try {
                hashval = genHash(EMULATORS.get(i));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            hashPortMapping.put(hashval, EMULATORS.get(i));
            chordMembers.add(hashval);
        }

        Collections.sort(chordMembers);

        try {
            //Log.e("creating socket", "on create");
            serverSocket = new ServerSocket(SERVER_PORT);
        } catch (IOException e) {
            //Log.e("exception", "on create");
            e.printStackTrace();
        }

        //set previous and successor port here
        int myIndex = chordMembers.indexOf(myHashValue);
        setPS(myIndex);
        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        //now here we have to tell previous port if there is something for this node, then give it to this node
        recoverNode();


        //Log.e("launching", "new server task");
        //Log.e("AfterServerClientthreds","");
        return false;
    }

    public void recoverNode() {
        String[] arr = getPortsForRecovery();
        //tellPredecessorIAmAliveAgain(previousPort);
        //ask for data from previous ports

        Log.e("calling delete", "during recovery");
        //deleteOwnDateBeforeRecovery();

        Log.e("asking others", "for data");

        for (int i = 0; i < remotePorts.length; i++) {

            if (remotePorts[i].equals(hm.get(myPort))) {
                Log.e("continue","do not ask myself");
                continue;
            }

            String message = "";
            try {
                message = new connectToAvdTask().execute(remotePorts[i], "Alive", myPort).get();
            } catch (Exception e) {
                message = "failed";
                e.printStackTrace();
            }

//            if (message == null) {
//                message = "failed";
//            }

            if (message.equals("failed") || message.equals("nothing")) {
                Log.e("message is", message);
                continue;
            } else {
                Log.e("got some data", "from "+remotePorts[i]);
                directInsertMultiple(message);
            }
        }

        Log.e("making flag","true");
        nodeRecovered = true;
    }

    public String[] getPortsForRecovery() {
        int index = chordMembers.indexOf(myHashValue);
        String[] arr = new String[2];

        if (index == 0) {
            arr[0] = hashPortMapping.get(chordMembers.get(3));
            arr[1] = hashPortMapping.get(chordMembers.get(4));
        } else if (index == 1) {
            arr[0] = hashPortMapping.get(chordMembers.get(0));
            arr[1] = hashPortMapping.get(chordMembers.get(4));
        } else {
            arr[0] = hashPortMapping.get(chordMembers.get(index-1));
            arr[1] = hashPortMapping.get(chordMembers.get(index-2));
        }

        Log.e("recovery for "+myPort, "from "+arr[0]+" "+arr[1]);
        return arr;
    }

    public void tellPredecessorIAmAliveAgain(String port) {
        String message = myPort;
        Log.e("telling predecesor", "abt alive");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, hm.get(port), message, "Alive");
    }

    public void setPS(int myIndex) {
        int prev_index = -1, next_index = -1;

        if (myIndex == 0) {
            prev_index = 4;
            next_index = 1;
        } else if (myIndex == 4) {
            prev_index = 3;
            next_index = 0;
        } else {
            prev_index = myIndex-1;
            next_index = myIndex+1;
        }

        successorPortHash = chordMembers.get(next_index);
        previousPortHash = chordMembers.get(prev_index);
        successorPort = hashPortMapping.get(successorPortHash);
        previousPort = hashPortMapping.get(previousPortHash);
        Log.e("done setting", "pred succ");
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        Log.e("query is", selection);

        while (!nodeRecovered) {
            Log.e("waiting for recovery", "waiting query");
        }

        sqLiteDatabase = storageSqliteOpenHelper.getWritableDatabase();
        String keyToHash = "";
        String[] dataCols = {COLUMN_KEY, COLUMN_VALUE};
        String[] query = {selection};

        try {
            keyToHash = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        String searchParams = COLUMN_KEY + " = ?";

        if (selection.equals("@")) {
            return fetchOwnData();
        } else if (selection.equals("*")) {

            //send requests to all 5 avd and wait for their response
            resultCount = 0;
            totalResult = "";
            MatrixCursor mC = new MatrixCursor(dataCols);
            String message = "*" + "#" + myPort;

            for (int i = 0; i < remotePorts.length; i++) {
                if (!remotePorts[i].equals(hm.get(myPort))) {
                    //Log.e("luunching client task", remotePorts[i]);
                    clientTaskForQuery(remotePorts[i], message, "Query");
                }
            }

            while (true) {
                //here we check if we receive messages for all 5 avds
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                //Log.e("resultCount", String.valueOf(resultCount));
                Log.e("number of Live", String.valueOf(numberOfLiveDevice));
                if (resultCount >= 3) {
                    //here I have got the result from all 4 avds in my totalResult so just return cursor now
                    //Log.e("total result split", totalResult);

                    String[] allAvdResults = totalResult.split(":");
                    //Log.e("allAvdResultSize", String.valueOf(allAvdResults.length));

                    for (int i=0; i<allAvdResults.length; i++) {
                        String[] keyVals = allAvdResults[i].split("\\|");
                        //Log.e("keyvals length", String.valueOf(keyVals.length));
                        for (int j=0;j<keyVals.length;j++) {
                            //Log.e("inside key", keyVals[j]);
                            String[] pair = keyVals[j].split("-");
                            if (pair.length > 1) {
                                //Log.e("pair length", String.valueOf(pair.length));
                                Object[] values = {pair[0], pair[1]};
                                mC.addRow(values);
                            }
                        }
                    }
                    break;
                }
            }

            //now add own avd results
            //Log.e("adding own results", "*");
            Cursor cursor = sqLiteDatabase.rawQuery("SELECT * FROM "+TABLE_NAME, null);
            cursor.moveToFirst();

            while (!cursor.isAfterLast()) {
                Object[] avdData = {cursor.getString(0), cursor.getString(1)};
                mC.addRow(avdData);
                cursor.moveToNext();
            }

            cursor.close();
            //sqLiteDatabase.close();
            return mC;

        } else {
            //here the key is with some other avd we need to wait and pass request to next avd to send back
            //Log.e("Query to another avd", hm.get(successorPort));
            //here if query is not with the avd it belongs to, check where the replica exists

            Log.e("inside else", "query");
            String[] arr = getPortsForInsertion(keyToHash);
            String message = selection+"#"+myPort;
            MatrixCursor mR = new MatrixCursor(dataCols);
            dataFound = false;

            List<String> portsMap = Arrays.asList(arr);
            failedNode = "";
            String remote_port;
            String result = "";

            if (portsMap.contains(myPort)) {
                Log.e("exists", "my port");
                result = queryMySelf(selection, myPort);
                Log.e("queryvalavd", queryValAvd);
                dataFound = true;
            }

            if (!dataFound) {
                remote_port = hm.get(arr[0]);
                Log.e("asking 1", remote_port);
                //result = connectToAvd(remote_port, "Query", message);
                try {
                    result = new connectToAvdTask().execute(remote_port, "Query", message).get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                //clientTaskForQuery(remote_port, message, "Query");
                Log.e("failed node in1 asking2", failedNode);

                if (result.equals("failed") || result.equals("")) {
                    remote_port = hm.get(arr[1]);
                    Log.e("asking 2", remote_port);
                    //clientTaskForQuery(remote_port, message, "Query");
                    //result = connectToAvd(remote_port, "Query", message);

                    try {
                        result = new connectToAvdTask().execute(remote_port, "Query", message).get();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    Log.e("failed node in2 asking3", failedNode);
                    if (result.equals("failed") || result.equals("")) {
                        remote_port = hm.get(arr[2]);
                        Log.e("asking 3", remote_port);
                        //result = connectToAvd(remote_port, "Query", message);
                        //clientTaskForQuery(remote_port, message, "Query");

                        try {
                            result = new connectToAvdTask().execute(remote_port, "Query", message).get();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

            Log.e("result is", result);
            String[] p = result.split("&");
            Object[] values = {p[0], p[1]};
            mR.addRow(values);
            return mR;
        }
        //return null;
    }

    public String queryMySelf(String selection, String requestingPort) {
        Log.e("in", "query my self");
        sqLiteDatabase = storageSqliteOpenHelper.getWritableDatabase();
        String[] dataCols = {COLUMN_KEY, COLUMN_VALUE};
        String[] query = {selection};
        String searchParams = COLUMN_KEY + " = ?";

        //get the value from DB and send to the requesting avd
        // Log.e("in check condition", "myport is"+myPort);
        Cursor cursor = sqLiteDatabase.query(TABLE_NAME, dataCols, searchParams, query, null, null, null);
        cursor.moveToFirst();
        Log.e("found data", selection);
        String k = cursor.getString(0);
        String v = cursor.getString(1);
        //now send back to requesting port
        String message = k + "&" + v;
        //Log.e("lauching client", "telling");
       return message;
    }

    public String querySelf(String selection, String requestingPort) {
        Log.e("in", "query self");
        sqLiteDatabase = storageSqliteOpenHelper.getWritableDatabase();
        String[] dataCols = {COLUMN_KEY, COLUMN_VALUE};
        String[] query = {selection};
        String searchParams = COLUMN_KEY + " = ?";

        //get the value from DB and send to the requesting avd
        // Log.e("in check condition", "myport is"+myPort);

        Cursor cursor = sqLiteDatabase.query(TABLE_NAME, dataCols, searchParams, query, null, null, null);
        cursor.moveToFirst();
        Log.e("found data", selection);
        String k = cursor.getString(0);
        String v = cursor.getString(1);
        //now send back to requesting port
        String message = k + "&" + v;
        Log.e("lauching client", "telling "+requestingPort);
        return message;
        //informing port back again
        //launchCTask(hm.get(requestingPort), message, "Queried");
    }

    public void clientTaskForQuery(String remote_port, String message, String check) {
        Log.e("insi clientTaskforQuery", "asking "+remote_port);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, remote_port, message, check);
    }

    //need to check this conditions
    private boolean checkCondition(String keyHash) {
        if (keyHash.compareTo(previousPortHash) > 0 && keyHash.compareTo(myHashValue) <= 0) {
            return true;
        } else if (previousPortHash.compareTo(myHashValue) > 0 && successorPortHash.compareTo(myHashValue) > 0) {
            if ((keyHash.compareTo(previousPortHash)) > 0 || (keyHash.compareTo(myHashValue) <= 0)) {
                return true;
            }
        }
        return false;
    }

    public MatrixCursor fetchOwnData() {
        Log.e("fetching own data", myPort);
        String[] dataCols = {COLUMN_KEY, COLUMN_VALUE};
        MatrixCursor m= new MatrixCursor(dataCols);
        sqLiteDatabase = storageSqliteOpenHelper.getWritableDatabase();
        Cursor cursor = sqLiteDatabase.rawQuery("SELECT * FROM "+TABLE_NAME, null);
        //Log.e("row count", String.valueOf(cursor.getCount()));
        cursor.moveToFirst();
        //Log.e("row count", String.valueOf(cursor.getCount()));
        //Log.e("collecting own data", hm.get(myPort));

        while (!cursor.isAfterLast()) {
            //Log.e("adding result", cursor.getString(0)+" "+cursor.getString(1));
            Object[] avdData = {cursor.getString(0), cursor.getString(1)};
            m.addRow(avdData);
            cursor.moveToNext();
        }

        cursor.close();
        //sqLiteDatabase.close();
        Log.e("returning cursor","back");
        return m;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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

    public void sendToClientTask(String sendMessage, String port) {
        Log.e("Send final result to", hm.get(port));
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, hm.get(port), sendMessage, "Result");
    }

    public void querySelfAndAnotherAvd(String key, String requestingPort) {
        //Log.e("quering key "+key, "request from "+requestingPort+" to "+myPort);

        sqLiteDatabase = storageSqliteOpenHelper.getWritableDatabase();
        String hashRes="";
        String[] dataCols = {COLUMN_KEY, COLUMN_VALUE};
        String[] query = {key};
        String searchParams = COLUMN_KEY + " = ?";

        try {
            hashRes = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (checkCondition(hashRes)) {
            //get the value from DB and send to the requesting avd
            // Log.e("in check condition", "myport is"+myPort);
            Cursor cursor = sqLiteDatabase.query(TABLE_NAME, dataCols, searchParams, query, null, null, null);
            cursor.moveToFirst();
            String k = cursor.getString(0);
            String v = cursor.getString(1);
            //now send back to requesting port
            String message = k+"&"+v;
            //Log.e("lauching client", "telling");
            launchCTask(hm.get(requestingPort), message, "Queried");
        } else {
            //check for next successor
            String message = key+"#"+requestingPort;
            //Log.e("in next queryselfavd", message);
            launchCTask(hm.get(successorPort), message, "Query");
        }
    }

    public void launchCTask(String remote_port, String message, String check) {
        //Log.e("inside launchCT", "final result to "+remote_port);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, remote_port, message, check);
    }

    public void tellPredSucc(String remote_port, String message, String check) {
        //Log.e("inside tellPS", "telling "+remote_port);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, remote_port, message, check);
    }

    public String checkAndInsertAfterRecovery(String requestingPort) {
        requestingPort = hm.get(requestingPort);
        Log.e("in insertion after r", "requesting port "+requestingPort);
        Log.e("data available", String.valueOf(mp.containsKey(requestingPort)));

        String message = "";
        if (mp.containsKey(requestingPort)) {
            List<ContentValues> cv = mp.get(requestingPort);
            //prepare message and send data to requesting port

            for (int i=0;i< cv.size();i++) {
                String tempMessage = cv.get(i).getAsString("key")+"#"+cv.get(i).getAsString("value");
                if (message.equals("")) {
                    message += tempMessage;
                } else {
                    message += "-"+tempMessage;
                }
            }
            //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requestingPort, message, "RecoveryInsertion");
        } else {
            message = "nothing";
        }

        return message;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /** Here we are continuously creating socket coz it breaks after one communication **/
            while (true) {
                try {
                    Log.e("on server", "request received");
                    Socket socket = null;
                    socket = serverSocket.accept();
                    socket.setSoTimeout(2000);
                    //Log.e("on server", "accepted socket");
                    BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    //Log.e("reading line", "now");
                    String message = input.readLine();
                    String[] parts = message.split("\\:");

                    //Log.e("request type is ",parts[0]);

                    if (parts[0].equals("Insert")) {
                        ContentValues cv = new ContentValues();
                        Log.e("data in insert " + message, parts[1]);
                        String[] d = parts[1].split("\\#");
                        String k = d[0];
                        String v = d[1];
                        cv.put("key", k);
                        cv.put("value", v);
                        out.println("Done");
                        out.close();
                        insert(cpUri, cv);
                    } else if (parts[0].equals("DirectInsert")) {
                        ContentValues cv = new ContentValues();
                        //Log.e("data in insert " + message, parts[1]);
                        String[] d = parts[1].split("\\#");
                        String k = d[0];
                        String v = d[1];
                        cv.put("key", k);
                        cv.put("value", v);
                        directInsert(cv);
                        out.println("Done");
                        out.close();
                    } else if (parts[0].equals("Query")) {
                        Log.e("In query", "start");
                        String[] svl = parts[1].split("\\#");
                        String sel = svl[0];
                        String requesting_port = svl[1];
                        //Log.e("sel is "+sel, "req port "+requesting_port);

                        if (sel.equals("*")) {
                            //here we need to return all own data, some AVD requested the data
                            Log.e("* operation from "+requesting_port, myPort);
                            Cursor cursor = fetchOwnData();
                            cursor.moveToFirst();
                            String sendResult = "";
                            int local_count = 0;

                            //Log.e("cursor count server", String.valueOf(cursor.getCount()));
                            while (!cursor.isAfterLast()) {
                                //Log.e("inside while", "adding result");
                                String intermediateResult = cursor.getString(0)+"-"+ cursor.getString(1);

                                //Log.e("intermedite ", intermediateResult);
                                if (local_count == 0) {
                                    sendResult += intermediateResult;
                                } else {
                                    sendResult += "|"+intermediateResult;
                                }
                                local_count++;
                                cursor.moveToNext();
                            }

                            //Log.e("closing", "cursor");
                            cursor.close();

                            String resultMsg="";
                            if (sendResult.equals("")) {
                                resultMsg = requesting_port + "^empty";
                            } else {
                                resultMsg = requesting_port + "^" + sendResult;
                            }

                            //Log.e("writing back from query", resultMsg);
                            out.println(resultMsg);
                            out.close();
                            //return the result back to requesting avd
                            //Log.e("sending * result from "+myPort, "to "+requesting_port);
                            //sendToClientTask(sendResult, requesting_port);

                        } else {
                            //here we need to process query to check if it is on this avd and return the result
                            //Log.e("in else of", "query "+sel);
                            String res =  querySelf(sel, requesting_port);
                            out.println(res);
                            out.close();
                            //querySelfAndAnotherAvd(sel, requesting_port);
                        }
                    } else if (parts[0].equals("Delete")) {
                        if (parts[1].equals("*")) {
                            Log.e("inside delete", "deleting data");
                            sqLiteDatabase = storageSqliteOpenHelper.getWritableDatabase();
                            sqLiteDatabase.delete(TABLE_NAME, null, null);
                            out.println("Done");
                            out.close();
                        } else {
                            Log.e("inside delete", "deleting key");
                            deleteOwnDataWithKey(parts[1]);
                            out.println("Done");
                            out.close();
                            //delete(cpUri, parts[1], null);
                        }
                    } else if (parts[0].equals("Result")) {
                        resultCount++;
                        //Log.e("increasing result count", String.valueOf(resultCount));
                        if (totalResult.equals("")) {
                            totalResult += parts[2];
                        } else {
                            totalResult += "$$"+parts[2];
                        }
                        out.println("Done");
                        out.close();
                    } else if (parts[0].equals("Queried")) {

                        Log.e("inside queried","check");
                        String res = parts[1];
                        queryValAvd = res;
                        dataFound = true;
                        Log.e("got queryvalavd", queryValAvd);
                        out.println("Done");
                        out.close();

                    } else if (parts[0].equals("Alive")) {
                        //check if ny data of this port exists, send back to that port
                        Log.e("in Alive", "from port "+parts[1]);
                        String dt = checkAndInsertAfterRecovery(parts[1]);
                        Log.e("writing back", dt);
                        out.println(dt);
                        out.close();
                    } else if (parts[0].equals("RecoveryInsertion")) {
                        Log.e("recovery insertion", "insertiing");
                        out.println("Done");
                        out.close();
                        directInsertMultiple(parts[1]);
                    }
                } catch (SocketTimeoutException e) {
                    e.printStackTrace();
                } catch (SocketException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                /*
                 * TODO: Fill in your server code that receives messages and passes them
                 * to onProgressUpdate().
                 */
//                return null;
            }
        }

        public String joinChordDHT(String node) {
            //Log.e("node joining", node);
            chordMembers.add(node);
            //Log.e("chordmember size ", String.valueOf(chordMembers.size()));
            //printChordMembers();
            //sort the nodes
            Collections.sort(chordMembers);
            int position = chordMembers.indexOf(node);
            //Log.e("position is", String.valueOf(position));
            String current_successor = "";
            String current_predecesor = "";

            if (position == 0) {
                current_successor = chordMembers.get(1);
                current_predecesor = chordMembers.get(chordMembers.size() - 1);
            } else if (position == (chordMembers.size() - 1)) {
                current_successor = chordMembers.get(0);
                current_predecesor = chordMembers.get(chordMembers.size() - 2);
            } else {
                current_successor = chordMembers.get(position+1);
                current_predecesor = chordMembers.get(position-1);
            }

            //Log.e("current pre "+current_predecesor, "current suc "+current_successor);
            return current_predecesor+"!"+current_successor;
        }

        @Override
        protected void onProgressUpdate(String... values) {
        }
    }

    public void setFromClient(String m) {
        //Log.e("in set from client", m);
        String[] portVals = m.split("%");
        //Log.e("val 1"+portVals[0], "val 2"+portVals[1]);
        previousPortHash = portVals[0];
        successorPortHash = portVals[1];
        //Log.e("previousPort set to "+hashPortMapping.get(previousPortHash), "SuccPort to "+hashPortMapping.get(successorPortHash));
        previousPort = hashPortMapping.get(previousPortHash);
        successorPort = hashPortMapping.get(successorPortHash);
        //Log.e("in client previous port", myPort+" "+previousPort);
        //Log.e("in client sucessor port", myPort+" "+successorPort);
    }

    public void incrementResult(String mes) {
        String[] data = mes.split("\\^");
        //Log.e("in incrementResult", data[1]);
        resultCount++;
        if (!data[1].equals("empty")) {
            Log.e("incrementing result", String.valueOf(resultCount));
            if (totalResult.equals("")) {
                totalResult += data[1];
            } else {
                totalResult += ":" + data[1];
            }
        }
        //Log.e("total_result", totalResult);
    }

    public void storeTempData(String port, ContentValues contentValues) {
        Log.e("storing", "temp data for "+port);

        if (mp.get(port) != null) {
            List<ContentValues> l = mp.get(port);
            l.add(contentValues);
            mp.put(port, l);
        } else {
            List<ContentValues> l = new ArrayList<ContentValues>();
            l.add(contentValues);
            mp.put(port, l);
        }
        //Log.e("mp size is", String.valueOf(mp.get(port).size()));
    }

    public ContentValues getCV(String message){
        ContentValues cv = new ContentValues();
        String[] d = message.split("\\#");
        String k = d[0];
        String v = d[1];
        cv.put("key", k);
        cv.put("value", v);
        return cv;
    }

    public class connectToAvdTask extends AsyncTask<String, String, String> {
        String returnMsg = "";

        @Override
        protected String doInBackground(String... strings) {
            String remote_port = strings[0];
            String request_type = strings[1];
            String msgs = strings[2];

            try {
                //Log.e("sending to port " + remote_port, request_type + " from " + myPort);
                //Log.e("in client", msgs);

                String msg = request_type + ":" + msgs + ":" + myPort;
                //Log.e("in client", "trying to connect to " + remote_port);

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remote_port));

                socket.setSoTimeout(2000);
                //socket.setSoTimeout(2000);

                //Log.e("Connection", "accepted " + remote_port);
                BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                //sending mesage
                //Log.e("writing msg", "to " + remote_port);
                out.println(msg);
                String m = input.readLine();
                //Log.e("Ack from server", m);
                //Log.e("first", "point");
                //Log.e("Connection", "accepted " + remote_port);


                if (m.contains("%")) {
                    //Log.e("setting ports ps", "inside client");
                    //Log.e("m is "+m, "in client");
                    setFromClient(m);
                } else if (m.contains("^")) {
                    //Log.e("inside increment", "result");
                    incrementResult(m);
                } else if (m.contains("&")) {
                    returnMsg = m;
                }


                returnMsg = m;
                //Log.e("closing", "in client");
                out.close();
                socket.close();
                //Log.e("socket closed", "socket "+m);
                /*
                 * TODO: Fill in your client code that sends out a message.
                 */

            } catch (NullPointerException e) {
                failedNode = remote_port;
                if (request_type.equals("DirectInsert")) {
                    //Log.e("Doing", "Direct Insert");
                    ContentValues cv = getCV(msgs);
                    storeTempData(remote_port, cv);
                }
                returnMsg = "failed";
                e.printStackTrace();
            } catch (SocketException e) {
                failedNode = remote_port;
                if (request_type.equals("DirectInsert")) {
                    //Log.e("Doing", "Direct Insert");
                    ContentValues cv = getCV(msgs);
                    storeTempData(remote_port, cv);
                }
                //Log.e("node failed "+failedNode, "First Client exception");
                returnMsg = "failed";
                e.printStackTrace();
            } catch (SocketTimeoutException e) {
                failedNode = remote_port;
                if (request_type.equals("DirectInsert")) {
                    //Log.e("Doing", "Direct Insert");
                    ContentValues cv = getCV(msgs);
                    storeTempData(remote_port, cv);
                }
                //Log.e("node failed "+failedNode, "Second Client exception");
                returnMsg = "failed";
                e.printStackTrace();
            } catch (UnknownHostException e) {
                failedNode = remote_port;
                if (request_type.equals("DirectInsert")) {
                    //Log.e("Doing", "Direct Insert");
                    ContentValues cv = getCV(msgs);
                    storeTempData(remote_port, cv);
                }
                //Log.e("node failed "+failedNode, "Third Client exception");
                returnMsg = "failed";
                e.printStackTrace();
            } catch (Exception e) {
                failedNode = remote_port;
                if (request_type.equals("DirectInsert")) {
                    //Log.e("Doing", "Direct Insert");
                    ContentValues cv = getCV(msgs);
                    storeTempData(remote_port, cv);
                }
                //Log.e("node failed "+failedNode, "Fourth Client exception");
                returnMsg = "failed";
                e.printStackTrace();
            }
            return returnMsg;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String request_type = msgs[2];
            String remote_port =  msgs[0];

            try {

                Log.e("sending to port " + remote_port, request_type + " from " + myPort);
                Log.e("in client", msgs[1]);

                String msg = request_type + ":" + msgs[1] + ":" + myPort;
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remote_port));
                socket.setSoTimeout(2000);
                Log.e("Connection", "accepted " + remote_port);
                BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                //sending mesage
                Log.e("writing msg", "to " + remote_port);
                out.println(msg);
                String m = input.readLine();

                if (m.contains("%")) {
                    //Log.e("setting ports ps", "inside client");
                    //Log.e("m is "+m, "in client");
                    setFromClient(m);
                } else if (m.contains("^")) {
                    //Log.e("inside increment", "result");
                    incrementResult(m);
                }


                Log.e("closing", "in client");
                out.close();
                Log.e("socket closing", "socket");
                socket.close();
                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
            } catch (NullPointerException e) {
                failedNode = remote_port;
                if (request_type.equals("DirectInsert")) {
                    Log.e("Doing", "Direct Insert");
                    ContentValues cv = getCV(msgs[1]);
                    storeTempData(remote_port, cv);
                }
                e.printStackTrace();
            } catch (SocketException e) {
                numberOfLiveDevice--;
                failedNode = remote_port;
                if (request_type.equals("DirectInsert")) {
                    Log.e("Doing", "Direct Insert");
                    ContentValues cv = getCV(msgs[1]);
                    storeTempData(remote_port, cv);
                }
                Log.e("node failed "+failedNode, "First Client exception");
                e.printStackTrace();
            } catch (SocketTimeoutException e) {
                numberOfLiveDevice--;

                failedNode = remote_port;
                if (request_type.equals("DirectInsert")) {
                    Log.e("Doing", "Direct Insert");
                    ContentValues cv = getCV(msgs[1]);
                    storeTempData(remote_port, cv);
                }
                Log.e("node failed "+failedNode, "Second Client exception");
                e.printStackTrace();
            } catch (UnknownHostException e) {
                numberOfLiveDevice--;

                failedNode = remote_port;
                if (request_type.equals("DirectInsert")) {
                    Log.e("Doing", "Direct Insert");
                    ContentValues cv = getCV(msgs[1]);
                    storeTempData(remote_port, cv);
                }
                Log.e("node failed "+failedNode, "Third Client exception");
                e.printStackTrace();
            } catch (Exception e) {
                numberOfLiveDevice--;

                failedNode = remote_port;
                if (request_type.equals("DirectInsert")) {
                    Log.e("Doing", "Direct Insert");
                    ContentValues cv = getCV(msgs[1]);
                    storeTempData(remote_port, cv);
                }
                Log.e("node failed "+failedNode, "Fourth Client exception");
                e.printStackTrace();
            }
            return null;
        }
    }
}
