package edu.buffalo.cse.cse486586.simpledynamo;


import java.io.File;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final String[] REMOTE_PORTS = {"11108","11112","11116","11120","11124"};
    static String portStr;
    static SQLiteDatabase database;
    static String TABLE_NAME = "DYNAMOTABLE";
    static int messageCount =0;
    static TreeMap<String,String> connectedAVDMAP;
    static ArrayList<String> AvdList;
    static String myFirstPrevious;
    static String myFirstNext;
    static String mySecondPrevious;
    static String mySecondNext;
    static String myhashedID;
    static String myhashedSecondPrevious;
    static String myhashedFirstPrevious;
    static String myhashedFirstNext;
    static String myhashedSecondNext;
    static HashMap<String,String> GlobalHashMap;
    static boolean waitforResults;
    static int SOCKET_TIMEOUT = 1500;
    static boolean waitforRecovery = true;
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

        Log.d("Delete","######################################");
        Log.d("Delete",selection);
        // handle the @ for delete
        if(selection.equals("@"))
        {
           database.execSQL("delete from " + TABLE_NAME);

        }
        //handle * for delete
        else if(selection.equals("*"))
        {
            database.execSQL("delete  from " + TABLE_NAME);
            for(int i=0;i<REMOTE_PORTS.length;i++)
            {
                Message m = new Message();
                m.msgType = 6;
                try
                {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORTS[i]));
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(m);
                    oos.close();
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
        // handle single key deletion
        else
        {
            try {
                String hashedKey = genHash(selection);
                 // Check if  the actual owner of the key is the one on which delete key is called.
                if(hashedKey.compareTo(myhashedFirstPrevious)>0 && hashedKey.compareTo(myhashedID)<0)
                {
                    Log.d("Delete","Locally Deleting the key");
                    database.execSQL("delete from "+ TABLE_NAME + " where key='" +selection+"'");
                    Message m = new Message();
                    m.key = selection;
                    m.msgType = 7;
                    m.firstNext = myFirstNext;
                    m.secondNext = mySecondNext;
                    String[] ports = {m.firstNext,m.secondNext};
                    // forward this delete request to my two succesors
                    for(int i=0;i<ports.length;i++)
                    {
                        Log.d("Delete", "Forwarding the Delete request to " + ports[i]);
                        try
                        {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(ports[i])*2);
                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            oos.writeObject(m);
                            oos.close();
                            socket.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }



                }
                // handle the corner case where key is greater than the largest AVD or smaller than smallest AVD by hash value.
                else if ((hashedKey.compareTo(genHash(AvdList.get(AvdList.size()-1)))>0 && hashedKey.compareTo(genHash(AvdList.get(0)))>0) || hashedKey.compareTo(genHash(AvdList.get(0)))<0 )
                {
                    // If this delete has come on the smallest AVD by hash value.
                    if(portStr.equals(AvdList.get(0)))
                    {
                        Log.d("Delete","Boundary condition");
                        database.execSQL("delete from "+ TABLE_NAME + " where key='" +selection+"'");

                        Message m = new Message();
                        m.ownerID = portStr;
                        m.msgType = 7;
                        m.firstNext = AvdList.get(1);
                        m.secondNext = AvdList.get(2);
                        m.key = selection;
                        String[] ports = {m.firstNext,m.secondNext};
                        for(int i=0;i<ports.length;i++)
                        {
                            Log.d("Delete", "Forwarding the Delete request to " + ports[i]);
                            try
                            {
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(ports[i])*2);
                                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                                oos.writeObject(m);
                                oos.close();
                                socket.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    // otherwise forward this delete request to smallest AVD and its two succesors.
                    else
                    {

                        Message m = new Message();
                        m.msgType = 7;
                        m.ownerID = AvdList.get(0);
                        m.firstNext = AvdList.get(1);
                        m.secondNext = AvdList.get(2);
                        m.key = selection;
                        String[] ports = {m.firstNext,m.secondNext};
                        Log.d("Delete","Forwarding the delete key " + selection + " to smallest Avd");
                        try
                        {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(m.ownerID)*2);
                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            oos.writeObject(m);
                            oos.close();
                            socket.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        for(int i=0;i<ports.length;i++)
                        {
                            Log.d("Delete", "Forwarding the Delete request to " + ports[i]);
                            try
                            {
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(ports[i])*2);
                                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                                oos.writeObject(m);
                                oos.close();
                                socket.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                }
                // Otherwise find the correct Coordinator and its two successor for key deletion
                else
                {
                    for(int i=0;i<AvdList.size()-1;i++)
                    {
                        if(hashedKey.compareTo(genHash(AvdList.get(i)))>0 && hashedKey.compareTo(genHash(AvdList.get(i+1)))<0)
                        {
                            Message m = new Message();
                            m.msgType = 7;
                            m.ownerID = AvdList.get(i+1);
                            m.firstNext = AvdList.get((i+2)%AvdList.size());
                            m.secondNext = AvdList.get((i+3)%AvdList.size());
                            m.key = selection;
                            String[] ports = {m.firstNext,m.secondNext};
                            Log.d("Delete","Forwarding the delete key " + selection + " to " + m.ownerID);
                            try
                            {
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(m.ownerID)*2);
                                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                                oos.writeObject(m);
                                oos.close();
                                socket.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                            for(int j=0;j<ports.length;j++)
                            {
                                Log.d("Delete", "Forwarding the Delete request to " + ports[j]);
                                try
                                {
                                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(ports[j])*2);
                                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                                    oos.writeObject(m);
                                    oos.close();
                                    socket.close();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                            break;
                        }

                    }

                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

        }
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

        String key = values.get("key").toString();
        String message= (String) values.get("value");
        String path = getContext().getFilesDir().getPath();

        Log.d(TAG,"#################################");
        Log.d(TAG,"Insert Key : " + key + " Value : " + message);
       /* Cursor c1 = database.rawQuery("select * from " + TABLE_NAME, null);
        Log.e("CURSOR-- ",c1.getCount()+" ");

        database.insertWithOnConflict(TABLE_NAME,null,values,SQLiteDatabase.CONFLICT_REPLACE);
        Cursor c = database.rawQuery("select * from " + TABLE_NAME, null);*/
       // Log.e("CURSOR-- ",c.getCount()+" ");
        try {
            // get the hash value for the key
            String hashedKey = genHash(key);
            // check if the coordinator for the key is the AVD on which insert was called.
            if(hashedKey.compareTo(myhashedFirstPrevious)>0 && hashedKey.compareTo(myhashedID)<0)
            {
                Log.d("Insert","Local Insert in to " + portStr);
                values.put("type",portStr);
                database.insertWithOnConflict(TABLE_NAME,null,values,SQLiteDatabase.CONFLICT_REPLACE);
                Message m = new Message();
                m.ownerID = portStr;
                m.msgType = 3;
                m.firstNext = myFirstNext;
                m.secondNext = mySecondNext;
                m.key = key;
                m.value = message;
                // replicate this key and value to its two successor.
                String[] ports = {m.firstNext,m.secondNext};
                for(int i=0;i<ports.length;i++)
                {
                    Log.d("Insert","Sending Replication Request to " + ports[i]);
                    try
                    {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(ports[i])*2);
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        oos.writeObject(m);
                        oos.close();
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }


            }
            // check if the hashed key is greater than the greatest AVD or smaller than smallest AVD by hash value.
            else if ((hashedKey.compareTo(genHash(AvdList.get(AvdList.size()-1)))>0 && hashedKey.compareTo(genHash(AvdList.get(0)))>0) || hashedKey.compareTo(genHash(AvdList.get(0)))<0 )
            {
                Log.d("Insert : ","Boundary Condition");
                // If I am the smallest AVD on which Insert method is called.
                if(portStr.equals(AvdList.get(0)))
                {
                    Log.d("Insert","Inserting in to  Smallest");
                    values.put("type",portStr);
                    database.insertWithOnConflict(TABLE_NAME,null,values,SQLiteDatabase.CONFLICT_REPLACE);
                    Message m = new Message();
                    m.ownerID = portStr;
                    m.msgType = 3;
                    m.firstNext = AvdList.get(1);
                    m.secondNext = AvdList.get(2);
                    m.key = key;
                    m.value = message;
                    String[] ports = {m.firstNext,m.secondNext};
                    // Replicate this key and value to two successors of smallest AVD by hashed value.
                    for(int i=0;i<ports.length;i++)
                    {
                        Log.d("Insert","Sending Replication Request to " + ports[i]);
                        try
                        {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(ports[i])*2);
                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            oos.writeObject(m);
                            oos.close();
                            socket.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
                // Otherwise forward this Key to smallest AVD by hashed value and its two successor.
                else
                {
                    Log.d("Insert","Forwarding to  Smallest");
                    Message m = new Message();
                    m.msgType = 2;
                    m.ownerID = AvdList.get(0);
                    m.firstNext = AvdList.get(1);
                    m.secondNext = AvdList.get(2);
                    m.key = key;
                    m.value = message;
                    String[] ports = {m.firstNext,m.secondNext};
                    try
                    {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(m.ownerID)*2);
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        oos.writeObject(m);
                        oos.close();
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    m.msgType = 3;
                    for(int i=0;i<ports.length;i++)
                    {
                        Log.d("Insert","Sending Replication Request to " + ports[i]);
                        try
                        {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(ports[i])*2);
                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            oos.writeObject(m);
                            oos.close();
                            socket.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            // Find the correct coordinator and its two successor for this key
            else
            {
                for(int i=0;i<AvdList.size()-1;i++)
                {
                    if(hashedKey.compareTo(genHash(AvdList.get(i)))>0 && hashedKey.compareTo(genHash(AvdList.get(i+1)))<0)
                    {
                        Log.d("Insert", "Coordinator for the key " + key + " is " + AvdList.get(i+1));
                        Message m = new Message();
                        m.msgType = 2;
                        m.ownerID = AvdList.get(i+1);
                        m.firstNext = AvdList.get((i+2)%AvdList.size());
                        m.secondNext = AvdList.get((i+3)%AvdList.size());
                        m.key = key;
                        m.value = message;
                        String[] ports = {m.firstNext,m.secondNext};
                        try
                        {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(m.ownerID)*2);
                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            oos.writeObject(m);
                            oos.close();
                            socket.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        // forward the replication request for this key value to two successor of the coordinator
                        m.msgType = 3;
                        for(int j=0;j<ports.length;j++)
                        {
                            Log.d("Insert","Sending Replication Request to " + ports[j]);
                            try
                            {
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(ports[j])*2);
                                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                                oos.writeObject(m);
                                oos.close();
                                socket.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        break;
                    }

                }
            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return null;
	}


	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
        connectedAVDMAP = new TreeMap<>();
        GlobalHashMap = new HashMap<>();
        AvdList = new ArrayList<>();
        waitforResults = true;
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try {
            myhashedID = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        // set my two predecessor and two successor
        setMyPreviousAndNext();
        Context context = getContext();
        MySimpleDynamoDBHelper dynmodb = new MySimpleDynamoDBHelper(context);
        database = dynmodb.getWritableDatabase();
        // make a folder to check failure of the AVD.
        // If folder exists, this mean that AVD is recovering from failure.
        // Otherwise , it is starting from fresh install.
        File failureDetection = getContext().getFileStreamPath("FailureCheckFile");
        if(!failureDetection.exists())
        {
            Log.d("OnCreate","Starting for the first time");
            failureDetection.mkdir();
        }
        else {
            // handle the failure part
            Message m = new Message();
            m.msgType = 8;
            m.senderID = portStr;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,m,null);
            while(waitforRecovery)
            {
                //wait for recovery to finish
            }

        }

        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }
		return false;
	}
    public void setMyPreviousAndNext()
    {
        if(portStr.equals("5554"))
        {
            mySecondPrevious = "5562";
            myFirstPrevious = "5556";
            myFirstNext = "5558";
            mySecondNext = "5560";

        }
        else if(portStr.equals("5556"))
        {
            mySecondPrevious = "5560";
            myFirstPrevious = "5562";
            myFirstNext = "5554";
            mySecondNext = "5558";
        }
        else if(portStr.equals("5558"))
        {
            mySecondPrevious = "5556";
            myFirstPrevious = "5554";
            myFirstNext = "5560";
            mySecondNext = "5562";

        }
        else if(portStr.equals("5560"))
        {
            mySecondPrevious = "5554";
            myFirstPrevious = "5558";
            myFirstNext = "5562";
            mySecondNext = "5556";

        }
        else if(portStr.equals("5562"))
        {
            mySecondPrevious = "5558";
            myFirstPrevious = "5560";
            myFirstNext = "5556";
            mySecondNext = "5554";
        }
        try
        {
            myhashedID = genHash(portStr);
            myhashedSecondPrevious = genHash(mySecondPrevious);
            myhashedFirstPrevious = genHash(myFirstPrevious);
            myhashedFirstNext = genHash(myFirstNext);
            myhashedSecondNext = genHash(mySecondNext);
            AvdList.add("5562");
            AvdList.add("5556");
            AvdList.add("5554");
            AvdList.add("5558");
            AvdList.add("5560");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }
	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub


        Log.d("Query","#####################################");
        Log.d("Query",selection);

        // handle the @ query
        if(selection.equals("\"@\""))
        {
            Log.d("Query","@ Query");
            Cursor cursor = database.rawQuery("select key,value from " + TABLE_NAME, null);

            return cursor;
        }
        // handle * query
        else if(selection.equals("\"*\""))
        {
            Log.d("Query","* Query");
            HashMap<String,String> starQueryMap = new HashMap<>();
            String myPort= Integer.toString(Integer.parseInt(portStr)*2);
            MatrixCursor matrixcursor = new MatrixCursor(new String[]{"key","value"});
            // send the * query to every AVD except myself.
            for(int i=0;i<REMOTE_PORTS.length;i++)
            {
                if(!REMOTE_PORTS[i].equals(myPort))
                {
                    Log.d("Query","Sending the * Query to " + REMOTE_PORTS[i]);
                    Message m = new Message();
                    m.msgType = 5;

                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(REMOTE_PORTS[i]));
                        socket.setSoTimeout(2500);
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        oos.writeObject(m);
                        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                        Message queryResponse = (Message)ois.readObject();
                        Log.d("Query","Receving the response for * query");
                        for(Map.Entry<String,String> entry : queryResponse.resultMap.entrySet())
                        {
                            String key = entry.getKey();
                            String value = entry.getValue();
                            Log.d("Query","Adding key " + key + " and value " + value + " to matrix cursor");
                            starQueryMap.put(key,value);

                        }
                        ois.close();
                        oos.close();
                        socket.close();

                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
            // get my own data from the DB and add it to the cursor.
            Cursor cursor = database.rawQuery("select key,value from " + TABLE_NAME, null);
            Log.d("Query","Count for Current Data " + cursor.getCount());
            if(cursor.getCount()>0)
            {
                cursor.moveToFirst();
                Log.d("Query","Adding Local data to * Query");
                do{

                    Log.d("Query","Adding " + cursor.getString(cursor.getColumnIndex("key")) +" " + cursor.getString(cursor.getColumnIndex("value")));
                    starQueryMap.put(cursor.getString(cursor.getColumnIndex("key")),cursor.getString(cursor.getColumnIndex("value")));

                }while(cursor.moveToNext());
            }
            // add data from the hash map to matrix cursor
            for(Map.Entry<String,String> entry : starQueryMap.entrySet())
            {
                String key = entry.getKey();
                String value = entry.getValue();
                Log.d("Query","Adding key " + key + " and value " + value + " to matrix cursor");
                matrixcursor.addRow(new Object[]{key,value});

            }


            return matrixcursor;

        }
        // handle the query for single key
        else
        {
            try
            {
                String hashedKey = genHash(selection);
                // check if the AVD on which query is called , is coordinator for the key
                if(hashedKey.compareTo(myhashedFirstPrevious)>0 && hashedKey.compareTo(myhashedID)<0)
                {
                    MatrixCursor matrixcursor = new MatrixCursor(new String[]{"key","value"});
                    Log.d("Query : ", "Handling the  Query locally ");
                    Cursor cursor = database.rawQuery("select key,value from " + TABLE_NAME + " where key ='" + selection+"'" , null);
                    if(cursor.getCount()>0)
                    {
                        cursor.moveToFirst();
                        String key = cursor.getString(cursor.getColumnIndex("key"));
                        String value = cursor.getString(cursor.getColumnIndex("value"));
                        Log.d("Query","Adding key " + key + " and value " + value + " to matrix cursor");
                        matrixcursor.addRow(new Object[]{key,value});
                    }

                    return matrixcursor;
                }
                // check if the  coordinator for the key is the smallest AVD by hash value of port
                else if ((hashedKey.compareTo(genHash(AvdList.get(AvdList.size()-1)))>0 && hashedKey.compareTo(genHash(AvdList.get(0)))>0) || hashedKey.compareTo(genHash(AvdList.get(0)))<0 )
                {
                    Log.d("Query : ","Boundary Condition");
                    // if the current AVD is the smallest AVD by hashed value of the port
                    if(portStr.equals(AvdList.get(0)))
                    {
                        MatrixCursor matrixcursor = new MatrixCursor(new String[]{"key","value"});
                        Log.d("Query : ", "Local Query");
                        Cursor cursor = database.rawQuery("select key,value from " + TABLE_NAME + " where key ='" + selection+"'" , null);
                        if(cursor.getCount()>0)
                        {
                            cursor.moveToFirst();
                            String key = cursor.getString(cursor.getColumnIndex("key"));
                            String value = cursor.getString(cursor.getColumnIndex("value"));
                            Log.d("Query","Adding key " + key + " and value " + value + " to matrix cursor");
                            matrixcursor.addRow(new Object[]{key,value});
                            return matrixcursor;
                        }


                    }
                    // if not , send the query request to smallest AVD.
                    else
                    {
                        Log.d("Query","Forwarding the Query to smallest AVD");
                        Message m = new Message();
                        m.msgType = 4;
                        m.ownerID = AvdList.get(0);
                        m.key = selection;
                        m.firstNext = AvdList.get(1);
                        m.secondNext= AvdList.get(2);
                        Socket socket = null;
                        try {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(m.ownerID)*2);
                            socket.setSoTimeout(2500);
                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            oos.writeObject(m);
                            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                            Message queryResponse = (Message)ois.readObject();
                            MatrixCursor matrixcursor = new MatrixCursor(new String[]{"key","value"});
                            Log.d("Query","Adding key " + queryResponse.key + " and value " + queryResponse.value + " to matrix cursor");
                            matrixcursor.addRow(new Object[]{queryResponse.key,queryResponse.value});
                            oos.flush();
                            oos.close();
                            ois.close();
                            socket.close();
                            return matrixcursor;

                        } catch (IOException e) {
                            e.printStackTrace();
                            //send the query to Coordinator's  first Next AVD
                            try {
                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(m.firstNext)*2);
                                socket.setSoTimeout(2500);
                                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                                oos.writeObject(m);
                                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                                Message queryResponse = (Message)ois.readObject();
                                MatrixCursor matrixcursor = new MatrixCursor(new String[]{"key","value"});
                                Log.d("Query","Adding key " + queryResponse.key + " and value " + queryResponse.value + " to matrix cursor");
                                matrixcursor.addRow(new Object[]{queryResponse.key,queryResponse.value});
                                oos.flush();
                                oos.close();
                                ois.close();
                                socket.close();
                                return matrixcursor;

                            } catch (IOException e1) {
                                e.printStackTrace();
                            } catch (ClassNotFoundException e2) {
                                e.printStackTrace();
                            }
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }


                    }

                }
                else
                {
                    for(int i=0;i<AvdList.size()-1;i++)
                    {
                        if(hashedKey.compareTo(genHash(AvdList.get(i)))>0 && hashedKey.compareTo(genHash(AvdList.get(i+1)))<0)
                        {
                            Log.d("Query", "Coordinator for the key " + selection + " is " + AvdList.get(i+1));
                            Message m = new Message();
                            m.msgType = 4;
                            m.ownerID = AvdList.get(i+1);
                            m.firstNext = AvdList.get((i+2)%AvdList.size());
                            m.secondNext = AvdList.get((i+3)%AvdList.size());
                            m.key = selection;

                            try
                            {
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(m.ownerID)*2);
                                socket.setSoTimeout(2500);
                                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                                oos.writeObject(m);
                                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                                Message queryResponse = (Message)ois.readObject();
                                MatrixCursor matrixcursor = new MatrixCursor(new String[]{"key","value"});
                                Log.d("Query","Received response for key " + selection);
                                Log.d("Query","Adding key " + queryResponse.key + " and value " + queryResponse.value + " to matrix cursor");
                                matrixcursor.addRow(new Object[]{queryResponse.key,queryResponse.value});
                                oos.close();
                                ois.close();
                                socket.close();
                                return  matrixcursor;
                            } catch (IOException e) {
                                e.printStackTrace();
                                // Coordinator failed send the query to first next of the coordinator.
                                try
                                {
                                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(m.firstNext)*2);
                                    socket.setSoTimeout(2500);
                                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                                    oos.writeObject(m);
                                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                                    Message queryResponse = (Message)ois.readObject();
                                    MatrixCursor matrixcursor = new MatrixCursor(new String[]{"key","value"});
                                    Log.d("Query","Received response for key " + selection);
                                    Log.d("Query","Adding key " + queryResponse.key + " and value " + queryResponse.value + " to matrix cursor");
                                    matrixcursor.addRow(new Object[]{queryResponse.key,queryResponse.value});
                                    oos.close();
                                    ois.close();
                                    socket.close();
                                    return  matrixcursor;
                                } catch (IOException e1) {
                                    e.printStackTrace();
                                } catch (ClassNotFoundException e1) {
                                    e.printStackTrace();
                                }
                            } catch (ClassNotFoundException e) {
                                e.printStackTrace();
                            }

                            break;
                        }

                    }
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
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
    private class ServerTask extends AsyncTask<ServerSocket, String, Void>{

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try{
                while(true)
                {
                    Socket acceptSocket = serverSocket.accept();
                    ObjectInputStream ois = new ObjectInputStream(acceptSocket.getInputStream());
                    Message receivedMessage = (Message)ois.readObject();

                    if(receivedMessage.msgType == 2)
                    {
                        Log.d(TAG,"received Coordinator Insert for key : " + receivedMessage.key + " value : " +receivedMessage.value);
                        ContentValues cv = new ContentValues();
                        cv.put("key",receivedMessage.key);
                        cv.put("value",receivedMessage.value);
                        cv.put("type",receivedMessage.ownerID);
                        database.insertWithOnConflict(TABLE_NAME,null,cv,SQLiteDatabase.CONFLICT_REPLACE);

                    }
                    else if(receivedMessage.msgType == 3)  //for first and second replication message
                    {
                       Log.d(TAG,"received First Replication  for key : " + receivedMessage.key + " value : " +receivedMessage.value);
                        ContentValues cv = new ContentValues();
                        cv.put("key",receivedMessage.key);
                        cv.put("value",receivedMessage.value);
                        cv.put("type",receivedMessage.ownerID);
                        database.insertWithOnConflict(TABLE_NAME,null,cv,SQLiteDatabase.CONFLICT_REPLACE);
                    }
                    else if (receivedMessage.msgType == 4)
                    {
                        Log.d("Server Task :", " Received  Query for " + receivedMessage.key);
                        Cursor cursor = database.rawQuery("select key,value from " + TABLE_NAME + " where key = '" + receivedMessage.key+"'" , null);
                        if(cursor.getCount()>0)
                        {
                            cursor.moveToFirst();
                            String key = cursor.getString(cursor.getColumnIndex("key"));
                            String value = cursor.getString(cursor.getColumnIndex("value"));
                            Log.d("Server Task:","Added Key " + key + " Value " + value + " to Response");
                            receivedMessage.key = key;
                            receivedMessage.value = value;
                            Log.d("Server Task :", "Sending the Response Back for the Query");
                            ObjectOutputStream oos = new ObjectOutputStream(acceptSocket.getOutputStream());
                            oos.writeObject(receivedMessage);
                            oos.close();
                            acceptSocket.close();
                        }

                    }
                    else if(receivedMessage.msgType == 5)
                    {
                        Log.d("Server Task :", " Received * Query");
                        Cursor cursor = database.rawQuery("select key,value from " + TABLE_NAME, null);
                        Log.d("Server Task :","Cursor Count " + cursor.getCount());
                        if (cursor.getCount()>0)
                        {
                            cursor.moveToFirst();
                            do{
                                Log.d("ServerTask :" , "Adding " + cursor.getString(cursor.getColumnIndex("key")) + " " + cursor.getString(cursor.getColumnIndex("value")));
                                receivedMessage.resultMap.put(cursor.getString(cursor.getColumnIndex("key")),cursor.getString(cursor.getColumnIndex("value")));
                            }while(cursor.moveToNext());

                            Log.d("Server Task :", "Sending the Response Back for the Query");
                            ObjectOutputStream oos = new ObjectOutputStream(acceptSocket.getOutputStream());
                            oos.writeObject(receivedMessage);
                            oos.close();
                            acceptSocket.close();
                        }

                    }
                    else if(receivedMessage.msgType == 6)
                    {
                        Log.d("Server Task :", " Received * Delete");
                        database.execSQL("delete from " +TABLE_NAME);

                    }
                    else if(receivedMessage.msgType == 7)
                    {
                        Log.d("Server Task :", " Received  Delete for the key " +receivedMessage.key);
                        database.execSQL("delete from " +TABLE_NAME +" where key= '" + receivedMessage.key+"'");
                    }
                    else if(receivedMessage.msgType == 8)
                    {
                        Log.d("Server Task :", " Received Recovery Request to transfer my own data");
                        Cursor cursor = database.rawQuery("select key,value from " + TABLE_NAME + " where type ='" + portStr+"'" , null);
                        Log.d("Server Task :","Cursor Count " + cursor.getCount());
                        if (cursor.getCount()>0)
                        {
                            cursor.moveToFirst();
                            do{
                                Log.d("ServerTask :" , "Adding " + cursor.getString(cursor.getColumnIndex("key")) + " " + cursor.getString(cursor.getColumnIndex("value")));
                                receivedMessage.resultMap.put(cursor.getString(cursor.getColumnIndex("key")),cursor.getString(cursor.getColumnIndex("value")));
                            }while(cursor.moveToNext());

                            Log.d("Server Task :", "Sending the Response Back for the Query");
                            ObjectOutputStream oos = new ObjectOutputStream(acceptSocket.getOutputStream());
                            oos.writeObject(receivedMessage);
                            oos.close();
                            acceptSocket.close();
                        }
                    }
                    else if(receivedMessage.msgType == 9)
                    {
                        Log.d("Server Task :", " Received Recovery Request to transfer my  data of " + receivedMessage.ownerID);
                        Cursor cursor = database.rawQuery("select key,value from " + TABLE_NAME + " where type ='" + receivedMessage.ownerID+"'" , null);
                        Log.d("Server Task :","Cursor Count " + cursor.getCount());
                        if (cursor.getCount()>0)
                        {
                            cursor.moveToFirst();
                            do{
                                Log.d("ServerTask :" , "Adding " + cursor.getString(cursor.getColumnIndex("key")) + " " + cursor.getString(cursor.getColumnIndex("value")));
                                receivedMessage.resultMap.put(cursor.getString(cursor.getColumnIndex("key")),cursor.getString(cursor.getColumnIndex("value")));
                            }while(cursor.moveToNext());

                            Log.d("Server Task :", "Sending the Response Back for the Query");
                            ObjectOutputStream oos = new ObjectOutputStream(acceptSocket.getOutputStream());
                            oos.writeObject(receivedMessage);
                            oos.close();
                            acceptSocket.close();
                        }
                    }
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (OptionalDataException e) {
                e.printStackTrace();
            } catch (StreamCorruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    public class ClientTask extends AsyncTask<Message, Void, Void>{

        @Override
        protected Void doInBackground(Message... msgs) {

            Log.d("OnCreate", "Starting from the failure");
            Log.d("OnCreate", "After getWritableDatabase");
            Log.d("OnCreate", "Requesting to my Two Previous");
            String[] ports = {mySecondPrevious, myFirstPrevious};
            SQLiteDatabase db = SQLiteDatabase.openDatabase("/data/data/edu.buffalo.cse.cse486586.simpledynamo/databases/SimpleDynamo",null, 0);

            for(int i=0;i<ports.length;i++)
            {
                Log.d("OnCreate","Inside For Loop");
                Log.d("Oncreate"," ");
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(ports[i])*2);
                    socket.setSoTimeout(2500);
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(msgs[0]);
                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    Message queryResponse = (Message)ois.readObject();
                    Log.d("Recovery","Receiving the Replication data from " + ports[i]);
                    for(Map.Entry<String,String> entry : queryResponse.resultMap.entrySet())
                    {
                        String key = entry.getKey();
                        String value = entry.getValue();
                        Log.d("Recovery","Adding key " + key + " and value " + value + " to Database");
                        ContentValues values = new ContentValues();
                        values.put("key",key);
                        values.put("value",value);
                        values.put("type",ports[i]);
                        db.insertWithOnConflict(TABLE_NAME,null,values,SQLiteDatabase.CONFLICT_REPLACE);

                    }
                    ois.close();
                    oos.close();
                    socket.close();

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
            // get my own data from my first Next
            msgs[0].msgType = 9;
            msgs[0].ownerID = portStr;
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(myFirstNext)*2);
                socket.setSoTimeout(2500);
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject(msgs[0]);
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                Message queryResponse = (Message)ois.readObject();
                Log.d("Recovery","Receiving My data from " + myFirstNext + " while I was down");
                for(Map.Entry<String,String> entry : queryResponse.resultMap.entrySet())
                {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    Log.d("Recovery","Adding key " + key + " and value " + value + " to Database");
                    ContentValues values = new ContentValues();
                    values.put("key",key);
                    values.put("value",value);
                    values.put("type",portStr);
                    db.insertWithOnConflict(TABLE_NAME,null,values,SQLiteDatabase.CONFLICT_REPLACE);

                }
                ois.close();
                oos.close();
                socket.close();

            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            // recovery finshed . reset the boolean
            waitforRecovery = false;
            Log.d("Recovery"," Recovery finished");
            return null;
        }
    }
    private static class MySimpleDynamoDBHelper extends SQLiteOpenHelper{

        MySimpleDynamoDBHelper(Context context)
        {

            super(context, "SimpleDynamo", null, 2);

        }
        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL("create table " + TABLE_NAME + "( key TEXT PRIMARY KEY,value TEXT,type INTEGER" + ")");
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            db.execSQL("drop table " + TABLE_NAME +"IF EXISTS");
            onCreate(db);
        }
    }
}
