package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Rex Store client for YCSB.
 * This client communicates with the Rex Store via its TCP protocol
 * with length-prefixed JSON messages.
 * 
 * Modified to get a random node every single time while maintaining connection
 * pool.
 */
public class RexStoreClient extends DB {
    private static final Logger logger = Logger.getLogger(RexStoreClient.class.getName());
    private static final int LENGTH_PREFIX_SIZE = 4;

    private static final ConcurrentHashMap<String, List<Socket>> CONNECTION_POOL = new ConcurrentHashMap<>();

    private static final Set<String> KNOWN_NODES = Collections.synchronizedSet(new HashSet<>());

    private String defaultServer;
    private int maxPoolSize;

    /**
     * Initialize the client.
     */
    @Override
    public void init() throws DBException {
        Properties props = getProperties();

        defaultServer = props.getProperty("rex.server", "127.0.0.1:8000");
        maxPoolSize = Integer.parseInt(props.getProperty("rex.connectionPoolSize", "10"));

        try {
            if (KNOWN_NODES.isEmpty()) {
                KNOWN_NODES.add(defaultServer);
                discoverNodes(defaultServer);
            }

            logger.info("Initialized RexStoreClient with default server: " + defaultServer);
            logger.info("Known cluster nodes: " + KNOWN_NODES);
        } catch (Exception e) {
            throw new DBException("Failed to initialize RexStoreClient", e);
        }
    }

    /**
     * Cleanup any state for this DB.
     */
    @Override
    public void cleanup() throws DBException {
        // No need to maintain a current connection anymore
        // The connection pool will handle all connections
    }

    /**
     * Read a record from the database.
     */
    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        try {
            JSONObject getCommand = new JSONObject();
            JSONObject getParams = new JSONObject();
            getParams.put("key", key);
            getCommand.put("Get", getParams);

            JSONObject response = executeCommandWithRandomNode(getCommand);
            if (response.has("Ok") && response.getJSONObject("Ok").has("Value")) {
                JSONObject valueObj = response.getJSONObject("Ok").getJSONObject("Value");

                if (valueObj.has("found") && valueObj.getBoolean("found")) {
                    if (valueObj.has("value") && !valueObj.isNull("value")) {
                        String value = valueObj.getString("value");
                        result.put("value", new StringByteIterator(value));
                        return Status.OK;
                    }
                }

                return Status.NOT_FOUND;
            }

            logger.warning("Unexpected response format for key " + key + ": " + response.toString());
            return Status.ERROR;
        } catch (DBException e) {
            logger.log(Level.WARNING, "DBException reading key " + key, e);
            return Status.ERROR;
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error reading key " + key, e);
            return Status.ERROR;
        }
    }

    /**
     * Perform a range scan for a set of records.
     */
    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result) {
        // Todo
        return Status.NOT_IMPLEMENTED;
    }

    /**
     * Update a record in the database.
     */
    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        // Update and insert are the same?
        return insert(table, key, values);
    }

    /**
     * Insert a record in the database.
     */
    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        try {
            // We do not support multi-valued keys
            String valueToInsert = "";
            if (!values.isEmpty()) {
                String firstField = values.keySet().iterator().next();
                valueToInsert = values.get(firstField).toString();
            }

            JSONObject setCommand = new JSONObject();
            JSONObject setParams = new JSONObject();
            setParams.put("key", key);
            setParams.put("value", valueToInsert);
            setCommand.put("Set", setParams);

            JSONObject response = executeCommandWithRandomNode(setCommand);

            if (response.has("error")) {
                logger.warning("Error inserting key " + key + ": " + response.getString("error"));
                return Status.ERROR;
            }

            return Status.OK;
        } catch (DBException e) {
            logger.log(Level.WARNING, "DBException inserting key " + key, e);
            return Status.ERROR;
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error inserting key " + key, e);
            return Status.ERROR;
        }
    }

    /**
     * Delete a record from the database.
     */
    @Override
    public Status delete(String table, String key) {
        // TODO?
        return Status.NOT_IMPLEMENTED;
    }

    /**
     * Execute a command against a random node in the Rex Store cluster.
     */
    private JSONObject executeCommandWithRandomNode(JSONObject command) throws IOException, DBException {
        String randomNode = getRandomNode();
        Socket connection = null;

        try {
            connection = getConnection(randomNode);

            byte[] commandBytes = command.toString().getBytes();
            int commandLength = commandBytes.length;

            byte[] lengthPrefix = ByteBuffer.allocate(LENGTH_PREFIX_SIZE).putInt(commandLength).array();

            OutputStream out = connection.getOutputStream();
            out.write(lengthPrefix);
            out.write(commandBytes);
            out.flush();

            InputStream in = connection.getInputStream();
            byte[] responseLengthBytes = new byte[LENGTH_PREFIX_SIZE];
            if (in.read(responseLengthBytes) != LENGTH_PREFIX_SIZE) {
                throw new IOException("Failed to read response length prefix");
            }

            int responseLength = ByteBuffer.wrap(responseLengthBytes).getInt();

            byte[] responseBytes = new byte[responseLength];
            int totalBytesRead = 0;
            while (totalBytesRead < responseLength) {
                int bytesRead = in.read(responseBytes, totalBytesRead, responseLength - totalBytesRead);
                if (bytesRead == -1) {
                    throw new IOException("End of stream reached before reading complete response");
                }
                totalBytesRead += bytesRead;
            }

            String responseString = new String(responseBytes);
            return new JSONObject(responseString);
        } finally {
            if (connection != null) {
                returnConnectionToPool(connection);
            }
        }
    }

    /**
     * Get a random node from the known nodes.
     */
    private String getRandomNode() {
        String[] knownNodesArray = KNOWN_NODES.toArray(new String[0]);
        if (knownNodesArray.length > 0) {
            int randomIndex = ThreadLocalRandom.current().nextInt(knownNodesArray.length);
            return knownNodesArray[randomIndex];
        } else {
            // If no nodes are known, use the default server
            KNOWN_NODES.add(defaultServer);
            return defaultServer;
        }
    }

    /**
     * Get a connection from the pool or create a new one.
     */
    private Socket getConnection(String server) throws IOException {
        List<Socket> connections = CONNECTION_POOL.computeIfAbsent(server, k -> new ArrayList<>());

        synchronized (connections) {
            // Check for existing connections in the pool
            Iterator<Socket> it = connections.iterator();
            while (it.hasNext()) {
                Socket socket = it.next();
                it.remove();

                // Check if connection is still valid
                if (!socket.isClosed() && socket.isConnected()) {
                    return socket;
                } else {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        // Ignore
                    }
                }
            }
        }

        // No valid connection in pool, create a new one
        String[] hostPort = server.split(":");
        String host = hostPort[0];
        int port = Integer.parseInt(hostPort[1]);

        Socket socket = new Socket(host, port);
        socket.setKeepAlive(true);
        socket.setSoTimeout(30000);
        return socket;
    }

    /**
     * Return a connection to the pool.
     */
    private void returnConnectionToPool(Socket connection) {
        if (connection == null || connection.isClosed() || !connection.isConnected()) {
            return;
        }

        String server = connection.getInetAddress().getHostAddress() + ":" + connection.getPort();

        List<Socket> connections = CONNECTION_POOL.computeIfAbsent(server, k -> new ArrayList<>());
        synchronized (connections) {
            if (connections.size() < maxPoolSize) {
                connections.add(connection);
                return;
            }
        }

        // Pool is full, close connection
        try {
            connection.close();
        } catch (IOException e) {
            logger.log(Level.WARNING, "Error closing connection", e);
        }
    }

    /**
     * Discover nodes in the cluster.
     */
    private void discoverNodes(String server) {
        try {
            Socket tempConnection = getConnection(server);

            try {
                JSONObject pingCommand = new JSONObject();
                JSONObject gossipParams = new JSONObject();
                gossipParams.put("Ping", JSONObject.NULL);
                pingCommand.put("Gossip", gossipParams);

                byte[] commandBytes = pingCommand.toString().getBytes();
                int commandLength = commandBytes.length;

                byte[] lengthPrefix = ByteBuffer.allocate(LENGTH_PREFIX_SIZE).putInt(commandLength).array();

                OutputStream out = tempConnection.getOutputStream();
                out.write(lengthPrefix);
                out.write(commandBytes);
                out.flush();

                InputStream in = tempConnection.getInputStream();
                byte[] responseLengthBytes = new byte[LENGTH_PREFIX_SIZE];
                if (in.read(responseLengthBytes) != LENGTH_PREFIX_SIZE) {
                    throw new IOException("Failed to read response length prefix");
                }

                int responseLength = ByteBuffer.wrap(responseLengthBytes).getInt();

                byte[] responseBytes = new byte[responseLength];
                int totalBytesRead = 0;
                while (totalBytesRead < responseLength) {
                    int bytesRead = in.read(responseBytes, totalBytesRead, responseLength - totalBytesRead);
                    if (bytesRead == -1) {
                        throw new IOException("End of stream reached before reading complete response");
                    }
                    totalBytesRead += bytesRead;
                }

                String responseString = new String(responseBytes);
                JSONObject response = new JSONObject(responseString);

                // Parse the nested structure: Ok -> GossipMessage -> Pong -> members
                if (response.has("Ok")) {
                    JSONObject okResponse = response.getJSONObject("Ok");
                    if (okResponse.has("GossipMessage")) {
                        JSONObject gossipMessage = okResponse.getJSONObject("GossipMessage");
                        if (gossipMessage.has("Pong")) {
                            JSONObject pong = gossipMessage.getJSONObject("Pong");

                            // Add the sender node
                            if (pong.has("sender")) {
                                String sender = pong.getString("sender");
                                KNOWN_NODES.add(sender);
                            }

                            // Add the member nodes
                            if (pong.has("members")) {
                                JSONArray members = pong.getJSONArray("members");
                                for (int i = 0; i < members.length(); i++) {
                                    String member = members.getString(i);
                                    KNOWN_NODES.add(member);
                                }
                            }

                            logger.info("Discovered nodes: " + KNOWN_NODES);
                        }
                    }
                }
            } finally {
                returnConnectionToPool(tempConnection);
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error discovering nodes", e);
        }
    }
}