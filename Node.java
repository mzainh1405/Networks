// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  Muhammed-Zain
//  230025299
//  MUHAMMED.HAMID@city.ac.uk

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Stack;
import java.util.Random;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

// DO NOT EDIT starts
interface NodeInterface {
    public void setNodeName(String nodeName) throws Exception;
    public void openPort(int portNumber) throws Exception;
    public void handleIncomingMessages(int delay) throws Exception;
    public boolean isActive(String nodeName) throws Exception;
    public void pushRelay(String nodeName) throws Exception;
    public void popRelay() throws Exception;
    public boolean exists(String key) throws Exception;
    public String read(String key) throws Exception;
    public boolean write(String key, String value) throws Exception;
    public boolean CAS(String key, String currentValue, String newValue) throws Exception;
}
// DO NOT EDIT ends

public class Node implements NodeInterface {

    // STEP 3: Node state

    private String nodeName;
    private String nodeHashID;
    private DatagramSocket socket;
    private int port;
    private HashMap<String, String> addressStore = new HashMap<>();
    private HashMap<String, String> dataStore = new HashMap<>();
    private Stack<String> relayStack = new Stack<>();
    private HashMap<String, byte[]> pendingResponses = new HashMap<>();
    private Random random = new Random();
    private HashSet<String> processedRequests = new HashSet<>();
    private HashMap<String, Object[]> relayMap = new HashMap<>();

    // STEP 1: String encoding/decoding

    public static String encodeString(String s) {
        int spaces = 0;
        for (char c : s.toCharArray()) if (c == ' ') spaces++;
        return spaces + " " + s + " ";
    }

    public static String decodeString(String encoded) {
        int firstSpace = encoded.indexOf(' ');
        if (firstSpace == -1) return encoded;
        String content = encoded.substring(firstSpace + 1);
        if (content.length() == 0) return "";
        return content.substring(0, content.length() - 1);
    }

    // STEP 2: SHA-256 hashing + distance calculation

    public static String computeHashID(String s) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] hash = md.digest(s.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (byte b : hash) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    public static int computeDistance(String hashID1, String hashID2) {
        int matchingBits = 0;
        for (int i = 0; i < hashID1.length(); i++) {
            int xor = Character.digit(hashID1.charAt(i), 16) ^ Character.digit(hashID2.charAt(i), 16);
            if (xor == 0) {
                matchingBits += 4;
            } else {
                if ((xor & 0x8) == 0) matchingBits++;
                if ((xor & 0x8) == 0 && (xor & 0x4) == 0) matchingBits++;
                if ((xor & 0xC) == 0 && (xor & 0x2) == 0) matchingBits++;
                break;
            }
        }
        return 256 - matchingBits;
    }

    // STEP 3: Node setup

    public void setNodeName(String nodeName) throws Exception {
        if (nodeName == null || !nodeName.startsWith("N:")) throw new Exception("Invalid node name");
        this.nodeName = nodeName;
        this.nodeHashID = computeHashID(nodeName);
    }

    public void openPort(int portNumber) throws Exception {
        this.port = portNumber;
        this.socket = new DatagramSocket(portNumber);
        addressStore.put(nodeName, InetAddress.getLocalHost().getHostAddress() + ":" + portNumber);
    }

    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName);
    }

    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) relayStack.pop();
    }

    // STEP 4: Message infrastructure + Name messages (G/H)

    private byte[] generateTxID() {
        byte[] txID = new byte[2];
        do { txID[0] = (byte) random.nextInt(256); } while (txID[0] == 0x20);
        do { txID[1] = (byte) random.nextInt(256); } while (txID[1] == 0x20);
        return txID;
    }

    private String txIDToKey(byte[] txID) {
        return String.format("%02x%02x", txID[0] & 0xFF, txID[1] & 0xFF);
    }

    private byte[] buildMessage(byte[] txID, String content) throws Exception {
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        byte[] msg = new byte[3 + contentBytes.length];
        msg[0] = txID[0];
        msg[1] = txID[1];
        msg[2] = 0x20;
        System.arraycopy(contentBytes, 0, msg, 3, contentBytes.length);
        return msg;
    }

    private void sendPacket(String address, byte[] data) throws Exception {
        String[] parts = address.split(":");
        InetAddress ip = InetAddress.getByName(parts[0]);
        int p = Integer.parseInt(parts[1]);
        socket.send(new DatagramPacket(data, data.length, ip, p));
    }

    private DatagramPacket receivePacket() throws Exception {
        byte[] buf = new byte[65536];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        try {
            socket.receive(packet);
            return packet;
        } catch (java.net.SocketTimeoutException e) {
            return null;
        }
    }

    private void processPacket(DatagramPacket packet) throws Exception {
        try {
            byte[] data = packet.getData();
            int len = packet.getLength();
            if (len < 4) return;

            // Validate txID bytes are not spaces
            if (data[0] == 0x20 || data[1] == 0x20) return;
            // Validate separator is a space
            if (data[2] != 0x20) return;

            byte[] txID = new byte[]{data[0], data[1]};
            String txKey = txIDToKey(txID);
            char type = (char)(data[3] & 0xFF);
            String senderAddress = packet.getAddress().getHostAddress() + ":" + packet.getPort();

            // Deduplicate incoming requests
            String requestKey = txKey + type;
            boolean isRequest = "GNERWCV".indexOf(type) >= 0;
            if (isRequest) {
                if (processedRequests.contains(requestKey)) return;
                processedRequests.add(requestKey);
            }

            // Handle relay with raw bytes before any String conversion
            if (type == 'V') {
                handleRelay(txID, data, 4, len, senderAddress);
                return;
            }

            String rest = new String(data, 4, len - 4, StandardCharsets.UTF_8);

            switch (type) {
                case 'G':
                    sendPacket(senderAddress, buildMessage(txID, "H" + encodeString(nodeName)));
                    break;
                case 'N':
                    handleNearest(txID, rest, senderAddress);
                    break;
                case 'E':
                    handleExists(txID, rest, senderAddress);
                    break;
                case 'R':
                    handleRead(txID, rest, senderAddress);
                    break;
                case 'W':
                    handleWrite(txID, rest, senderAddress);
                    break;
                case 'C':
                    handleCAS(txID, rest, senderAddress);
                    break;
                default:
                    byte[] copy = new byte[len];
                    System.arraycopy(data, 0, copy, 0, len);
                    if (relayMap.containsKey(txKey)) {
                        Object[] relayInfo = relayMap.remove(txKey);
                        byte[] originalTxID = (byte[]) relayInfo[0];
                        String originalSender = (String) relayInfo[1];
                        copy[0] = originalTxID[0];
                        copy[1] = originalTxID[1];
                        sendPacket(originalSender, copy);
                    } else {
                        pendingResponses.put(txKey, copy);
                    }
                    break;
            }
        } catch (Exception e) {
            // STEP 10: Swallow all errors from malformed packets — never crash
        }
    }

    private byte[] sendRequest(String address, byte[] message) throws Exception {
        return sendRequest(address, null, message);
    }

    private byte[] sendRequest(String targetAddress, String targetName, byte[] message) throws Exception {
        if (targetAddress == null) return null;
        byte[] msgToSend = message;
        String addrToSend = targetAddress;

        if (!relayStack.isEmpty() && targetName != null) {
            ArrayList<String> relays = new ArrayList<>(relayStack);
            byte[] currentMsg = message;
            for (int i = relays.size() - 1; i >= 0; i--) {
                String forwardTo = (i == relays.size() - 1) ? targetName : relays.get(i + 1);
                byte[] prefix = ("V" + encodeString(forwardTo)).getBytes(StandardCharsets.UTF_8);
                byte[] newTxID = generateTxID();
                byte[] wrapped = new byte[3 + prefix.length + currentMsg.length];
                wrapped[0] = newTxID[0];
                wrapped[1] = newTxID[1];
                wrapped[2] = 0x20;
                System.arraycopy(prefix, 0, wrapped, 3, prefix.length);
                System.arraycopy(currentMsg, 0, wrapped, 3 + prefix.length, currentMsg.length);
                currentMsg = wrapped;
            }
            msgToSend = currentMsg;
            addrToSend = addressStore.get(relays.get(0));
            if (addrToSend == null) return null;
        }

        String txKey = txIDToKey(new byte[]{msgToSend[0], msgToSend[1]});
        for (int attempt = 0; attempt < 3; attempt++) {
            sendPacket(addrToSend, msgToSend);
            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline) {
                int remaining = (int)(deadline - System.currentTimeMillis());
                if (remaining <= 0) break;
                socket.setSoTimeout(remaining);
                DatagramPacket pkt = receivePacket();
                if (pkt == null) break;
                processPacket(pkt);
                if (pendingResponses.containsKey(txKey)) return pendingResponses.remove(txKey);
            }
        }
        return null;
    }

    public void handleIncomingMessages(int delay) throws Exception {
        if (delay == 0) {
            socket.setSoTimeout(0);
            while (true) {
                DatagramPacket packet = receivePacket();
                if (packet != null) processPacket(packet);
            }
        } else {
            long deadline = System.currentTimeMillis() + delay;
            while (System.currentTimeMillis() < deadline) {
                int remaining = (int)(deadline - System.currentTimeMillis());
                socket.setSoTimeout(remaining);
                DatagramPacket packet = receivePacket();
                if (packet == null) break;
                processPacket(packet);
            }
        }
    }

    public boolean isActive(String nodeName) throws Exception {
        String address = addressStore.get(nodeName);
        if (address == null) return false;
        byte[] txID = generateTxID();
        byte[] response = sendRequest(address, nodeName, buildMessage(txID, "G"));
        if (response == null || response.length < 4) return false;
        if ((char)response[3] != 'H') return false;
        String returnedName = decodeString(new String(response, 4, response.length - 4, StandardCharsets.UTF_8));
        return returnedName.equals(nodeName);
    }

    // STEP 5: Nearest messages (N/O)

    private ArrayList<String[]> getClosestNodes(String targetHashID, int count) throws Exception {
        ArrayList<String[]> candidates = new ArrayList<>();
        for (String name : addressStore.keySet()) {
            String hash = computeHashID(name);
            int dist = computeDistance(targetHashID, hash);
            candidates.add(new String[]{name, addressStore.get(name), String.valueOf(dist)});
        }
        Collections.sort(candidates, Comparator.comparingInt(a -> Integer.parseInt(a[2])));
        return new ArrayList<>(candidates.subList(0, Math.min(count, candidates.size())));
    }

    private void handleNearest(byte[] txID, String rest, String senderAddress) throws Exception {
        String targetHashID = rest.trim();
        if (targetHashID.length() != 64) return;
        ArrayList<String[]> closest = getClosestNodes(targetHashID, 3);
        StringBuilder sb = new StringBuilder("O");
        for (String[] entry : closest) {
            sb.append(encodeString(entry[0]));
            sb.append(encodeString(entry[1]));
        }
        sendPacket(senderAddress, buildMessage(txID, sb.toString()));
    }

    private ArrayList<String[]> sendNearest(String address, String name, String targetHashID) throws Exception {
        byte[] txID = generateTxID();
        byte[] response = sendRequest(address, name, buildMessage(txID, "N" + targetHashID));
        ArrayList<String[]> results = new ArrayList<>();
        if (response == null || response.length < 4) return results;
        if ((char)response[3] != 'O') return results;

        String body = new String(response, 4, response.length - 4, StandardCharsets.UTF_8);
        int i = 0;
        while (i < body.length()) {
            int sp1 = body.indexOf(' ', i);
            if (sp1 == -1) break;
            int nameSpaces;
            try { nameSpaces = Integer.parseInt(body.substring(i, sp1)); }
            catch (NumberFormatException e) { break; }
            int nameStart = sp1 + 1;
            int nameEnd = findEncodedEnd(body, nameStart, nameSpaces);
            if (nameEnd >= body.length()) break;
            String parsedName = body.substring(nameStart, nameEnd);
            i = nameEnd + 1;

            int sp2 = body.indexOf(' ', i);
            if (sp2 == -1) break;
            int addrSpaces;
            try { addrSpaces = Integer.parseInt(body.substring(i, sp2)); }
            catch (NumberFormatException e) { break; }
            int addrStart = sp2 + 1;
            int addrEnd = findEncodedEnd(body, addrStart, addrSpaces);
            String addr = body.substring(addrStart, Math.min(addrEnd, body.length()));
            i = addrEnd + 1;

            if (parsedName.startsWith("N:") && addr.contains(":")) {
                results.add(new String[]{parsedName, addr});
                storeAddress(parsedName, addr);
            }
        }
        return results;
    }

    private int findEncodedEnd(String s, int start, int spaces) {
        int found = 0;
        for (int i = start; i < s.length(); i++) {
            if (s.charAt(i) == ' ') {
                if (found == spaces) return i;
                found++;
            }
        }
        return s.length();
    }

    private void storeAddress(String name, String addr) throws Exception {
        if (name == null || addr == null) return;
        if (name.equals(nodeName)) return;
        if (!name.startsWith("N:") || !addr.contains(":")) return;
        String targetHash = computeHashID(name);
        int dist = computeDistance(nodeHashID, targetHash);

        int countAtDist = 0;
        for (String n : addressStore.keySet()) {
            if (n.equals(nodeName)) continue;
            String h = computeHashID(n);
            if (computeDistance(nodeHashID, h) == dist) countAtDist++;
        }

        if (countAtDist < 3) addressStore.put(name, addr);
    }

    public ArrayList<String[]> findClosestNodes(String targetHashID) throws Exception {
        ArrayList<String[]> best = getClosestNodes(targetHashID, 3);
        boolean improved = true;
        while (improved) {
            improved = false;
            for (String[] entry : new ArrayList<>(best)) {
                ArrayList<String[]> found = sendNearest(entry[1], entry[0], targetHashID);
                for (String[] candidate : found) {
                    boolean alreadyIn = false;
                    for (String[] b : best) if (b[0].equals(candidate[0])) { alreadyIn = true; break; }
                    if (!alreadyIn) {
                        best.add(candidate);
                        improved = true;
                    }
                }
            }
            Collections.sort(best, Comparator.comparingInt(a -> {
                try { return computeDistance(targetHashID, computeHashID(a[0])); }
                catch (Exception e) { return 256; }
            }));
            if (best.size() > 3) best = new ArrayList<>(best.subList(0, 3));
        }
        return best;
    }

    // STEP 6: Key/Value storage + Exists/Read/Write

    private boolean isOneOfClosest(String keyHashID) throws Exception {
        ArrayList<String[]> closest = getClosestNodes(keyHashID, 3);
        for (String[] entry : closest) {
            if (entry[0].equals(nodeName)) return true;
        }
        return false;
    }

    private void handleExists(byte[] txID, String rest, String senderAddress) throws Exception {
        String key = decodeString(rest);
        String keyHashID = computeHashID(key);
        boolean hasKey = dataStore.containsKey(key);
        boolean isClosest = isOneOfClosest(keyHashID);

        String response;
        if (hasKey) response = "FY";
        else if (isClosest) response = "FN";
        else response = "F?";

        sendPacket(senderAddress, buildMessage(txID, response));
    }

    private void handleRead(byte[] txID, String rest, String senderAddress) throws Exception {
        String key = decodeString(rest);
        String keyHashID = computeHashID(key);
        boolean hasKey = dataStore.containsKey(key);
        boolean isClosest = isOneOfClosest(keyHashID);

        String response;
        if (hasKey) response = "SY" + encodeString(dataStore.get(key));
        else if (isClosest) response = "SN";
        else response = "S?";

        sendPacket(senderAddress, buildMessage(txID, response));
    }

    private void handleWrite(byte[] txID, String rest, String senderAddress) throws Exception {
        int sp1 = rest.indexOf(' ');
        if (sp1 == -1) return;
        int keySpaces;
        try { keySpaces = Integer.parseInt(rest.substring(0, sp1)); }
        catch (NumberFormatException e) { return; }
        int keyStart = sp1 + 1;
        int keyEnd = findEncodedEnd(rest, keyStart, keySpaces);
        if (keyEnd >= rest.length()) return;
        String key = rest.substring(keyStart, keyEnd);

        int sp2 = rest.indexOf(' ', keyEnd + 1);
        if (sp2 == -1) return;
        int valSpaces;
        try { valSpaces = Integer.parseInt(rest.substring(keyEnd + 1, sp2)); }
        catch (NumberFormatException e) { return; }
        int valContentStart = sp2 + 1;
        int valEnd = findEncodedEnd(rest, valContentStart, valSpaces);
        String value = rest.substring(valContentStart, Math.min(valEnd, rest.length()));

        String keyHashID = computeHashID(key);
        boolean hasKey = dataStore.containsKey(key);
        boolean isClosest = isOneOfClosest(keyHashID);

        String response;
        if (hasKey) {
            dataStore.put(key, value);
            response = "XR";
        } else if (isClosest) {
            dataStore.put(key, value);
            response = "XA";
        } else {
            response = "XX";
        }

        sendPacket(senderAddress, buildMessage(txID, response));
    }

    private boolean sendExists(String address, String name, String key) throws Exception {
        byte[] txID = generateTxID();
        byte[] response = sendRequest(address, name, buildMessage(txID, "E" + encodeString(key)));
        if (response == null || response.length < 5) return false;
        return (char) response[4] == 'Y';
    }

    private String sendRead(String address, String name, String key) throws Exception {
        byte[] txID = generateTxID();
        byte[] response = sendRequest(address, name, buildMessage(txID, "R" + encodeString(key)));
        if (response == null || response.length < 5) return null;
        if ((char) response[4] != 'Y') return null;
        return decodeString(new String(response, 5, response.length - 5, StandardCharsets.UTF_8));
    }

    private char sendWrite(String address, String name, String key, String value) throws Exception {
        byte[] txID = generateTxID();
        byte[] response = sendRequest(address, name, buildMessage(txID, "W" + encodeString(key) + encodeString(value)));
        if (response == null || response.length < 5) return 'X';
        return (char) response[4];
    }

    public boolean exists(String key) throws Exception {
        if (key == null) return false;
        String keyHashID = computeHashID(key);
        ArrayList<String[]> closest = findClosestNodes(keyHashID);
        for (String[] entry : closest) {
            if (entry[0].equals(nodeName)) {
                if (dataStore.containsKey(key)) return true;
            } else {
                if (sendExists(entry[1], entry[0], key)) return true;
            }
        }
        return false;
    }

    public String read(String key) throws Exception {
        if (key == null) return null;
        String keyHashID = computeHashID(key);
        ArrayList<String[]> closest = findClosestNodes(keyHashID);
        for (String[] entry : closest) {
            String value;
            if (entry[0].equals(nodeName)) {
                value = dataStore.get(key);
            } else {
                value = sendRead(entry[1], entry[0], key);
            }
            if (value != null) return value;
        }
        return null;
    }

    public boolean write(String key, String value) throws Exception {
        if (key == null || value == null) return false;
        String keyHashID = computeHashID(key);
        ArrayList<String[]> closest = findClosestNodes(keyHashID);
        boolean anySuccess = false;
        for (String[] entry : closest) {
            char result;
            if (entry[0].equals(nodeName)) {
                boolean hasKey = dataStore.containsKey(key);
                boolean isClosest = isOneOfClosest(keyHashID);
                if (hasKey || isClosest) {
                    dataStore.put(key, value);
                    result = hasKey ? 'R' : 'A';
                } else {
                    result = 'X';
                }
            } else {
                result = sendWrite(entry[1], entry[0], key, value);
            }
            if (result == 'R' || result == 'A') anySuccess = true;
        }
        return anySuccess;
    }

    // STEP 7: UDP reliability

    private double packetLossProbability = 0.0;

    public void setPacketLoss(double probability) {
        this.packetLossProbability = probability;
    }

    // STEP 8: Relay messages (V)

    private void handleRelay(byte[] txID, byte[] data, int dataOffset, int dataLen, String senderAddress) throws Exception {
        String header = new String(data, dataOffset, dataLen - dataOffset, StandardCharsets.UTF_8);

        int sp1 = header.indexOf(' ');
        if (sp1 == -1) return;
        int nameSpaces;
        try { nameSpaces = Integer.parseInt(header.substring(0, sp1)); }
        catch (NumberFormatException e) { return; }
        int nameStart = sp1 + 1;
        int nameEnd = findEncodedEnd(header, nameStart, nameSpaces);
        String targetName = header.substring(nameStart, nameEnd);

        int embeddedOffset = dataOffset + nameEnd + 1;
        int embeddedLen = dataLen - embeddedOffset;
        if (embeddedLen < 3) return;

        String targetAddress = addressStore.get(targetName);
        if (targetAddress == null) return;

        byte[] newTxID = generateTxID();
        byte[] forwarded = new byte[embeddedLen];
        forwarded[0] = newTxID[0];
        forwarded[1] = newTxID[1];
        System.arraycopy(data, embeddedOffset + 2, forwarded, 2, embeddedLen - 2);

        String newTxKey = txIDToKey(newTxID);
        relayMap.put(newTxKey, new Object[]{txID.clone(), senderAddress});

        sendPacket(targetAddress, forwarded);
    }

    // STEP 9: Compare-and-Swap (C/D)

    // Handle incoming C request — atomic compare-and-swap
    private void handleCAS(byte[] txID, String rest, String senderAddress) throws Exception {
        // Parse key
        int sp1 = rest.indexOf(' ');
        if (sp1 == -1) return;
        int keySpaces;
        try { keySpaces = Integer.parseInt(rest.substring(0, sp1)); }
        catch (NumberFormatException e) { return; }
        int keyStart = sp1 + 1;
        int keyEnd = findEncodedEnd(rest, keyStart, keySpaces);
        if (keyEnd >= rest.length()) return;
        String key = rest.substring(keyStart, keyEnd);

        // Parse expected (current) value
        int sp2 = rest.indexOf(' ', keyEnd + 1);
        if (sp2 == -1) return;
        int expectedSpaces;
        try { expectedSpaces = Integer.parseInt(rest.substring(keyEnd + 1, sp2)); }
        catch (NumberFormatException e) { return; }
        int expectedStart = sp2 + 1;
        int expectedEnd = findEncodedEnd(rest, expectedStart, expectedSpaces);
        if (expectedEnd >= rest.length()) return;
        String expectedValue = rest.substring(expectedStart, expectedEnd);

        // Parse new value
        int sp3 = rest.indexOf(' ', expectedEnd + 1);
        if (sp3 == -1) return;
        int newSpaces;
        try { newSpaces = Integer.parseInt(rest.substring(expectedEnd + 1, sp3)); }
        catch (NumberFormatException e) { return; }
        int newStart = sp3 + 1;
        int newEnd = findEncodedEnd(rest, newStart, newSpaces);
        String newValue = rest.substring(newStart, Math.min(newEnd, rest.length()));

        String keyHashID = computeHashID(key);
        boolean hasKey = dataStore.containsKey(key);
        boolean isClosest = isOneOfClosest(keyHashID);

        String response;
        // Atomic block
        synchronized (dataStore) {
            if (hasKey) {
                if (dataStore.get(key).equals(expectedValue)) {
                    dataStore.put(key, newValue);
                    response = "DR";
                } else {
                    response = "DN";
                }
            } else if (isClosest) {
                dataStore.put(key, newValue);
                response = "DA";
            } else {
                response = "DX";
            }
        }

        sendPacket(senderAddress, buildMessage(txID, response));
    }

    // Send a C request and return the response code char
    private char sendCAS(String address, String name, String key, String currentValue, String newValue) throws Exception {
        byte[] txID = generateTxID();
        byte[] response = sendRequest(address, name, buildMessage(txID,
                "C" + encodeString(key) + encodeString(currentValue) + encodeString(newValue)));
        if (response == null || response.length < 5) return 'X';
        return (char) response[4];
    }

    // NodeInterface: compare-and-swap across the network
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        if (key == null || currentValue == null || newValue == null) return false;
        String keyHashID = computeHashID(key);
        ArrayList<String[]> closest = findClosestNodes(keyHashID);
        boolean anySuccess = false;
        for (String[] entry : closest) {
            char result;
            if (entry[0].equals(nodeName)) {
                synchronized (dataStore) {
                    boolean hasKey = dataStore.containsKey(key);
                    boolean isClosest = isOneOfClosest(keyHashID);
                    if (hasKey) {
                        if (dataStore.get(key).equals(currentValue)) {
                            dataStore.put(key, newValue);
                            result = 'R';
                        } else {
                            result = 'N';
                        }
                    } else if (isClosest) {
                        dataStore.put(key, newValue);
                        result = 'A';
                    } else {
                        result = 'X';
                    }
                }
            } else {
                result = sendCAS(entry[1], entry[0], key, currentValue, newValue);
            }
            if (result == 'R' || result == 'A') anySuccess = true;
        }
        return anySuccess;
    }

}