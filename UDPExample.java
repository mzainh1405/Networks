import java.lang.InterruptedException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

public class UDPExample {

    public static void main(String[] args) {

	// Must be run with a command line argument
	if ((args.length != 1) ||
	    !(args[0].equals("red") || args[0].equals("green"))) {
	    System.err.println("Run with either:\njava UDPExample red\nor\njava UDPExample green");
	    return;
	}

	// Only one program can listen on a port on each computer.
	// So that we can do this exercise on one computer
	// we have to use the command line arguments to distinguish
	// between the two copies of the program running.
	int listeningPort;
	int targetPort;
	String message = null;
	if (args[0].equals("red")) {
	    System.out.println("I am \033[31;1mRed!\033[0m");
	    listeningPort = 4022;
	    targetPort = 4023;
	    message = "Hello from red!";
	} else {
	    System.out.println("I am \033[32;1mGreen!\033[0m");
	    listeningPort = 4023;
	    targetPort = 4022;
	    message = "Hello from green!";
	}

	// We need to open a DatagramSocket to receive packets
	System.out.println("Listening on port " + listeningPort);
	DatagramSocket ds = null;
	try {
	    ds = new DatagramSocket(listeningPort);
	} catch (SocketException e) {
	    System.out.println("Exception opening socket");
	    e.printStackTrace(System.err);
	    return;
	}


	// To make it easier to run both, let's delay just a bit...
	System.out.print("Waiting ... ");
	try {
	    int secondsToWaitInStartup = 5;
	    Thread.sleep(secondsToWaitInStartup * 1000);
	} catch (InterruptedException e) {
	    // Can ignore, will just shorten the delay
	}
	System.out.println("done!");

	// Let's build a packet!
	// Because UDP is not connection oriented, each packet needs
	// the destination address.
        String IPAddressString = "127.0.0.1";
        InetAddress targetIP = null;
	try {
	    targetIP = InetAddress.getByName(IPAddressString);
	} catch (UnknownHostException e) {
	    System.out.println("Exception opening socket");
	    e.printStackTrace(System.err);
	    return;
	}

	// The contents is an array of bytes.
	// One way to produce these is to convert a string to bytes
	byte[] contents = message.getBytes(StandardCharsets.UTF_8);
	DatagramPacket packet = new DatagramPacket(contents, contents.length, targetIP, targetPort);

	// Now we can send it
	System.out.print("Sending message to port " + targetPort + " ... ");
	try {
	    ds.send(packet);
	} catch (Exception e) {
	    System.out.println("error!");
	    e.printStackTrace(System.err);
	    return;
	}
	System.out.println("done!");

	// We want to receive anything the other side has sent.
	// Create a space for the received packet
	int receivedContentsSize = 65535;  // We will find out why in lecture 5!
	byte [] receivedContents = new byte[receivedContentsSize];
	DatagramPacket received = new DatagramPacket(receivedContents, receivedContentsSize);

	// As UDP is an unreliable protocol we can't guarantee
	// that anything will arrive.  Rather than waiting forever,
	// we set a time out.
	int secondsToWaitForPacket = 5;   // 0 will wait until something arrives
	try {
	    ds.setSoTimeout(secondsToWaitForPacket * 1000);
	} catch (SocketException e) {
	    System.out.println("Exception setting time out");
	    e.printStackTrace(System.err);
	    return;
	}

	// And now we wait...
	System.out.print("Waiting for a packet ... ");
	try {
	    ds.receive(received);
	} catch (SocketTimeoutException ste) {
	    System.out.println("time out! :-(");
	    return;
	} catch (Exception e) {
	    System.out.println("error! }-(");
	    e.printStackTrace(System.err);
	    return;
	}
	System.out.println("got one! :-)");


	// Let's see what we have been sent
	// The packet contains who sent it:
	System.out.println("Got a message from " + received.getAddress() + " port " + received.getPort());

	// The data is an array of bytes.
	// This can represent any data.
	// We will try to interpret it as a string.
	String receivedMessage = new String(received.getData(), StandardCharsets.UTF_8);
	System.out.println("They say : " + receivedMessage);

	// Finally, close the socket
	ds.close();

	return;
    }

}
