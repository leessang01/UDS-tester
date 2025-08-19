package org.example.develop;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import jdk.net.ExtendedSocketOptions;
import jdk.net.UnixDomainPrincipal;

import static java.net.StandardProtocolFamily.UNIX;
import static java.net.StandardProtocolFamily.INET;
import static java.net.StandardProtocolFamily.INET6;

public class Socat {
    static void usage() {
        String ustring = """
                
usage: java Socat -s <baddr>...
                
    java Socat -c [-bind <baddr>] <daddr> N [delay]

    java Socat -h
                
-s means create one or more listening servers bound to addresses <baddr>...,
then accept all incoming connections and display (counts of) received data. If
more than one <baddr> is supplied, then multiple channels are created, each
bound to one of the supplied addresses. All channels are non-blocking and
managed by one Selector.
                
-c means create a client, connect it to <daddr> and send N (16 Kb) buffers. The
client may optionally bind to a given address <baddr>. If a delay is specified,
then the program pauses for the specified number of milliseconds between each
send. After sending, the client reads until EOF and then exits.
                
Note: AF_UNIX client sockets do not bind to an address by default. Therefore,
the remote address seen on the server side (and the client's local address) is
an empty path. This is slightly different from AF_INET/6 sockets, which, if the
user does not choose a local port, then a randomly chosen one is assigned.

-h means print this message and exit.

<baddr> and <daddr> are addresses specified as follows:
                
    UNIX:{path}
                
    INET:{host}:port
                
    INET6:{host}:port
                
{path} is the name of a socket file surrounded by curly brackets,
{}, which can be empty when binding a server signifying a randomly chosen local
address.
                
{host}:port is an internet address comprising a domain name or IPv4/v6 literal
surrounded by curly brackets, {}, which can be empty when binding (signifying
any local address) and a port number, which can be zero when binding.
""";
        System.out.println(ustring);
    }

    static boolean isClient;
    static boolean initialized = false;
    static final int BUFSIZE = 8 * 1024;
    static int N;           // Number of buffers to send
    static int DELAY = 0;   // Milliseconds to delay between sends

    static List<AddressAndFamily> locals = new LinkedList<>();
    static AddressAndFamily remote;

    // family is only needed in cases where address is null.
    // It could be a Record type.

    static class AddressAndFamily {
        SocketAddress address;
        ProtocolFamily family;
        AddressAndFamily(ProtocolFamily family, SocketAddress address) {
            this.address = address;
            this.family = family;
        }
    }

    static AddressAndFamily parseAddress(String addr) throws UnknownHostException {
        char c = addr.charAt(0);
        if (c != 'U' && c != 'I')
            throw new IllegalArgumentException("invalid address");

        String family = addr.substring(0, addr.indexOf(':')).toUpperCase();

        return switch (family) {
            case "UNIX" -> parseUnixAddress(addr);
            case "INET" -> parseInetSocketAddress(INET, addr);
            case "INET6" -> parseInetSocketAddress(INET6, addr);
            default -> throw new IllegalArgumentException();
        };
    }

    static AddressAndFamily parseUnixAddress(String token) {
        String path = getPathDomain(token);
        UnixDomainSocketAddress address;
        if (path.isEmpty())
            address = null;
        else
            address = UnixDomainSocketAddress.of(path);
        return new AddressAndFamily(UNIX, address);
    }

    static AddressAndFamily parseInetSocketAddress(StandardProtocolFamily family, String token) throws UnknownHostException {
        String domain = getPathDomain(token);
        InetAddress address;
        if (domain.isEmpty()) {
            address = (family == StandardProtocolFamily.INET)
                    ? InetAddress.getByName("0.0.0.0")
                    : InetAddress.getByName("::0");
        } else {
            address = InetAddress.getByName(domain);
        }
        int cp = token.lastIndexOf(':') + 1;
        int port = Integer.parseInt(token.substring(cp));
        var isa = new  InetSocketAddress(address, port);
        return new AddressAndFamily(family, isa);
    }

    // Return the token between braces, that is, a domain name or UNIX path.

    static String getPathDomain(String s) {
        int start = s.indexOf('{') + 1;
        int end = s.indexOf('}');
        if (start == -1 || end == -1 || (start > end))
            throw new IllegalArgumentException(s);
        return s.substring(start, end);
    }

    // Return false if the program must exit.

    static void parseArgs(String[] args) throws UnknownHostException {
        if (args[0].equals("-h")) {
            usage();
        } else if (args[0].equals("-c")) {
            isClient = true;
            int nextArg;
            AddressAndFamily local = null;
            if (args[1].equals("-bind")) {
                local = parseAddress(args[2]);
                locals.add(local);
                nextArg = 3;
            } else {
                nextArg = 1;
            }
            remote = parseAddress(args[nextArg++]);
            N = Integer.parseInt(args[nextArg++]);
            if (nextArg == args.length - 1) {
                DELAY = Integer.parseInt(args[nextArg]);
            }
            initialized = true;
        } else if (args[0].equals("-s")) {
            isClient = false;
            for (int i = 1; i < args.length; i++) {
                locals.add(parseAddress(args[i]));
            }
            initialized = true;
        } else
            throw new IllegalArgumentException();
    }

    public static void main(String[] args) throws Exception {
        try {
            parseArgs(args);
        } catch (Exception e) {
            System.out.printf("\nInvalid arguments supplied. See the following for usage information\n");
            usage();
        }
        if (!initialized)
            return;
        if (isClient) {
            doClient();
        } else {
            doServer();
        }
    }

    static Map<SocketChannel,Integer> byteCounter = new HashMap<>();

    private static void initListener(AddressAndFamily aaf, Selector selector) {
        try {
            ProtocolFamily family = aaf.family;
            SocketAddress address = aaf.address;
            ServerSocketChannel server = ServerSocketChannel.open(family);
            server.bind(address);
            server.configureBlocking(false);
            postBind(address);
            server.register(selector, SelectionKey.OP_ACCEPT, null);
            System.out.println("Server: Listening on " + server.getLocalAddress());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void doServer() throws IOException {
        ByteBuffer readBuf = ByteBuffer.allocate(64 * 1024);
        final Selector selector = Selector.open();
        locals.forEach(localAddress -> initListener(localAddress, selector));
        int nextConnectionId = 1;
        while (true) {
            selector.select();
            var keys = selector.selectedKeys();
            for (SelectionKey key : keys) {
                try {
                    SelectableChannel c = key.channel();
                    if (c instanceof ServerSocketChannel) {
                        var server = (ServerSocketChannel)c;
                        var ch = server.accept();
                        var userid = "";
                        if (server.getLocalAddress() instanceof UnixDomainSocketAddress) {

                            // An illustration of additional capability of UNIX
                            // channels; it's not required behavior.

                            UnixDomainPrincipal pr = ch.getOption(ExtendedSocketOptions.SO_PEERCRED);
                            userid = "user: " + pr.user().toString() + " group: " +
                                    pr.group().toString();
                        }
                        ch.configureBlocking(false);
                        byteCounter.put(ch, 0);
                        System.out.printf("Server: new connection\n\tfrom {%s}\n", ch.getRemoteAddress());
                        System.out.printf("\tConnection id: %s\n", nextConnectionId);
                        if (userid.length() > 0) {
                            System.out.printf("\tpeer credentials: %s\n", userid);
                        }
                        System.out.printf("\tConnection count: %d\n",  byteCounter.size());
                        ch.register(selector, SelectionKey.OP_READ, nextConnectionId++);
                    } else {
                        var ch = (SocketChannel) c;
                        int id = (Integer)key.attachment();
                        int bytes = byteCounter.get(ch);
                        readBuf.clear();
                        int n = ch.read(readBuf);
                        if (n < 0) {
                            String remote = ch.getRemoteAddress().toString();
                            System.out.printf("Server: closing connection\n\tfrom: {%s} Id: %d\n", remote, id);
                            System.out.printf("\tBytes received: %d\n", bytes);
                            byteCounter.remove(ch);
                            ch.close();
                        } else {
                            readBuf.flip();
                            bytes += n;
                            byteCounter.put(ch, bytes);
                            display(ch, readBuf, id);
                        }
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            };
            keys.clear();
        }
    }

    private static void postBind(SocketAddress address) {
        if (address instanceof UnixDomainSocketAddress) {
            var usa = (UnixDomainSocketAddress)address;
            usa.getPath().toFile().deleteOnExit();
        }
    }

    private static void display(SocketChannel ch, ByteBuffer readBuf, int id)
            throws IOException
    {
        System.out.printf("Server: received %d bytes from: {%s} Id: %d\n",
                readBuf.remaining(), ch.getRemoteAddress(), id);
    }

    private static void doClient() throws Exception {
        SocketChannel client;
        if (locals.isEmpty())
            client = SocketChannel.open(remote.address);
        else {
            AddressAndFamily aaf = locals.get(0);
            client = SocketChannel.open(aaf.family);
            client.bind(aaf.address);
            postBind(aaf.address);
            client.connect(remote.address);
        }
        ByteBuffer sendBuf = ByteBuffer.allocate(BUFSIZE);
        for (int i=0; i<N; i++) {
            fill(sendBuf);
            client.write(sendBuf);
            Thread.sleep(DELAY);
        }
        client.shutdownOutput();
        ByteBuffer rxb = ByteBuffer.allocate(64 * 1024);
        int c;
        while ((c = client.read(rxb)) > 0) {
            rxb.flip();
            System.out.printf("Client: received %d bytes\n", rxb.remaining());
            rxb.clear();
        }
        client.close();
    }

    private static void fill(ByteBuffer sendBuf) {

        // Because this example is for demonstration purposes, this method
        // doesn't fill the ByteBuffer sendBuf with data. Instead, it sets the
        // limits of sendBuf to its capacity and its position to zero.
        // Consequently, when the example writes the contents of sendBuf, it
        // writes the entire contents of whatever happened to be in memory when
        // sendBuf was allocated.

        sendBuf.limit(sendBuf.capacity());
        sendBuf.position(0);
    }
}