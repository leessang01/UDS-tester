package org.example.develop;

import java.io.IOException;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import static java.net.StandardProtocolFamily.*;

public class EchoServer {
    record ConnState(Queue<ByteBuffer> outbound) {}
    public static void main(String[] args) throws Exception {
        // Args: --family=UNIX|INET --addr=/tmp/uds.sock | --host=127.0.0.1 --port=9001
        Map<String,String> a = parse(args);
        String fam = a.getOrDefault("family","UNIX");
        Selector sel = Selector.open();

        ServerSocketChannel server;
        if ("UNIX".equalsIgnoreCase(fam)) {
            server = ServerSocketChannel.open(UNIX);
            String path = a.getOrDefault("addr","/tmp/uds-echo.sock");
            Path p = Path.of(path);
            try { Files.deleteIfExists(p); } catch (Exception ignored) {}
            server.bind(java.net.UnixDomainSocketAddress.of(p));
            // optional: delete on exit so socket file is cleaned up
            p.toFile().deleteOnExit();
        } else {
            server = ServerSocketChannel.open(INET);
            String host = a.getOrDefault("host","127.0.0.1");
            int port = Integer.parseInt(a.getOrDefault("port","9001"));
            server.bind(new InetSocketAddress(host, port));
        }
        server.configureBlocking(false);
        server.register(sel, SelectionKey.OP_ACCEPT);

        ByteBuffer readBuf = ByteBuffer.allocateDirect(256 * 1024); // big enough for your msg sizes
        System.out.println("Listening on " + server.getLocalAddress());

        while (true) {
            sel.select();
            Iterator<SelectionKey> it = sel.selectedKeys().iterator();
            while (it.hasNext()) {
                SelectionKey key = it.next(); it.remove();
                if (!key.isValid()) continue;

                if (key.isAcceptable()) {
                    SocketChannel ch = server.accept();
                    if (ch == null) continue;
                    ch.configureBlocking(false);
                    try {
                        // For TCP only; harmless/no-op on UNIX if unsupported
                        ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
                    } catch (UnsupportedOperationException ignored) {}
                    key = ch.register(sel, SelectionKey.OP_READ, new ConnState(new ArrayDeque<>()));
                } else if (key.isReadable()) {
                    SocketChannel ch = (SocketChannel) key.channel();
                    readBuf.clear();
                    int n = ch.read(readBuf);
                    if (n < 0) { key.cancel(); ch.close(); continue; }
                    readBuf.flip();
                    // Echo back what we read. Handle partial writes by queueing.
                    ConnState st = (ConnState) key.attachment();
                    ByteBuffer out = ByteBuffer.allocate(readBuf.remaining());
                    out.put(readBuf).flip();
                    st.outbound.add(out);
                    key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                } else if (key.isWritable()) {
                    SocketChannel ch = (SocketChannel) key.channel();
                    ConnState st = (ConnState) key.attachment();
                    while (!st.outbound.isEmpty()) {
                        ByteBuffer buf = st.outbound.peek();
                        ch.write(buf);
                        if (buf.hasRemaining()) break;
                        st.outbound.poll();
                    }
                    if (st.outbound.isEmpty()) key.interestOps(SelectionKey.OP_READ);
                }
            }
        }
    }

    private static Map<String,String> parse(String[] args) {
        Map<String,String> m = new HashMap<>();
        for (String s : args) {
            if (s.startsWith("--") && s.contains("=")) {
                int i = s.indexOf('=');
                m.put(s.substring(2, i), s.substring(i+1));
            }
        }
        return m;
    }
}
