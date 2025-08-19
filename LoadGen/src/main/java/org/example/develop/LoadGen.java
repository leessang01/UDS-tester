package org.example.develop;

import java.io.*;
import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import static java.net.StandardProtocolFamily.*;

public class LoadGen {
    static class Conn {
        final SocketChannel ch;
        int sent = 0, receivedMsgs = 0;
        int bytesToRead = 0;
        final ByteBuffer sendBuf, recvBuf;
        Conn(SocketChannel ch, int msgSize) {
            this.ch = ch;
            this.sendBuf = ByteBuffer.allocateDirect(msgSize);
            this.recvBuf = ByteBuffer.allocateDirect(msgSize);
        }
    }

    public static void main(String[] args) throws Exception {
        // Args: --family=UNIX|INET --addr=/tmp/uds.sock | --host=127.0.0.1 --port=9001
        //       --conns=256 --msgs=1000 --msgSize=1024 --thinkNs=0
        Map<String,String> a = parse(args);
        String fam = a.getOrDefault("family","UNIX");
        int conns = Integer.parseInt(a.getOrDefault("conns","256"));
        int msgs  = Integer.parseInt(a.getOrDefault("msgs","1000"));
        int msgSize = Integer.parseInt(a.getOrDefault("msgSize","1024"));
        long thinkNs = Long.parseLong(a.getOrDefault("thinkNs","0"));

        Selector sel = Selector.open();
        List<Conn> all = new ArrayList<>(conns);
        for (int i=0;i<conns;i++) {
            SocketChannel ch;
            if ("UNIX".equalsIgnoreCase(fam)) {
                ch = SocketChannel.open(UnixDomainSocketAddress.of(a.getOrDefault("addr","/tmp/uds-echo.sock")));
//                ch = SocketChannel.open(UnixDomainSocketAddress.of(a.getOrDefault("addr","/home/sanghaklee2/IdeaProjects/Study/UDS/Socat/target/temp.sock")));
            } else {
                String host = a.getOrDefault("host","127.0.0.1");
                int port = Integer.parseInt(a.getOrDefault("port","9001"));
                ch = SocketChannel.open(new InetSocketAddress(host, port));
                try { ch.setOption(StandardSocketOptions.TCP_NODELAY, true); } catch (UnsupportedOperationException ignored) {}
            }
            ch.configureBlocking(false);
            Conn c = new Conn(ch, msgSize);
            c.bytesToRead = msgSize;
            ch.register(sel, SelectionKey.OP_READ, c);
            all.add(c);
        }

        // Latency storage (RTT in ns) – one per message per connection
        long[] lats = new long[(int)((long)conns * msgs)];
        int latIdx = 0;

        // Start a CPU sampler thread (process & system) while load runs
        CpuSampler sampler = new CpuSampler();
        Thread t = new Thread(sampler, "cpu-sampler"); t.start();

        long start = System.nanoTime();
        // Prime: send first message on each connection
        for (Conn c : all) send(c, thinkNs);

        while (!all.isEmpty()) {
            sel.select(1);
            Iterator<SelectionKey> it = sel.selectedKeys().iterator();
            while (it.hasNext()) {
                SelectionKey k = it.next(); it.remove();
                if (!k.isValid() || !k.isReadable()) continue;

                Conn c = (Conn) k.attachment();
                int n = c.ch.read(c.recvBuf);
                if (n < 0) { k.cancel(); all.remove(c); continue; }
                c.bytesToRead -= n;
                if (c.bytesToRead <= 0) {
                    c.recvBuf.flip();
                    long sentAt = c.recvBuf.getLong(0);
                    lats[latIdx++] = System.nanoTime() - sentAt;

                    c.recvBuf.clear();
                    c.bytesToRead = c.recvBuf.capacity();
                    c.receivedMsgs++;

                    if (c.sent < msgs) {
                        send(c, thinkNs);
                    } else {
                        // done for this conn
                        k.cancel();
                        c.ch.close();
                        all.remove(c);
                        break;
                    }
                }
            }
        }
        long end = System.nanoTime();
        sampler.stop();

        // Compute summary
        lats = Arrays.copyOf(lats, latIdx);
        Arrays.sort(lats);
        double durationSec = (end - start) / 1e9;
        long totalMsgs = latIdx;
        double tputOps = totalMsgs / durationSec;
        double tputBytes = (totalMsgs * (double) msgSize) / durationSec;

        System.out.printf("Connections=%d, Msgs/conn=%d, MsgSize=%d bytes%n",
                Integer.parseInt(a.getOrDefault("conns","256")), msgs, msgSize);
        System.out.printf("Throughput: %.0f ops/s, %.2f MB/s%n", tputOps, tputBytes / (1024*1024));
        System.out.printf("Latency (us): p50=%.2f p90=%.2f p99=%.2f p99.9=%.2f min=%.2f max=%.2f%n",
                p(lats, 0.50)/1e3, p(lats,0.90)/1e3, p(lats,0.99)/1e3, p(lats,0.999)/1e3,
                lats[0]/1e3, lats[lats.length-1]/1e3);
        System.out.printf("CPU(avg): process=%.1f%%, system=%.1f%%%n", sampler.avgProc()*100, sampler.avgSys()*100);
    }

    static void send(Conn c, long thinkNs) throws IOException {
        c.sendBuf.clear();
        c.sendBuf.putLong(System.nanoTime()); // timestamp at offset 0
        while (c.sendBuf.hasRemaining()) c.sendBuf.put((byte)0);
        c.sendBuf.flip();
        while (c.sendBuf.hasRemaining()) c.ch.write(c.sendBuf);
        c.sent++;
        if (thinkNs > 0) {
            long t = System.nanoTime() + thinkNs;
            while (System.nanoTime() < t) { /* spin */ }
        }
    }

    static double p(long[] a, double q) { int i = (int)Math.min(a.length-1, Math.max(0, Math.round(q*(a.length-1)))); return a[i]; }
    static Map<String,String> parse(String[] args){ Map<String,String> m=new HashMap<>(); for(String s:args){ if(s.startsWith("--")&&s.contains("=")){int i=s.indexOf('='); m.put(s.substring(2,i), s.substring(i+1));}} return m; }

    static class CpuSampler implements Runnable {
        final OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        volatile boolean running = true;
        final List<Double> proc = new ArrayList<>(), sys = new ArrayList<>();
        public void run() {
            try {
                while (running) {
                    proc.add(os.getProcessCpuLoad()); // 0.0..1.0
                    sys.add(os.getSystemCpuLoad());   // 0.0..1.0
                    Thread.sleep(200);
                }
            } catch (InterruptedException ignored) {}
        }
        void stop() { running = false; }
        double avgProc(){ return proc.stream().mapToDouble(Double::doubleValue).average().orElse(0.0); }
        double avgSys(){ return sys.stream().mapToDouble(Double::doubleValue).average().orElse(0.0); }
    }
}
