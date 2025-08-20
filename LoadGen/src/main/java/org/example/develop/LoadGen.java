package org.example.develop;

import java.io.*;
import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean; // HotSpot extension used widely
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.*;
import static java.net.StandardProtocolFamily.*;

/**
 * Selector-driven load generator for UDS vs TCP(loopback).
 * - Phases: WARMUP -> MEASURE -> COOLDOWN (time-based).
 * - Records a SINGLE CSV with: one 'meta' row, periodic 'ts' rows (time-series),
 *   and a final 'summary' row. No repeated input args on every data row.
 * - In 'ts' rows we write aggregates every --sampleMs (default 1000 ms):
 *     * message deltas & totals, error deltas & totals
 *     * instantaneous throughput (ops/s, MB/s)
 *     * latency stats for the interval (min/max/p50/p90/p99/p999) from completed RTTs
 *     * CPU snapshot (process/system %) on the CLIENT (this process' host)
 *     * active connection count
 * - Progress is printed once per sample (phase, %, delta ops/s, conns, CPU).
 */
public class LoadGen {

    // -------------------------- Phasing --------------------------
    enum Phase { WARMUP, MEASURE, COOLDOWN, DONE }

    // -------------------- Per-connection state -------------------
    static class Conn {
        final SocketChannel ch;
        final ByteBuffer sendBuf, recvBuf;
        int bytesToRead;
        Conn(SocketChannel ch, int msgSize) {
            this.ch = ch;
            this.sendBuf = ByteBuffer.allocateDirect(msgSize);
            this.recvBuf = ByteBuffer.allocateDirect(msgSize);
            this.bytesToRead = msgSize;
        }
    }

    // -------------- Thread-safe shared counters/state ------------
    static final class Shared {
        final AtomicInteger activeConns = new AtomicInteger(0);
        final LongAdder totalMsgs  = new LongAdder(); // completed RTTs (MEASURE only)
        final LongAdder totalErrs  = new LongAdder(); // failed/incomplete messages
        final ConcurrentLinkedQueue<Long> latQueue = new ConcurrentLinkedQueue<>(); // RTT ns for interval draining
        volatile Phase phase = Phase.WARMUP;
        volatile long warmStartNs, warmEndNs, measStartNs, measEndNs, coolStartNs, coolEndNs;
    }

    public static void main(String[] args) throws Exception {
        // ---------------- CLI / Config ----------------
        // --family=UNIX|INET --addr=/tmp/uds.sock | --host=127.0.0.1 --port=9001
        // --conns=256 --msgSize=1024 --thinkNs=0
        // --warmupSec=30 --measureSec=60 --cooldownSec=5
        // --sampleMs=1000   (time series granularity)
        // --csvDir=./results
        Map<String,String> a = parse(args);
        String fam = a.getOrDefault("family","UNIX");
        int conns = Integer.parseInt(a.getOrDefault("conns","256"));
        int msgSize = Integer.parseInt(a.getOrDefault("msgSize","1024"));
        long thinkNs = Long.parseLong(a.getOrDefault("thinkNs","0"));
        long warmupNs   = Long.parseLong(a.getOrDefault("warmupSec","30"))   * 1_000_000_000L;
        long measureNs  = Long.parseLong(a.getOrDefault("measureSec","60"))  * 1_000_000_000L;
        long cooldownNs = Long.parseLong(a.getOrDefault("cooldownSec","5"))  * 1_000_000_000L;
        long sampleMs   = Long.parseLong(a.getOrDefault("sampleMs","1000"));
        Path csvDir = Paths.get(a.getOrDefault("csvDir","."));
        Files.createDirectories(csvDir);

        // Output file (single CSV)
        String stamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        String famLc = ("UNIX".equalsIgnoreCase(fam) ? "uds" : "tcp");
        String base  = String.format("%s_conns%d_msg%d_%s", famLc, conns, msgSize, stamp);
        Path csvFile = csvDir.resolve(base + "-results.csv");

        // ---------------- Open channels / register ----------------
        Selector sel = Selector.open();
        List<Conn> all = new ArrayList<>(conns);
        for (int i=0;i<conns;i++) {
            SocketChannel ch;
            if ("UNIX".equalsIgnoreCase(fam)) {
                ch = SocketChannel.open(UnixDomainSocketAddress.of(a.getOrDefault("addr","/tmp/uds-echo.sock")));
            } else {
                String host = a.getOrDefault("host","127.0.0.1");
                int port = Integer.parseInt(a.getOrDefault("port","9001"));
                ch = SocketChannel.open(new InetSocketAddress(host, port));
                try { ch.setOption(StandardSocketOptions.TCP_NODELAY, true); } catch (UnsupportedOperationException ignored) {}
            }
            ch.configureBlocking(false);
            Conn c = new Conn(ch, msgSize);
            ch.register(sel, SelectionKey.OP_READ, c);
            all.add(c);
        }

        // Prime one message per connection before entering the loop
        for (Conn c : all) {
            try { send(c, thinkNs); }
            catch (IOException ioe) { safeClose(c.ch); }
        }

        // ---------------- Shared state / timing -------------------
        Shared S = new Shared();
        S.activeConns.set(conns);

        long t0 = System.nanoTime();
        S.warmStartNs = t0;
        S.warmEndNs   = t0 + warmupNs;
        S.measStartNs = S.warmEndNs;
        S.measEndNs   = S.measStartNs + measureNs;
        S.coolStartNs = S.measEndNs;
        S.coolEndNs   = S.coolStartNs + cooldownNs;

        // ---------------- CSV writer (single file) ----------------
        try (BufferedWriter csv = Files.newBufferedWriter(csvFile)) {
            // 1) Header (single superset—data rows won’t repeat args)
            csv.write(String.join(",",
                    "record_type","timestamp","elapsed_ms","phase",
                    // meta-only (present once, top row)
                    "family","conns","msgSize","thinkNs","warmupSec","measureSec","cooldownSec","sampleMs",
                    // time-series columns (every sample)
                    "msgs_total","msgs_delta","errs_total","errs_delta",
                    "tput_ops_s","tput_MB_s",
                    "lat_count","lat_ns_min","lat_ns_p50","lat_ns_p90","lat_ns_p99","lat_ns_p999","lat_ns_max",
                    "cpu_proc_pct","cpu_sys_pct","active_conns",
                    // summary-only extras
                    "sum_p50_us","sum_p90_us","sum_p99_us","sum_p999_us","sum_min_us","sum_max_us",
                    "sum_tput_ops_s","sum_tput_MB_s","sum_cpu_proc_avg","sum_cpu_sys_avg"
            ));
            csv.newLine();

            // 2) Meta row (no repetition later)
            csv.write(String.join(",",
                    "meta", stamp, "0", "WARMUP",
                    famLc, String.valueOf(conns), String.valueOf(msgSize), String.valueOf(thinkNs),
                    String.valueOf(warmupNs/1_000_000_000L), String.valueOf(measureNs/1_000_000_000L),
                    String.valueOf(cooldownNs/1_000_000_000L), String.valueOf(sampleMs),
                    "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""
            ));
            csv.newLine();
            csv.flush();

            // 3) Sampler (writes a 'ts' row every sampleMs and prints progress)
            Sampler sampler = new Sampler(S, csv, sampleMs, msgSize);
            Thread samplerThread = new Thread(sampler, "sampler");
            samplerThread.start();

            // ---- Banner / progress context ----
            System.out.printf("[INFO] family=%s conns=%d msgSize=%dB thinkNs=%d | warmup=%ds measure=%ds cooldown=%ds | sample=%dms%n",
                    famLc, conns, msgSize, thinkNs, warmupNs/1_000_000_000L, measureNs/1_000_000_000L, cooldownNs/1_000_000_000L, sampleMs);
            System.out.printf("[WARMUP] %ds…%n", warmupNs/1_000_000_000L);

            // ---------------- Event loop ----------------
            while (S.phase != Phase.DONE && !all.isEmpty()) {
                sel.select(1);
                long now = System.nanoTime();

                // Phase transitions (time-driven)
                if (S.phase == Phase.WARMUP && now >= S.warmEndNs) {
                    S.phase = Phase.MEASURE;
                    sampler.beginMeasure(now); // zero CPU rolling avgs and time origin
                    System.out.printf("[MEASURE] %ds…%n", measureNs/1_000_000_000L);
                } else if (S.phase == Phase.MEASURE && now >= S.measEndNs) {
                    S.phase = Phase.COOLDOWN;
                    sampler.endMeasure(); // stop including in CPU avgs
                    System.out.printf("[COOLDOWN] %ds (drain echoes, no new sends)…%n", cooldownNs/1_000_000_000L);
                } else if (S.phase == Phase.COOLDOWN && now >= S.coolEndNs) {
                    S.phase = Phase.DONE;
                }

                Iterator<SelectionKey> it = sel.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey k = it.next(); it.remove();
                    if (!k.isValid() || !k.isReadable()) continue;

                    Conn c = (Conn) k.attachment();
                    int n;
                    try {
                        n = c.ch.read(c.recvBuf);
                    } catch (IOException ioe) {
                        // Read error -> count as error and close
                        S.totalErrs.increment();
                        k.cancel(); safeClose(c.ch); all.remove(c); S.activeConns.decrementAndGet();
                        continue;
                    }

                    if (n < 0) {
                        // Unexpected EOF (incomplete message is an error)
                        if (c.bytesToRead > 0 && S.phase != Phase.COOLDOWN) {
                            S.totalErrs.increment();
                        }
                        k.cancel(); safeClose(c.ch); all.remove(c); S.activeConns.decrementAndGet();
                        continue;
                    }

                    c.bytesToRead -= n;
                    if (c.bytesToRead <= 0) {
                        // Full echo received -> measure RTT during MEASURE only
                        c.recvBuf.flip();
                        long sentAt = c.recvBuf.getLong(0);
                        long rttNs = System.nanoTime() - sentAt;
                        if (S.phase == Phase.MEASURE) {
                            S.latQueue.add(rttNs);
                            S.totalMsgs.increment();
                        }
                        // Reset for next message
                        c.recvBuf.clear();
                        c.bytesToRead = c.recvBuf.capacity();

                        if (S.phase != Phase.COOLDOWN) {
                            try { send(c, thinkNs); }
                            catch (IOException ioe) {
                                S.totalErrs.increment();
                                k.cancel(); safeClose(c.ch); all.remove(c); S.activeConns.decrementAndGet();
                            }
                        } else {
                            // During cooldown: stop sending; close after one echo drains
                            k.cancel(); safeClose(c.ch); all.remove(c); S.activeConns.decrementAndGet();
                            break;
                        }
                    }
                }
            }

            // Let sampler finish the last tick then stop
            sampler.stop();
            try { Thread.sleep(Math.min(200, sampleMs)); } catch (InterruptedException ignored) {}
            samplerThread.join();

            // 4) Write final summary row (MEASURE window)
            Summary sum = sampler.summary();
            csv.write(String.join(",",
                    "summary", stamp, String.valueOf(msSince(0, sum.measStartNs, sum.measEndNs)), "DONE",
                    "", "", "", "", "", "", "", "",
                    String.valueOf(sum.totalMsgs), "", String.valueOf(sum.totalErrs), "",
                    String.format(Locale.ROOT, "%.6f", sum.totalMsgs / sum.measDurationSec),
                    String.format(Locale.ROOT, "%.6f", (sum.totalMsgs * (double) msgSize) / (1024*1024) / sum.measDurationSec),
                    String.valueOf(sum.latCount), String.valueOf(sum.latMinNs),
                    String.valueOf(sum.latP50Ns), String.valueOf(sum.latP90Ns), String.valueOf(sum.latP99Ns),
                    String.valueOf(sum.latP999Ns), String.valueOf(sum.latMaxNs),
                    String.format(Locale.ROOT, "%.6f", sum.cpuProcAvg), String.format(Locale.ROOT, "%.6f", sum.cpuSysAvg),
                    // human-friendly latency summary (µs)
                    fmtUs(sum.latP50Ns), fmtUs(sum.latP90Ns), fmtUs(sum.latP99Ns), fmtUs(sum.latP999Ns),
                    fmtUs(sum.latMinNs), fmtUs(sum.latMaxNs),
                    String.format(Locale.ROOT, "%.6f", sum.totalMsgs / sum.measDurationSec),
                    String.format(Locale.ROOT, "%.6f", (sum.totalMsgs * (double) msgSize) / (1024*1024) / sum.measDurationSec),
                    String.format(Locale.ROOT, "%.6f", sum.cpuProcAvg), String.format(Locale.ROOT, "%.6f", sum.cpuSysAvg)
            ));
            csv.newLine();
            csv.flush();
        }

        System.out.println("[CSV] " + csvFile.toAbsolutePath());
    }

    // ---------------------- Sampler (time-series writer + progress) ----------------------
    static class Sampler implements Runnable {
        private final Shared S;
        private final BufferedWriter csv;
        private final OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        private final long sampleMs;
        private final int msgSize;
        private volatile boolean running = true;

        // State for MEASURE-only CPU averages and all-latency aggregation
        private long measStartNs = 0L, measEndNs = 0L;
        private final List<Double> cpuProc = new ArrayList<>(), cpuSys = new ArrayList<>();
        private final LongArray allLats = new LongArray(); // store ALL measured latencies for final quantiles

        // For deltas
        private long lastMsgs = 0, lastErrs = 0;
        private long lastTickNs = System.nanoTime();

        Sampler(Shared s, BufferedWriter csv, long sampleMs, int msgSize) {
            this.S = s; this.csv = csv; this.sampleMs = sampleMs; this.msgSize = msgSize;
        }

        void beginMeasure(long nowNs) { measStartNs = nowNs; cpuProc.clear(); cpuSys.clear(); }
        void endMeasure() { measEndNs = System.nanoTime(); }
        void stop() { running = false; }

        public void run() {
            while (running) {
                try { Thread.sleep(Math.max(100, sampleMs)); } catch (InterruptedException ignored) {}
                long now = System.nanoTime();
                Phase ph = S.phase;

                // Drain latencies that arrived since last tick
                ArrayList<Long> window = new ArrayList<>();
                for (Long v; (v = S.latQueue.poll()) != null; ) {
                    window.add(v);
                    allLats.add(v);
                }

                // Totals & deltas
                long msgs = S.totalMsgs.sum();
                long errs = S.totalErrs.sum();
                long dMsgs = msgs - lastMsgs;
                long dErrs = errs - lastErrs;
                double sec = (now - lastTickNs) / 1e9;
                double ops = sec > 0 ? dMsgs / sec : 0.0;
                double mbs = sec > 0 ? (dMsgs * (double) msgSize) / (1024*1024) / sec : 0.0;
                lastMsgs = msgs; lastErrs = errs; lastTickNs = now;

                // Lat stats for this window (ns)
                LatStats ls = LatStats.of(window);

                // CPU snapshot (client-side!)
                double p = os.getProcessCpuLoad() * 100.0;
                double s = os.getSystemCpuLoad()  * 100.0;
                if (ph == Phase.MEASURE && measStartNs != 0L) {
                    cpuProc.add(p); cpuSys.add(s);
                }

                // Print concise progress line
                double pct = percent(now, ph, S);
                System.out.printf("[%s] %5.1f%%  msgs+=%d  ops/s≈%.0f  conns=%d  CPU≈proc %.1f%% sys %.1f%%%n",
                        ph, pct, dMsgs, ops, S.activeConns.get(), avg(cpuProc), avg(cpuSys));

                // Write one 'ts' row
                try {
                    csv.write(String.join(",",
                            "ts",
                            // ts identity
                            tsNow(), String.valueOf(elapsedMs(S.warmStartNs, now)), ph.name(),
                            // meta cols (empty on ts rows)
                            "", "", "", "", "", "", "", "",
                            // counters / rates
                            String.valueOf(msgs), String.valueOf(dMsgs), String.valueOf(errs), String.valueOf(dErrs),
                            f(ops), f(mbs),
                            // latency (window) ns-level
                            String.valueOf(ls.count), String.valueOf(ls.min), String.valueOf(ls.p50), String.valueOf(ls.p90),
                            String.valueOf(ls.p99), String.valueOf(ls.p999), String.valueOf(ls.max),
                            // CPU snapshot and connections
                            f(p), f(s), String.valueOf(S.activeConns.get()),
                            // summary extras left blank on ts rows
                            "", "", "", "", "", "", "", "", ""
                    ));
                    csv.newLine();
                    csv.flush();
                } catch (IOException ioe) {
                    System.err.println("CSV write error: " + ioe.getMessage());
                }
            }
        }

        Summary summary() {
            long[] all = allLats.toArray();
            Arrays.sort(all);
            long min = all.length > 0 ? all[0] : 0;
            long max = all.length > 0 ? all[all.length-1] : 0;
            long p50 = q(all, 0.50), p90 = q(all, 0.90), p99 = q(all, 0.99), p999 = q(all, 0.999);
            double cpuP = avg(cpuProc), cpuS = avg(cpuSys);
            long end = (measEndNs == 0 ? System.nanoTime() : measEndNs);
            double durSec = (end - measStartNs) / 1e9;
            Summary sum = new Summary();
            sum.measStartNs = measStartNs; sum.measEndNs = end; sum.measDurationSec = Math.max(1e-9, durSec);
            sum.totalMsgs = S.totalMsgs.sum(); sum.totalErrs = S.totalErrs.sum();
            sum.latCount = all.length; sum.latMinNs=min; sum.latMaxNs=max; sum.latP50Ns=p50; sum.latP90Ns=p90; sum.latP99Ns=p99; sum.latP999Ns=p999;
            sum.cpuProcAvg = cpuP; sum.cpuSysAvg = cpuS;
            return sum;
        }

        private static String f(double d){ return String.format(Locale.ROOT, "%.6f", d); }
        private static double avg(List<Double> xs){ return xs.isEmpty()?0.0:xs.stream().mapToDouble(Double::doubleValue).average().orElse(0.0); }
        private static String tsNow(){ return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")); }
    }

    // ---------------------- Helper structs ----------------------
    static final class Summary {
        long measStartNs, measEndNs;
        double measDurationSec;
        long totalMsgs, totalErrs;
        int  latCount;
        long latMinNs, latMaxNs, latP50Ns, latP90Ns, latP99Ns, latP999Ns;
        double cpuProcAvg, cpuSysAvg;
    }
    static final class LatStats {
        final int count; final long min, p50, p90, p99, p999, max;
        LatStats(int c,long min,long p50,long p90,long p99,long p999,long max){
            this.count=c; this.min=min; this.p50=p50; this.p90=p90; this.p99=p99; this.p999=p999; this.max=max;
        }
        static LatStats of(List<Long> vals){
            if (vals.isEmpty()) return new LatStats(0,0,0,0,0,0,0);
            Collections.sort(vals);
            int n = vals.size();
            return new LatStats(n, vals.get(0), vals.get(idx(n,0.50)), vals.get(idx(n,0.90)),
                    vals.get(idx(n,0.99)), vals.get(idx(n,0.999)), vals.get(n-1));
        }
        static int idx(int n, double p){ return (int)Math.min(n-1, Math.max(0, Math.round(p*(n-1)))); }
    }
    static final class LongArray {
        long[] a = new long[1<<20]; int n=0;
        void add(long v){ if(n==a.length) a=Arrays.copyOf(a,a.length<<1); a[n++]=v; }
        long[] toArray(){ return Arrays.copyOf(a,n); }
    }

    // ------------------------ Utilities ------------------------
    static void send(Conn c, long thinkNs) throws IOException {
        c.sendBuf.clear();
        c.sendBuf.putLong(System.nanoTime());  // timestamp marker (ns)
        while (c.sendBuf.hasRemaining()) c.sendBuf.put((byte)0);
        c.sendBuf.flip();
        while (c.sendBuf.hasRemaining()) c.ch.write(c.sendBuf);
        if (thinkNs > 0) {
            long t = System.nanoTime() + thinkNs;
            while (System.nanoTime() < t) { /* predictable busy-wait */ }
        }
    }
    static long elapsedMs(long startNs, long nowNs){ return (nowNs - startNs) / 1_000_000L; }
    static long msSince(long base, long start, long end){ return (end - start) / 1_000_000L; }
    static String fmtUs(long ns){ return String.format(Locale.ROOT, "%.3f", ns/1000.0); }
    static Map<String,String> parse(String[] args){
        Map<String,String> m = new HashMap<>();
        for (String s: args) if (s.startsWith("--") && s.contains("=")) {
            int i = s.indexOf('='); m.put(s.substring(2,i), s.substring(i+1));
        }
        return m;
    }
    static void safeClose(AutoCloseable c){ try { if(c!=null) c.close(); } catch(Exception ignored){} }
    static long q(long[] a, double p){
        if (a.length==0) return 0L;
        int i = (int)Math.min(a.length-1, Math.max(0, Math.round(p*(a.length-1))));
        return a[i];
    }
    static double percent(long now, Phase ph, Shared S){
        if (ph == Phase.WARMUP)   return clamp01((now - S.warmStartNs)/(double)(S.warmEndNs - S.warmStartNs))*100.0;
        if (ph == Phase.MEASURE)  return clamp01((now - S.measStartNs)/(double)(S.measEndNs - S.measStartNs))*100.0;
        if (ph == Phase.COOLDOWN) return clamp01((now - S.coolStartNs)/(double)(S.coolEndNs - S.coolStartNs))*100.0;
        return 100.0;
    }
    static double clamp01(double x){ return x<0?0:(x>1?1:x); }
    static String f(double d){ return String.format(Locale.ROOT, "%.6f", d); }
}
