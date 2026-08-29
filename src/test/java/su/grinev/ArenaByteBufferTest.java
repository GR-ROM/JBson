package su.grinev;

import org.junit.jupiter.api.Test;
import su.grinev.pool.ArenaByteBuffer;
import su.grinev.pool.DynamicByteBuffer;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ArenaByteBuffer} — a native, arena-backed buffer whose memory
 * is released deterministically by {@link ArenaByteBuffer#destroy()}.
 */
public class ArenaByteBufferTest {

    @Test
    void allocatesLiveNativeMemory() {
        ArenaByteBuffer b = new ArenaByteBuffer(64);
        assertTrue(b.isAlive());
        assertNotEquals(0L, b.address(), "a real native segment has a non-zero address");
        assertEquals(64, b.capacity());
        assertEquals(64, b.getBuffer().capacity());
        b.destroy();
    }

    @Test
    void writesWithinPreallocatedCapacity_neverReallocate() {
        ArenaByteBuffer b = new ArenaByteBuffer(64);
        long address = b.address();

        // fill the buffer completely (16 ints == 64 bytes) checking room each time
        for (int i = 0; i < 16; i++) {
            b.ensureCapacity(Integer.BYTES);
            b.getBuffer().putInt(i);
        }

        assertEquals(address, b.address(), "preallocated capacity is never reallocated while it suffices");
        assertEquals(64, b.capacity());
        b.getBuffer().flip();
        assertEquals(0, b.getBuffer().getInt(0));
        b.destroy();
    }

    @Test
    void readsBackWhatWasWritten() {
        ArenaByteBuffer b = new ArenaByteBuffer(64);
        ByteBuffer buf = b.getBuffer();
        buf.putInt(0, 0x01020304);
        buf.putLong(8, 42L);
        assertEquals(0x01020304, buf.getInt(0));
        assertEquals(42L, buf.getLong(8));
        b.destroy();
    }

    @Test
    void isLittleEndian() {
        ArenaByteBuffer b = new ArenaByteBuffer(8);
        b.getBuffer().putInt(0, 1);
        assertEquals(1, b.getBuffer().get(0), "little-endian: least significant byte first");
        b.destroy();
    }

    @Test
    void manualDestroy_releasesMemory_andMakesAccessFail() {
        ArenaByteBuffer b = new ArenaByteBuffer(64, ArenaByteBuffer.Release.MANUAL);
        assertTrue(b.isAlive());

        b.destroy();

        assertFalse(b.isAlive(), "arena closed -> memory released");
        assertThrows(IllegalStateException.class, () -> b.getBuffer().getInt(0),
                "accessing a buffer after its arena is closed must fail");
    }

    @Test
    void autoMode_destroyIsNoop_andMemoryStaysUsable() {
        ArenaByteBuffer b = new ArenaByteBuffer(64); // AUTO is the default
        b.getBuffer().putInt(0, 123);

        b.destroy(); // no-op in AUTO mode — the GC owns the lifecycle

        assertTrue(b.isAlive(), "AUTO buffer is GC-managed; destroy() must not free it");
        assertEquals(123, b.getBuffer().getInt(0), "buffer remains usable after a no-op destroy");
    }

    @Test
    void dispose_runsOnDisposeCallback_withoutFreeingArena() {
        ArenaByteBuffer b = new ArenaByteBuffer(16);
        AtomicInteger recycles = new AtomicInteger();
        b.setOnDispose(recycles::incrementAndGet);

        b.dispose();

        assertEquals(1, recycles.get(), "dispose runs the recycle callback");
        assertTrue(b.isAlive(), "dispose recycles to the pool, it does not free the arena");
        b.destroy();
    }

    @Test
    void close_delegatesToDispose() {
        ArenaByteBuffer b = new ArenaByteBuffer(16);
        AtomicInteger recycles = new AtomicInteger();
        b.setOnDispose(recycles::incrementAndGet);

        b.close();

        assertEquals(1, recycles.get(), "close() delegates to dispose()");
        b.destroy();
    }

    @Test
    void manualDestroy_isIdempotent() {
        ArenaByteBuffer b = new ArenaByteBuffer(16, ArenaByteBuffer.Release.MANUAL);
        b.destroy();
        assertDoesNotThrow(b::destroy, "a second destroy must be a safe no-op");
    }

    /**
     * Regression: in MANUAL mode {@link DynamicByteBuffer#destroy()} must close the arena
     * (Arena.ofShared() is not GC-reclaimed, so it would otherwise leak).
     */
    @Test
    void dynamicByteBuffer_manualDestroy_closesArena() {
        DynamicByteBuffer b = new DynamicByteBuffer(64, ArenaByteBuffer.Release.MANUAL);
        assertTrue(b.isAlive());

        b.destroy();

        assertFalse(b.isAlive(), "destroy() must close the arena");
        assertThrows(IllegalStateException.class, () -> b.getBuffer().getInt(0));
    }

    @Test
    void ensureCapacity_isNoopWhenEnoughRoom() {
        ArenaByteBuffer b = new ArenaByteBuffer(64);
        long address = b.address();
        b.ensureCapacity(16);
        assertEquals(address, b.address(), "no reallocation when the preallocated capacity already suffices");
        assertTrue(b.isAlive());
        b.destroy();
    }

    /**
     * Fallback path: when content outgrows the preallocated capacity the buffer
     * reallocates into a fresh, larger arena, copies the content, and the old
     * arena is closed. This is the rare safety net, not the hot path.
     */
    @Test
    void ensureCapacity_fallbackGrowsAndPreservesContent() {
        ArenaByteBuffer b = new ArenaByteBuffer(8);
        b.getBuffer().putInt(777); // remaining now 4
        long oldAddress = b.address();

        b.ensureCapacity(64); // 4 < 64 -> outgrows preallocation, must grow

        assertTrue(b.capacity() >= 68);
        assertNotEquals(oldAddress, b.address(), "grown buffer lives in a fresh segment");
        assertTrue(b.isAlive());
        b.getBuffer().flip();
        assertEquals(777, b.getBuffer().getInt(0), "content is copied across the growth");
        b.destroy();
    }

    @Test
    void fixedCapacity_stillAllowsWritesThatFit() {
        ArenaByteBuffer b = new ArenaByteBuffer(64).fixCapacity();
        long address = b.address();

        for (int i = 0; i < 16; i++) {
            b.ensureCapacity(Integer.BYTES);
            b.getBuffer().putInt(i);
        }

        assertTrue(b.isFixedCapacity());
        assertEquals(address, b.address(), "fixing capacity must not change the fitting path");
        assertEquals(0, b.getBuffer().remaining(), "filled to the last byte without throwing");
        b.destroy();
    }

    /**
     * The point of {@link ArenaByteBuffer#fixCapacity()}: for a buffer sized to a protocol bound, a
     * growth is a sizing bug, so it must surface instead of silently allocating outside the pool's
     * direct-memory budget.
     */
    @Test
    void fixedCapacity_throwsInsteadOfGrowing_andLeavesTheBufferIntact() {
        ArenaByteBuffer b = new ArenaByteBuffer(8).fixCapacity();
        b.getBuffer().putInt(777); // remaining now 4
        long address = b.address();

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> b.ensureCapacity(64));

        assertTrue(e.getMessage().contains("64"), "message names the requested size: " + e.getMessage());
        assertEquals(8, b.capacity(), "a rejected growth must not resize");
        assertEquals(address, b.address(), "nor reallocate");
        assertEquals(777, b.getBuffer().getInt(0), "nor disturb the content already written");
        b.destroy();
    }

    /** A view stays attached, because the reallocation that would have detached it cannot happen. */
    @Test
    void fixedCapacity_keepsDuplicateViewsAttached() {
        DynamicByteBuffer b = new DynamicByteBuffer(8).fixCapacity();
        // duplicate() does not inherit the byte order — it is born BIG_ENDIAN regardless of the source.
        ByteBuffer view = b.duplicate().order(b.order());

        assertThrows(IllegalStateException.class, () -> b.ensureCapacity(64));

        b.putInt(0, 555);
        assertEquals(555, view.getInt(0), "the view still aliases the buffer's memory");
        b.destroy();
    }

    /**
     * The property the AUTO allocation strategy exists for: a gather-write of pooled buffers must not
     * allocate. {@code SocketChannel.write(ByteBuffer[])} acquires the memory session of every buffer
     * that has one and builds a releaser per buffer to let go of it — 40 bytes a buffer, on a data
     * plane hundreds of megabytes a minute — and skips all of it for a buffer with no session. An
     * arena-backed buffer has a session; a direct buffer does not. Measured per call with the thread's
     * own allocation counter, so a regression to {@code Arena.ofAuto()} fails here, not on a node.
     */
    @Test
    void gatherWriteOfAutoBuffersAllocatesNothing() throws Exception {
        try (ServerSocketChannel server = ServerSocketChannel.open()) {
            server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            try (SocketChannel client = SocketChannel.open(server.getLocalAddress());
                 SocketChannel accepted = server.accept()) {
                client.configureBlocking(false);
                Thread drain = new Thread(() -> {
                    ByteBuffer sink = ByteBuffer.allocateDirect(1 << 20);
                    try {
                        while (accepted.read(sink) >= 0) {
                            sink.clear();
                        }
                    } catch (Exception ignored) {
                        // the channel closes under us at the end of the test
                    }
                });
                drain.setDaemon(true);
                drain.start();

                ArenaByteBuffer[] owners = new ArenaByteBuffer[16];
                ByteBuffer[] bufs = new ByteBuffer[owners.length];
                for (int i = 0; i < owners.length; i++) {
                    owners[i] = new ArenaByteBuffer(1500);
                    bufs[i] = owners[i].getBuffer();
                }
                com.sun.management.ThreadMXBean threads =
                        (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
                long tid = Thread.currentThread().threadId();
                int rounds = 20_000;
                for (int r = 0; r < 2_000; r++) {          // warm the write path up before counting
                    writeSmall(client, bufs);
                }
                long before = threads.getThreadAllocatedBytes(tid);
                for (int r = 0; r < rounds; r++) {
                    writeSmall(client, bufs);
                }
                double perWrite = (threads.getThreadAllocatedBytes(tid) - before) / (double) rounds;
                assertTrue(perWrite < 8.0, "write(ByteBuffer[]) of 16 pooled buffers allocated "
                        + perWrite + " B per call; a session-backed buffer costs 40 B each");
            }
        }
    }

    private static void writeSmall(SocketChannel channel, ByteBuffer[] bufs) throws java.io.IOException {
        for (ByteBuffer b : bufs) {
            b.clear().limit(64);                            // 16 x 64 B: the socket never fills
        }
        channel.write(bufs);
    }
}
