package com.chriniko.filefacade;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

@ParserFacade.ThreadSafe
@ParserFacade.SingletonScope
public class ParserFacade {

	@GuardedBy(threadAccess = "ParserFacade class monitor")
	private static boolean ACQUIRE_GLOBAL_FILE_LOCK = false;

	private static final int READ_LOCK_TIMEOUT_SEC = 1;
	private static final int WRITE_LOCK_TIMEOUT_SEC = 1;

	private static final int OP_READ_RETRY_COUNT = 5;
	private static final int BUFFER_SIZE = 1024;

	@GuardedBy(threadAccess = "ParserFacade class monitor")
	private static ParserFacade INSTANCE;

	private final StampedLock stampedLock = new StampedLock();

	/*
		Note: From multiple thread access protection we could have used also AtomicReference, AtomicReferenceFieldUpdater,
		      sun.misc.Unsafe, or in Java9 VarHandle, based on contention (mid to low --> Atomic, high --> Lock).


		      From protection from different JVM processes, or other processes (for example this file might modified
		      from other applications-programs, etc.) it will be good
		      to use FileChannel-->FileLock (lock globally)
		      on top of our multithreading application management.

	 */
	@GuardedBy(threadAccess = "stampedLock", processAccess = "java.nio.channels.FileLock")
	private File file;

	/*
		Note: We use this mutex, in order to eliminate OverlappingFileLockException errors occurred inside the same JVM when
			  we want to acquire global file lock.
	 */
	private final Object MUTEX = new Object();

	private ParserFacade() {
		// Note: no instantiation
	}

	public static ParserFacade getInstance() {
		synchronized (ParserFacade.class) {
			if (INSTANCE == null) {
				INSTANCE = new ParserFacade();
			}
			return INSTANCE;
		}
	}

	public void setAcquireGlobalFileLock(boolean b) {
		synchronized (ParserFacade.class) {
			ACQUIRE_GLOBAL_FILE_LOCK = b;
		}
	}

	public Pair<Optional<File>, Optional<Long> /*optimistic read stamp*/> getFile() {

		// Note: first try optimistic read (non-pessimistic read lock). 90% of the cases.
		for (int i = 0; i < OP_READ_RETRY_COUNT; i++) {
			long optimisticRead = stampedLock.tryOptimisticRead();
			Optional<File> current = Optional.ofNullable(file);
			if (stampedLock.validate(optimisticRead)) {
				return Pair.of(current, Optional.of(optimisticRead));
			}
		}

		// Note: if we are here, we should downgrade to pessimistic read lock, it should be 10% of the cases.
		final long stamp = acquireReadLock();
		if (stamp != 0) {
			try {
				Optional<File> current = Optional.ofNullable(this.file);
				return Pair.of(current, Optional.empty());
			} finally {
				stampedLock.unlockRead(stamp);
			}
		} else {
			throw new CouldNotAcquireReadLock();
		}

	}

	public void setFile(File f) {
		setFile(f, null);
	}

	public void setFile(File f, @Nullable Long optimisticReadStamp) {

		// Note: try acquire write lock from optimistic read if provided
		if (optimisticReadStamp != null) {
			long stamp = stampedLock.tryConvertToWriteLock(optimisticReadStamp);
			if (stamp != 0) { // Note: acquired write lock
				try {
					file = f;
					return;
				} finally {
					stampedLock.unlockWrite(stamp);
				}
			}
		}

		// Note: otherwise, acquire write lock
		final long stamp = acquireWriteLock();
		if (stamp != 0) {
			try {
				file = f;
			} finally {
				stampedLock.unlockWrite(stamp);
			}
		} else {
			throw new CouldNotAcquireWriteLock();
		}
	}

	public Pair<String, Optional<Long> /*optimistic read stamp*/> getContent() {
		return getContentOperation(() -> __getContent(StandardCharsets.UTF_8, ACQUIRE_GLOBAL_FILE_LOCK));
	}

	public Pair<String, Optional<Long> /*optimistic read stamp*/> getContentWithoutUnicode() {
		return getContentOperation(this::__getContentWithoutUnicode);
	}

	public void saveContent(String content) {
		saveContent(content, StandardCharsets.UTF_8, null, false);
	}

	public void saveContent(String content, boolean append) {
		saveContent(content, StandardCharsets.UTF_8, null, append);
	}

	public void saveContent(String content, boolean append, long optimisticReadStamp) {
		saveContent(content, StandardCharsets.UTF_8, optimisticReadStamp, append);
	}

	public void clearContent() {
		saveContent("");
	}

	private void saveContent(String content,
			Charset encoding,
			@Nullable Long optimisticReadStamp,
			boolean append) {

		saveContentOperation(content,
				encoding,
				optimisticReadStamp,
				(_content, _encoding) -> __saveContent(_content, _encoding, append, ACQUIRE_GLOBAL_FILE_LOCK)
		);
	}

	private void saveContentOperation(String content,
			Charset encoding,
			@Nullable Long optimisticReadStamp,
			BiConsumer<String, Charset> contentStorage) {

		// Note: try acquire write lock from optimistic read if provided
		if (optimisticReadStamp != null) {
			long stamp = stampedLock.tryConvertToWriteLock(optimisticReadStamp);
			if (stamp != 0) { // Note: acquired write lock
				try {
					contentStorage.accept(content, encoding);
					return;
				} finally {
					stampedLock.unlockWrite(stamp);
				}
			}
		}

		// Note: otherwise, acquire write lock
		final long stamp = acquireWriteLock();
		if (stamp != 0) {
			try {
				contentStorage.accept(content, encoding);
			} finally {
				stampedLock.unlockWrite(stamp);
			}
		} else {
			throw new CouldNotAcquireWriteLock();
		}
	}

	private Pair<String, Optional<Long> /*optimistic read stamp*/> getContentOperation(Supplier<String> contentProvider) {
		// Note: first try optimistic read (non-pessimistic read lock). 90% of the cases.
		for (int i = 0; i < OP_READ_RETRY_COUNT; i++) {
			long optimisticRead = stampedLock.tryOptimisticRead();
			String current = contentProvider.get();
			if (stampedLock.validate(optimisticRead)) {
				return Pair.of(current, Optional.of(optimisticRead));
			}
		}

		// Note: if we are here, we should downgrade to pessimistic read lock, it should be 10% of the cases.
		final long stamp = acquireReadLock();
		if (stamp != 0) {
			try {
				return Pair.of(contentProvider.get(), Optional.empty());
			} finally {
				stampedLock.unlockRead(stamp);
			}
		} else {
			throw new CouldNotAcquireReadLock();
		}
	}

	private void __saveContent(String content, Charset encoding, boolean append, boolean acquireGlobalFileLock) {
		try (FileOutputStream o = new FileOutputStream(file, append)) {

			if (acquireGlobalFileLock) {
				synchronized (MUTEX) {
					FileLock exclusiveLock = o.getChannel().tryLock(0L, Long.MAX_VALUE, false);
					if (exclusiveLock == null) {
						throw new CouldNotAcquireGlobalFileLock(false);
					}
					try {
						writeToFile(content, encoding, o);
					} finally {
						exclusiveLock.release();
					}
				}
			} else {
				writeToFile(content, encoding, o);
			}

		} catch (IOException error) {
			throw new SaveOperationFailure("could not save content", error);
		}
	}

	private String __getContent(Charset charset, boolean acquireGlobalFileLock) {

		try (FileInputStream i = new FileInputStream(file)) {
			StringBuilder output = new StringBuilder();

			if (acquireGlobalFileLock) {
				synchronized (MUTEX) {
					FileLock sharedLock = i.getChannel().tryLock(0, Long.MAX_VALUE, true);
					if (sharedLock == null) {
						throw new CouldNotAcquireGlobalFileLock(true);
					}
					try {
						readFromFile(charset, i, output);
					} finally {
						sharedLock.release();
					}
				}
			} else {
				readFromFile(charset, i, output);
			}

			return output.toString();

		} catch (IOException error) {
			throw new ReadOperationFailure("could not read content", error);
		}

	}

	// Note: from old code --> 0x80 == 128 in decimal which is exclusive upper limit of ascii, so we read in ascii encoding.
	private String __getContentWithoutUnicode() {
		return __getContent(StandardCharsets.US_ASCII, ACQUIRE_GLOBAL_FILE_LOCK);
	}

	private void readFromFile(Charset charset, FileInputStream in, StringBuilder output) throws IOException {
		int data;
		byte[] buf = new byte[BUFFER_SIZE];
		while ((data = in.read(buf, 0, buf.length)) > 0) {
			output.append(new String(buf, 0, data, charset));
		}
	}

	private void writeToFile(String content, Charset encoding, FileOutputStream out) throws IOException {
		byte[] contentBytes = content.getBytes(encoding);
		out.write(contentBytes, 0, contentBytes.length);
	}

	private long acquireReadLock() {
		try {
			return stampedLock.tryReadLock(READ_LOCK_TIMEOUT_SEC, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}
	}

	private long acquireWriteLock() {
		try {
			return stampedLock.tryWriteLock(WRITE_LOCK_TIMEOUT_SEC, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}
	}

	@Documented
	@Target({ ElementType.PARAMETER })
	@Retention(RetentionPolicy.SOURCE) // Note: documentation purpose
	@interface Nullable {
	}

	@Documented
	@Target({ ElementType.FIELD })
	@Retention(RetentionPolicy.SOURCE) // Note: documentation purpose
	@interface GuardedBy {

		String threadAccess();

		String processAccess() default "";
	}

	@Documented
	@Target({ ElementType.TYPE })
	@Retention(RetentionPolicy.SOURCE) // Note: documentation purpose
	@interface ThreadSafe {
	}

	@Documented
	@Target({ ElementType.TYPE })
	@Retention(RetentionPolicy.SOURCE) // Note: documentation purpose
	@interface SingletonScope {
	}

	// ---- code infrastructure(ds, errors, etc.) ---
	static class Pair<E1, E2> {

		private final E1 first;
		private final E2 second;

		private Pair(E1 first, E2 second) {
			this.first = first;
			this.second = second;
		}

		public static <E1, E2> Pair<E1, E2> of(E1 e1, E2 e2) {
			return new Pair<>(e1, e2);
		}

		public E1 getFirst() {
			return first;
		}

		public E2 getSecond() {
			return second;
		}
	}

	static class SaveOperationFailure extends RuntimeException {

		public SaveOperationFailure(String message, Throwable e) {
			super(message, e);
		}
	}

	static class ReadOperationFailure extends RuntimeException {

		public ReadOperationFailure(String message, Throwable e) {
			super(message, e);
		}
	}

	static class CouldNotAcquireReadLock extends RuntimeException {
		public CouldNotAcquireReadLock() {
			super("could not acquire read lock");
		}
	}

	static class CouldNotAcquireWriteLock extends RuntimeException {
		public CouldNotAcquireWriteLock() {
			super("could not acquire write lock");
		}
	}

	static class CouldNotAcquireGlobalFileLock extends RuntimeException {

		public CouldNotAcquireGlobalFileLock(boolean shared) {
			super("could not acquire global file lock, shared: " + shared);
		}
	}
}
