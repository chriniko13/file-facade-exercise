package com.chriniko.filefacade;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ParserFacadeTest {

	private ParserFacade parserFacade;

	@Before
	public void init() {
		parserFacade = ParserFacade.getInstance();
	}

	@Test
	public void setFileGetFileWorksAsExpected() throws Exception {

		// given
		int idx = ThreadLocalRandom.current().nextInt(10);
		Path testFile = Files.createTempFile("testFile__", idx + "");

		// when
		parserFacade.setFile(testFile.toFile());

		// then
		ParserFacade.Pair<Optional<File>, Optional<Long>> result = parserFacade.getFile();
		assertTrue(result.getFirst().isPresent());
		// Note: test runs from one thread so always we have optimistic read stamp.
		assertTrue(result.getSecond().isPresent());

		result.getSecond().ifPresent(optimisticReadStamp -> {

			try {

				// given
				Path newTestFile = Files.createTempFile("testFile__", idx + "");

				// when
				parserFacade.setFile(newTestFile.toFile(), optimisticReadStamp);

				// then
				ParserFacade.Pair<Optional<File>, Optional<Long>> r = parserFacade.getFile();
				assertTrue(r.getFirst().isPresent());
				// Note: test runs from one thread so always we have optimistic read stamp.
				assertTrue(r.getSecond().isPresent());

			} catch (Exception e) {
				fail(e.getMessage());
				e.printStackTrace(System.err);
			}

		});

	}

	@Test
	public void saveContentAndGetContentWorksAsExpected() throws Exception {

		// given
		String toWrite = "hello world, {} this is ascii, ôą this is utf-8 ";

		int idx = ThreadLocalRandom.current().nextInt(10);

		Path testFile = Files.createTempFile("testFile__", idx + "");
		parserFacade.setFile(testFile.toFile());

		// when
		parserFacade.saveContent(toWrite);

		ParserFacade.Pair<String, Optional<Long>> result = parserFacade.getContent();
		ParserFacade.Pair<String, Optional<Long>> resultNoUnicode = parserFacade.getContentWithoutUnicode();

		// then
		assertEquals(toWrite, result.getFirst());
		assertTrue(result.getSecond().isPresent());

		// Note: we can see utf8 characters not displayed properly, because we read with ascii encoding.
		assertEquals(
				"hello world, {} this is ascii, ���� this is utf-8 ",
				resultNoUnicode.getFirst()
		);
		assertTrue(resultNoUnicode.getSecond().isPresent());

	}

	@Test
	public void saveContentAppendModeWorksAsExpected() throws Exception {

		// given
		int idx = ThreadLocalRandom.current().nextInt(10);

		Path testFile = Files.createTempFile("testFile__", idx + "");
		parserFacade.setFile(testFile.toFile());

		// when
		String contents = "a,b,c,d,e,f,g";
		for (String elem : contents.split(",")) {
			parserFacade.saveContent(elem, true);
		}

		// then
		assertEquals("abcdefg", parserFacade.getContent().getFirst());

	}

	@Test
	public void checkMultiThreadOperationsSafety_PessimisticRead_AcquireGlobalLock() throws Exception {

		// given
		parserFacade.setAcquireGlobalFileLock(true);
		int runs = 200;

		// Note: comment/uncomment if you want to run for more than one process.
		//Path testFile = Paths.get("/Users/chriniko/mytxt.txt"); // Note: the file should be created outside of IDE, and should be clear (no contents)
		//int numberOfConcurrentProcesses = 1; // Note: modify accordingly

		// Note: comment/uncomment if you want to run for one process only.
		int idx = ThreadLocalRandom.current().nextInt(10);
		Path testFile = Files.createTempFile("testFile__", idx + "");

		parserFacade.setFile(testFile.toFile());

		//parserFacade.clearContent();

		for (int i = 0; i < runs; i++) {
			checkMultiThreadOperationsSafety(i, true, 1);
		}
	}

	@Test
	public void checkMultiThreadOperationsSafety_PessimisticRead_NoAcquireGlobalLock() throws Exception {

		// given
		int runs = 100;

		int idx = ThreadLocalRandom.current().nextInt(10);
		Path testFile = Files.createTempFile("testFile__", idx + "");
		parserFacade.setFile(testFile.toFile());

		for (int i = 0; i < runs; i++) {
			checkMultiThreadOperationsSafety(i, false, 1);
		}
	}

	@Test
	public void checkMultiThreadOperationsSafety_OptimisticRead_AcquireGlobalLock() throws Exception {

		// given
		parserFacade.setAcquireGlobalFileLock(true);

		int runs = 100;

		int idx = ThreadLocalRandom.current().nextInt(10);
		Path testFile = Files.createTempFile("testFile__", idx + "");
		parserFacade.setFile(testFile.toFile());

		for (int i = 0; i < runs; i++) {
			checkMultiThreadOperationsSafety(i, true, 1);
		}

	}

	@Test
	public void checkMultiThreadOperationsSafety_OptimisticRead_NoAcquireGlobalLock() throws Exception {

		// given
		int runs = 100;

		int idx = ThreadLocalRandom.current().nextInt(10);
		Path testFile = Files.createTempFile("testFile__", idx + "");
		parserFacade.setFile(testFile.toFile());

		for (int i = 0; i < runs; i++) {
			checkMultiThreadOperationsSafety(i, true, 1);
		}

	}

	// --- utils ---

	private void checkMultiThreadOperationsSafety(int run, boolean tryOptimisticReads, int numberOfConcurrentProcesses) {

		// create some readers to create traffic for read lock.
		int readersSize = 70;
		Phaser readersOnReadyState = new Phaser(readersSize + 1  /* plus one for junit test thread / waiter role */);

		ExecutorService readers = Executors.newFixedThreadPool(readersSize, new ThreadFactory() {

			private final AtomicInteger idx = new AtomicInteger(0);

			@Override public Thread newThread(Runnable r) {
				Thread t = new Thread(r);
				t.setName("reader--" + idx.getAndIncrement());
				return t;
			}
		});
		AtomicBoolean readersRunning = new AtomicBoolean(true);

		IntStream.rangeClosed(1, readersSize).forEach(readerIdx -> {
			readers.submit(() -> {
				try {
					readersOnReadyState.arriveAndDeregister();

					while (readersRunning.get()) {
						try {
							parserFacade.getContent();
							Thread.sleep(70);
							Thread.yield();
							parserFacade.getContentWithoutUnicode();
							Thread.sleep(40);
							Thread.yield();
						} catch (ParserFacade.CouldNotAcquireGlobalFileLock ex) { // Note: we need to catch when running multiple processes for global file lock.
							// Note: in cases where we run more than one junit test case process to check global file lock (native OS file lock)
							//       we just retry....
							System.out.println(Thread.currentThread().getName() + " --- will retry, reader error message: " + ex.getMessage());
						}
					}

				} catch (Exception e) { // Note: on unknown exception, let the reader exit.
					System.err.println(Thread.currentThread().getName() + " -- reader UNKNOWN error: " + e.getMessage());
				}
			});
		});

		readersOnReadyState.arriveAndAwaitAdvance();

		// create writers to create traffic for write lock.
		int workersSize = 40;

		Phaser writersFinishedWork = new Phaser(workersSize + 1 /* plus one for junit test thread / waiter role */);

		CyclicBarrier writersRendezvousBeforeWork = new CyclicBarrier(workersSize
				//, () -> System.out.println("all workers (size = " + workersSize + ") at 'fair' position to access/test save contents method")
		);

		ExecutorService writers = Executors.newFixedThreadPool(workersSize, new ThreadFactory() {

			private final AtomicInteger idx = new AtomicInteger(0);

			@Override public Thread newThread(Runnable r) {
				Thread t = new Thread(r);
				t.setName("writer--" + idx.getAndIncrement());
				return t;
			}
		});

		// when
		int timesEachWorkerWillSave = 10;

		for (int w = 0; w < workersSize; w++) {

			writers.submit(() -> {
				// ~~ rendezvous point ~~
				try {
					writersRendezvousBeforeWork.await();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				} catch (BrokenBarrierException ignored) {
					Assert.fail();
				}

				// ~~ actual work ~~

				int counter = timesEachWorkerWillSave;
				while (counter > 0) {

					try {

						if (tryOptimisticReads) {

							Optional<Long> optimisticReadStampH = Optional.empty();
							while (!optimisticReadStampH.isPresent()) {
								ParserFacade.Pair<String, Optional<Long>> result = parserFacade.getContent();
								optimisticReadStampH = result.getSecond();
							}
							parserFacade.saveContent("1,", true, optimisticReadStampH.get());

						} else {
							parserFacade.saveContent("1,", true);
						}
						counter--;

					} catch (ParserFacade.CouldNotAcquireGlobalFileLock ex) { // Note: we need to catch when running multiple processes for global file lock.
						// Note: in cases where we run more than one junit test case process to check global file lock (native OS file lock)
						//       we just retry....
						System.out.println(Thread.currentThread().getName() + " ### retrying... error message: " + ex.getMessage() + " --- progress: " + counter);
					}

				}

				writersFinishedWork.arriveAndDeregister();
			});

		} // workers operation END.

		// then
		writersFinishedWork.arriveAndAwaitAdvance();
		readersRunning.set(false);

		String contents = "";
		while (true) {
			try {
				contents = parserFacade.getContent().getFirst();
				break;
			} catch (ParserFacade.CouldNotAcquireGlobalFileLock e) {
				// Note: retry until you get the contents
				System.out.println(Thread.currentThread().getName() + " ###### could not extract content... retrying... error message: " + e.getMessage());

				try {
					Thread.sleep(300);
				} catch (InterruptedException ignored) {
				}
			}
		}

		int sum = Arrays.stream(contents.split(","))
				.map(Integer::parseInt)
				.reduce(0, Integer::sum);
		System.out.printf("[%s] DEBUG run: %d --- sum is: %d\n", Thread.currentThread().getName(), run, sum);

		if (numberOfConcurrentProcesses > 1) {

			int totalWriteFromOneProcess = workersSize * timesEachWorkerWillSave;
			int totalWritesFromAllProcesses = totalWriteFromOneProcess * numberOfConcurrentProcesses;

			assertTrue(sum >= totalWriteFromOneProcess);
			assertTrue(sum <= totalWritesFromAllProcesses);

		} else {
			assertEquals(
					workersSize * timesEachWorkerWillSave,
					sum
			);
		}

		//cleanup
		for (; ; ) {
			try {
				parserFacade.clearContent();
				break;
			} catch (ParserFacade.CouldNotAcquireGlobalFileLock e) {
				// Note: just retry, until you clear the contents.
			}
		}
		readersRunning.set(false);

		writers.shutdown();
		readers.shutdown();
	}

}
