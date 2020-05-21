package com.chriniko.filefacade;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
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

		int runs = 100;

		int idx = ThreadLocalRandom.current().nextInt(10);
		Path testFile = Files.createTempFile("testFile__", idx + "");
		parserFacade.setFile(testFile.toFile());


		for (int i = 0; i < runs; i++) {
			checkMultiThreadOperationsSafety(false);
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
			checkMultiThreadOperationsSafety(false);
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
			checkMultiThreadOperationsSafety(true);
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
			checkMultiThreadOperationsSafety(true);
		}

	}

	// --- utils ---

	private void checkMultiThreadOperationsSafety(boolean tryOptimisticReads) {

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
						parserFacade.getContent();
						Thread.sleep(70);
						Thread.yield();
						parserFacade.getContentWithoutUnicode();
						Thread.sleep(40);
						Thread.yield();

					}
				} catch (Exception e) {
					System.err.println(Thread.currentThread().getName() + " -- reader error: " + e.getMessage());
					// let the reader exit.
				}
			});
		});

		readersOnReadyState.arriveAndAwaitAdvance();

		// create writers to create traffic for write lock.
		int workersSize = 40;

		Phaser writersFinishedWork = new Phaser(workersSize + 1 /* plus one for junit test thread / waiter role */);

		CyclicBarrier writersRendezvousBeforeWork = new CyclicBarrier(workersSize,
				() -> System.out.println("all workers (size = " + workersSize + ") at 'fair' position to access/test save contents method")
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
				for (int i=0; i<timesEachWorkerWillSave; i++) {
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
				}
				writersFinishedWork.arriveAndDeregister();
			});

		}

		// then
		writersFinishedWork.arriveAndAwaitAdvance();
		readersRunning.set(false);

		String contents = parserFacade.getContent().getFirst();

		int sum = Arrays.stream(contents.split(","))
				.map(Integer::parseInt)
				.reduce(0, Integer::sum);

		assertEquals(workersSize * timesEachWorkerWillSave, sum);

		//cleanup
		parserFacade.clearContent();

		writers.shutdown();
		readers.shutdown();
	}

}
