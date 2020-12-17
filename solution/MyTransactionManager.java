/*
 * University of Warsaw
 * Concurrent Programming Course 2020/2021
 * Java Assignment
 * 
 * Author: Piotr Prabucki
 */
package cp1.solution;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.Set;
import java.util.HashSet;
import java.util.Stack;

import cp1.base.ActiveTransactionAborted;
import cp1.base.AnotherTransactionActiveException;
import cp1.base.LocalTimeProvider;
import cp1.base.NoActiveTransactionException;
import cp1.base.ResourceId;
import cp1.base.Resource;
import cp1.base.ResourceOperationException;
import cp1.base.ResourceOperation;
import cp1.base.TransactionManager;
import cp1.base.UnknownResourceIdException;

/**
 * Implementation of a transaction manager.
 * 
 * @author Piotr Prabucki
 */
public final class MyTransactionManager implements TransactionManager {

	/**
	 * Class representing a single operation of a transaction, containing
	 * a reference to the modified resource and the operation that was used
	 * to modify it.
	 */
	private final class Operation {
		
		private Resource resource;
		private ResourceOperation resourceOperation;

		public Operation(
			Resource resource,
			ResourceOperation resourceOperation
		) {
			this.resource = resource;
			this.resourceOperation = resourceOperation;
		}

		public Resource getResource() {
			return resource;
		}

		public ResourceOperation getResourceOperation() {
			return resourceOperation;
		}

	}

	/**
	 * Enum representing states that transaction can be in.
	 */
	enum TransactionState {
		ONGOING,
		ABORTED
	}

	private Collection<Resource> resources;
	private LocalTimeProvider timeProvider;

	private ConcurrentHashMap<Long, Long> transactionStart;
	private ConcurrentHashMap<Long, TransactionState> transactionState;
	private ConcurrentHashMap<Long, Stack<Operation>> transactionOperations;
	private ConcurrentHashMap<Long, Set<ResourceId>> transactionResources;
	private ConcurrentHashMap<ResourceId, Thread> resourceOwner;
	private ConcurrentHashMap<Long, ResourceId> requestedResource;
	private ConcurrentHashMap<ResourceId, Semaphore> resourceMutex;

	private Semaphore deadLockCheckMutex;

	/**
	 * Acquires resource for a thread.
	 * Thread gets the resource immediately if the resource was used earlier in
	 * its transaction.
	 * Otherwise, if requesting the resource would result in a dead lock,
	 * one thread (specified in the task description) gets interrupted and its
	 * transaction aborted.
	 * If thread's transaction wasn't aborted in this action, it acquires the
	 * resource's semaphore.
	 * @param tid : id of the thread
	 * @param rid : id of the resource
	 * @throws InterruptedException
	 */
	private void acquireResource(
		Long tid,
		ResourceId rid
	) throws
		InterruptedException {
		if (!transactionResources.get(tid).contains(rid)) {
			try {
				deadLockCheckMutex.acquire();
			}
			catch (Exception e) {
				return;
			}
			
			requestedResource.put(tid, rid);
			
			Thread threadToCancel = Thread.currentThread();
			Thread current = Thread.currentThread();
			Thread nextThread = resourceOwner.get(requestedResource.get(current.getId()));

			while (nextThread != null && nextThread.getId() != tid) {
				long currentTime = transactionStart.get(nextThread.getId());
				long threadToCancelTime = transactionStart.get(threadToCancel.getId());

				if (currentTime > threadToCancelTime) {
					threadToCancel = nextThread;
				}
				else if (
					currentTime == threadToCancelTime &&
					nextThread.getId() > threadToCancel.getId()
				) {
					threadToCancel = nextThread;
				}

				current = nextThread;
				if (requestedResource.get(current.getId()) == null) {
					nextThread = null;
				}
				else {
					nextThread = resourceOwner.get(requestedResource.get(current.getId()));
				}
			}

			if (nextThread != null) {
				if (threadToCancel.getId() == tid) {
					requestedResource.remove(tid);
					transactionState.replace(tid, TransactionState.ABORTED);
					deadLockCheckMutex.release();
					return;
				}
				else {
					transactionState.replace(threadToCancel.getId(), TransactionState.ABORTED);
					threadToCancel.interrupt();
				}
			}
			deadLockCheckMutex.release();

			try {
				resourceMutex.get(rid).acquire();
				requestedResource.remove(tid);
				resourceOwner.put(rid, Thread.currentThread());
				transactionResources.get(tid).add(rid);
			}
			catch (InterruptedException e) {
				throw e;
			}
		}
	}

	/**
	 * Releases a permit to each of thread's resources semaphores.
	 * Erases data about the connections.
	 * @param tid : id of the thread
	 */
	private void releaseResources(long tid) {
		if (transactionResources.get(tid) == null) {
			return;
		}

		for (ResourceId rid : transactionResources.get(tid)) {
			resourceOwner.remove(rid);
			resourceMutex.get(rid).release();
		}
		transactionResources.remove(tid);
	}

	public MyTransactionManager(
		Collection<Resource> resources,
		LocalTimeProvider timeProvider
	) {
		this.resources = resources;
		this.timeProvider = timeProvider;

		transactionStart = new ConcurrentHashMap<>();
		transactionState = new ConcurrentHashMap<>();
		transactionOperations = new ConcurrentHashMap<>();
		transactionResources = new ConcurrentHashMap<>();
		resourceOwner = new ConcurrentHashMap<>();
		requestedResource = new ConcurrentHashMap<>();
		resourceMutex = new ConcurrentHashMap<>();
		for (Resource resource : resources) {
			resourceMutex.put(resource.getId(), new Semaphore(1, true));
		}

		deadLockCheckMutex = new Semaphore(1, true);
	}

	public void startTransaction(
	) throws
		AnotherTransactionActiveException {
		if (isTransactionActive()) {
			throw new AnotherTransactionActiveException();
		}

		long tid = Thread.currentThread().getId();

		transactionStart.put(tid, timeProvider.getTime());
		transactionState.put(tid, TransactionState.ONGOING);
		transactionOperations.put(tid, new Stack<Operation>());
		transactionResources.put(tid, new HashSet<ResourceId>());
	}

	public void operateOnResourceInCurrentTransaction(
		ResourceId rid,
		ResourceOperation operation
	) throws
		NoActiveTransactionException,
		UnknownResourceIdException,
		ActiveTransactionAborted,
		ResourceOperationException,
		InterruptedException {
		if (isTransactionAborted()) {
			throw new ActiveTransactionAborted();
		}
		if (!isTransactionActive()) {
			throw new NoActiveTransactionException();
		}
		Resource resource = findResourceById(rid);
		if (resource == null) {
			throw new UnknownResourceIdException(rid);
		}
		
		long tid = Thread.currentThread().getId();

		try {
			acquireResource(tid, rid);
		}
		catch (InterruptedException e) {
			throw e;
		}

		try {
			operation.execute(resource);
			transactionOperations.get(tid).add(new Operation(resource, operation));
		}
		catch (ResourceOperationException e) {
			throw e;
		}
	}

	public void commitCurrentTransaction(
	) throws
		NoActiveTransactionException,
		ActiveTransactionAborted {
		if (isTransactionAborted()) {
			throw new ActiveTransactionAborted();
		}
		if (!isTransactionActive()) {
			throw new NoActiveTransactionException();
		}

		long tid = Thread.currentThread().getId();

		try {
			deadLockCheckMutex.acquire();
		}
		catch (Exception e) {
			return;
		}

		transactionStart.remove(tid);
		transactionState.remove(tid);
		transactionOperations.remove(tid);
		releaseResources(tid);

		deadLockCheckMutex.release();
	}

	public void rollbackCurrentTransaction() {
		long tid = Thread.currentThread().getId();
		if (transactionStart.contains(tid)) {
			return;
		}
		try {
			deadLockCheckMutex.acquire();
		}
		catch (Exception e) {
			return;
		}

		transactionStart.remove(tid);
		transactionState.remove(tid);
		
		Stack<Operation> operations = transactionOperations.get(tid);
		while (operations != null && !operations.empty()) {
			Operation operation = transactionOperations.get(tid).pop();
			Resource resource = operation.getResource();
			ResourceOperation resourceOperation =
				operation.getResourceOperation();
			
			resourceOperation.undo(resource);
		}
		releaseResources(tid);
		
		deadLockCheckMutex.release();
	}

	public boolean isTransactionActive() {
		long tid = Thread.currentThread().getId();
		return
			transactionState.get(tid) == TransactionState.ONGOING ||
			isTransactionAborted();
	}

	public boolean isTransactionAborted() {
		long tid = Thread.currentThread().getId();
		return transactionState.get(tid) == TransactionState.ABORTED;
	}

	/**
	 * Finds a resource with id equal to [rid].
	 * @param rid : id of the resource
	 * @return Resource with a matching id or null if there is no such resource.
	 */
	private Resource findResourceById(ResourceId rid) {
		for (Resource resource : resources) {
			if (resource.getId() == rid) {
				return resource;
			}
		}

		return null;
	}

}
