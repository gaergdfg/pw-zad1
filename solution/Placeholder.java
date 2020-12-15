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
// TODO: Change the name
// TODO: Change the name in `TransactionManagerFactory.java`
public final class Placeholder implements TransactionManager {

	/**
	 * Class representing a single operation of a transaction, containing
	 * a reference to the modified resource and the operation that was used
	 * to modify it.
	 */
	private final class Operation {
		
		Resource resource;
		ResourceOperation resourceOperation;

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

	Collection<Resource> resources;
	LocalTimeProvider timeProvider;

	ConcurrentHashMap<Long, Long> transactionStart;
	ConcurrentHashMap<Long, TransactionState> transactionState;
	ConcurrentHashMap<Long, Stack<Operation>> transactionOperations;
	ConcurrentHashMap<Long, Set<ResourceId>> transactionResources;
	ConcurrentHashMap<ResourceId, Long> resourceOwner;

	public Placeholder(
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
		if (!isTransactionActive()) {
			throw new NoActiveTransactionException();
		}
		Resource resource = findResourceById(rid);
		if (resource == null) {
			throw new UnknownResourceIdException(rid);
		}
		if (isTransactionAborted()) {
			throw new ActiveTransactionAborted();
		}
		
		// TODO: do different kind of magic
		// mapa <resourceId, Collection<Long>> - kolekcja czekajacych na resource
		// wait() and notify() dla watkow czekajacych na dany resource
	}

	public void commitCurrentTransaction(
	) throws
		NoActiveTransactionException,
		ActiveTransactionAborted {
		if (!isTransactionActive()) {
			throw new NoActiveTransactionException();
		}
		if (isTransactionAborted()) {
			throw new ActiveTransactionAborted();
		}

		long tid = Thread.currentThread().getId();
		transactionStart.remove(tid);
		transactionOperations.remove(tid);
		// TODO: unlink all resources from thread

		transactionState.remove(tid);
	}

	public void rollbackCurrentTransaction() {
		long tid = Thread.currentThread().getId();
		if (transactionStart.contains(tid)) {
			return;
		}

		transactionStart.remove(tid);
		while (!transactionOperations.get(tid).empty()) {
			Operation operation = transactionOperations.get(tid).pop();
			Resource resource = operation.getResource();
			ResourceOperation resourceOperation =
				operation.getResourceOperation();
			
			resourceOperation.undo(resource);
		}
		// TODO: unlink all resources from thread
		
		transactionState.remove(tid);
	}

	public boolean isTransactionActive() {
		long tid = Thread.currentThread().getId();
		return transactionState.get(tid) == TransactionState.ONGOING;
	}

	public boolean isTransactionAborted() {
		long tid = Thread.currentThread().getId();
		return transactionState.get(tid) == TransactionState.ABORTED;
	}

	/**
	 * Finds a resource with id equal to [rid].
	 * @param rid : id of the resource
	 * @return Resource with a matching id or null.
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
