/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import actorbintree.BinaryTreeNode.CopyFinished
import actorbintree.BinaryTreeSet.{Contains, ContainsResult, Insert, Operation, OperationFinished}
import akka.actor.*

import scala.collection.immutable.Queue

object BinaryTreeSet:

  trait Operation:
    def requester: ActorRef
    def id: Int
    def elem: Int

  trait OperationReply:
    def id: Int

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply



class BinaryTreeSet extends Actor:
  import BinaryTreeSet.*
  import BinaryTreeNode.*

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  var pendingQueue = Queue.empty[Operation]

  def receive = normal

  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = { 
    case op: Operation => root forward op
    case GC => 
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
  }

  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op: Operation => pendingQueue :+= op
    case CopyFinished =>
      context.stop(root)
      root = newRoot
      pendingQueue.foreach { root ! _ }
      pendingQueue = Queue.empty
      context.become(normal)
    case GC =>
  }

object BinaryTreeNode:
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
   * Acknowledges that a copy has been completed. This message should be sent
   * from a node to its parent, when this node and all its children nodes have
   * finished being copied.
   */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor:
  import BinaryTreeNode.*
  import BinaryTreeSet.*

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  def receive = normal

  private def defineChild(e: Int): Position = if (e < elem) Left else Right

  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, e) =>
      if (e == elem) {
        removed = false
        requester ! OperationFinished(id)
      } else {
        val child = defineChild(e)
        if (!subtrees.contains(child)) subtrees += (child -> context.actorOf(BinaryTreeNode.props(e, initiallyRemoved = false)))
        subtrees(child) ! Insert(requester, id, e)
      }
    case Contains(requester, id, e) => 
      if (e == elem) requester ! ContainsResult(id, !removed)
      else {
        subtrees.get(defineChild(e)) match {
          case Some(ref) => ref ! Contains(requester, id, e)
          case None => requester ! ContainsResult(id, false)
        }
      }
    case Remove(requester, id, e) =>
      if (e == elem) {
        removed = true
        requester ! OperationFinished(id)
      } else {
        subtrees.get(defineChild(e)) match {
          case Some(ref) => ref ! Remove(requester, id, e)
          case None => requester ! OperationFinished(id)
        }
      }
    case CopyTo(newNode) => 
      if (!removed) {
        newNode ! Insert(self, -1, elem)
      }
      subtrees.values.foreach { _ ! CopyTo(newNode) }
      if (removed && subtrees.isEmpty) {
        sender ! CopyFinished
      } else {
        context.become(copying(subtrees.values.toSet, removed, sender))
      }
      
  }

  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean, parent: ActorRef): Receive = {
    case OperationFinished(_) =>
      expected.isEmpty match {
        case true =>
          parent ! CopyFinished
          context.become(normal)
        case false => context.become(copying(expected, true, parent))
      }
    case CopyFinished =>
      val updatedExpected = expected - sender
      updatedExpected.isEmpty match {
        case true if insertConfirmed => 
          parent ! CopyFinished
          context.become(normal)
        case _ => context.become(copying(updatedExpected, insertConfirmed, parent))
      }
  }
