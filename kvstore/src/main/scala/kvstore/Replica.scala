package kvstore

import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, actorRef2Scala}
import kvstore.Arbiter.*
import akka.pattern.{ask, pipe}

import scala.concurrent.duration.*
import akka.util.Timeout
import kvstore.Replica.{GetResult, OperationFailed, TimePassed}

import java.util.concurrent.atomic.AtomicLong

object Replica:

  sealed trait Operation:
    def key: String

    def id: Long

  case class Insert(key: String, value: String, id: Long) extends Operation

  case class Remove(key: String, id: Long) extends Operation

  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case class TimePassed(key: String, id: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(Replica(arbiter, persistenceProps))

class Replica(val arbiter: ActorRef, persistenceProps: Props)extends Actor :

  import Replica.*
  import Replicator.*
  import Persistence.*
  import context.dispatcher

  val persistenceActor = context.actorOf(persistenceProps)

  var kv = Map.empty[String, String]
  var secondaries = Map.empty[ActorRef, ActorRef]
  var replicators = Set.empty[ActorRef]
  var persisted = Map.empty[String, Boolean]
  var updateGoing = Map.empty[String, Boolean]
  var pending = Map.empty[String, (ActorRef, Persist)]
  var acks = Map.empty[String, Set[ActorRef]]
  val exptextedSeq = new AtomicLong(0)

  override val supervisorStrategy = OneForOneStrategy() { case _: PersistenceException => SupervisorStrategy.Restart }

  locally {
    arbiter ! Join
    context.system.scheduler.scheduleWithFixedDelay(100.millisecond, 100.millisecond) {
      new Runnable {
        override def run(): Unit = pending
          .filter((key, _) => !persisted.getOrElse(key, true))
          .foreach((_, value) => persistenceActor ! value._2)
      }
    }
  }

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  val leader: Receive = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)

    case Insert(key, value, id) =>
      kv += (key -> value)
      persisted += key -> false
      updateGoing += key -> true
      persistenceActor ! Persist(key, Some(value), id)
      pending += key -> (sender, Persist(key, Some(value), id))
      acks += (key -> secondaries.values.toSet)
      secondaries.values.foreach {
        _ ! Replicate(key, Some(value), id)
      }
      context.system.scheduler.scheduleOnce(1.second) {
        self ! TimePassed(key, id)
      }

    case Remove(key, id) =>
      kv -= key
      persisted += (key -> false)
      updateGoing += (key -> true)
      persistenceActor ! Persist(key, None, id)
      pending += (key -> (sender, Persist(key, None, id)))
      acks += (key -> secondaries.values.toSet)
      secondaries.values.foreach {
        _ ! Replicate(key, None, id)
      }

      context.system.scheduler.scheduleOnce(1.second) {
        self ! TimePassed(key, id)
      }

    case TimePassed(key, id) => updateGoing.getOrElse(key, false) match {
      case true => pending(key)._1 ! OperationFailed(id)
      case false =>
    }

    case Persisted(key, id) =>
      persisted += (key -> true)
      if (acks(key).isEmpty) {
        updateGoing += key -> false
        pending(key)._1 ! OperationAck(id)
        pending -= key
      }

    case Replicated(key, id) if id != -1 =>
      acks += (key -> acks(key).excl(sender))
      if (acks(key).isEmpty && persisted(key)) {
        updateGoing += key -> false
        pending(key)._1 ! OperationAck(id)
        pending -= key
      }

    case Replicas(all) =>
      val replicas = all - self
      val oldReplicas = secondaries.keys.toSet -- replicas
      val oldReplicators = oldReplicas.map(el => secondaries(el))
      acks = for ((k, s) <- acks) yield k -> (s -- oldReplicators)
      oldReplicators.foreach { _ ! PoisonPill }
      acks.keys.foreach { key =>
        if (acks(key).isEmpty && updateGoing(key)) pending(key)._1 ! OperationAck(pending(key)._2.id)
      }
      val newReplicaReplicator = (for {
        r <- replicas -- secondaries.keys.toSet
      } yield r -> context.actorOf(Props(new Replicator(r)))).toMap
      secondaries --= oldReplicas
      secondaries ++= newReplicaReplicator
      newReplicaReplicator.values.foreach(x => kv.foreach(pair => x ! Replicate(pair._1, Some(pair._2), -1)))
  }

  val replica: Receive = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
    case Snapshot(key, valueOption, seq) =>
      if (seq < exptextedSeq.get()) sender ! SnapshotAck(key, seq)
      else if (exptextedSeq.get() == seq) {
        valueOption match {
          case Some(v) => kv += (key -> v)
          case None => kv -= key
        }
        exptextedSeq.incrementAndGet()
        persistenceActor ! Persist(key, valueOption, seq)
        persisted += key -> false
        pending += (key -> (sender, Persist(key, valueOption, seq)))
      }
    case Persisted(key, seq) =>
      persisted += key -> true
      pending(key)._1 ! SnapshotAck(key, seq)
      pending -= key
  }
