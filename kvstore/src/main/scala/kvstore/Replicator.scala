package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import kvstore.Replicator.Replicated

import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.*

object Replicator:
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(Replicator(replica))

class Replicator(val replica: ActorRef) extends Actor:
  import Replicator.*
  import context.dispatcher

  var acks = Map.empty[Long, (ActorRef, Replicate)]
  var pending = Vector.empty[Snapshot]
  
  val seqCounter = new AtomicLong(0)
  
  def receive: Receive = {
    case Replicate(key, valueOption, id) =>
      val y = seqCounter.getAndIncrement()
      acks += (y -> (sender, Replicate(key, valueOption, id)))
      replica ! Snapshot(key, valueOption, y)
    case SnapshotAck(key, seq) =>
      acks.get(seq).map { (sender, request) =>
        acks -= seq
        sender ! Replicated(key, request.id)
      }
  }


