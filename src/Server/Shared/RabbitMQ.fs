module ServerCode.RabbitMQ

open System.Collections.Generic
open RabbitMQ.Client
open RabbitMQ.Client.Events
open Hopac
open Hopac.Infixes
open RabbitMQ.Client.Events
open System.Diagnostics.Tracing

type MessageId = System.Guid

/// This is the message created by the request to the API server.
type Message =
    /// This is the data you'd like to send.
  { data: byte[]
    /// Get from client request or generate based in deterministic hashing
    /// of inputs
    id: MessageId }

/// In this sample app, we either get a publisher confirm within N seconds,
/// and return Successful or otherwise tell the client to retry.
///
/// We don't expect the client to have to retry very often, because the queue
/// is already a highly available buffer; but unless we cater for this possibility
/// we may actually drop messages; it's better to be explicit about the protocol
/// that the API server talks with the backend broker, than to leave the
/// caller confused as to whether the call to the API succeeded.
type PublishResult =
  | Successful
  | FailureClientShouldRetry

module Impl =

  (*
    ## Paragraph 1 from https://www.rabbitmq.com/confirms.html — Acknowledgement Modes

    When the multiple field is set to true, RabbitMQ will acknowledge all outstanding
    delivery tags up to and including the tag specified in the acknowledgement.
    Like everything else related to acknowledgements, this is scoped per channel.
    For example, given that there are delivery tags 5, 6, 7, and 8 unacknowledged
    on channel Ch, when an acknowledgement frame arrives on that channel with
    delivery_tag set to 8 and multiple set to true, all tags from 5 to 8 will be
    acknowledged. If multiple was set to false, deliveries 5, 6, and 7 would still
    be unacknowledged.

    ## Paragraph 2 — Publisher Confirms and Guaranteed Delivery

    The broker loses persistent messages if it crashes before said messages are
    written to disk. Under certain conditions, this causes the broker to behave
    in surprising ways.

    For instance, consider this scenario:

      1. a client publishes a persistent message to a durable queue
      2. a client consumes the message from the queue (noting that the message is
         persistent and the queue durable), but doesn't yet ack it,
      3. the broker dies and is restarted, and
      4. the client reconnects and starts consuming messages.

    At this point, the client could reasonably assume that the message will be delivered again.
    This is not the case: the restart has caused the broker to lose the message.
    In order to guarantee persistence, a client should use confirms. If the
    publisher's channel had been in confirm mode, the publisher would not have
    received an ack for the lost message (since the message hadn't been written
    to disk yet).
  *)

  let logger = Suave.Logging.Log.create "RabbitMQ"

  type T =
    private {
      conn: IConnection
      model: IModel
      publish: Ch<Message[] * IVar<PublishResult>>
    }

  let createModel (c: IConnection) =
    let model = c.CreateModel()
    let acks, nacks, reconnected = Stream.Src.create (), Stream.Src.create (), Stream.Src.create ()
    // Once a channel is in confirm mode, both the broker and the client count messages (counting starts at 1 on the first confirm.select)
    model.ConfirmSelect()

    model.BasicAcks.Add(printfn "BasicAcks: %A")
    model.BasicAcks.Add(Stream.Src.value acks >> start)

    model.BasicNacks.Add(printfn "BasicNacks: %A")
    model.BasicNacks.Add(Stream.Src.value nacks >> start)

    // https://www.rabbitmq.com/blog/2011/02/10/introducing-publisher-confirms/ says:
    // Thirdly, if the connection between the publisher and broker drops with outstanding confirms, it does not necessarily mean that the messages were lost, so republishing may result in duplicate messages.
    model.BasicRecoverOk.Add(printfn "BasicRecoverOk: %A")
    model.BasicRecoverOk.Add(Stream.Src.value reconnected >> start)

    model.BasicReturn.Add(printfn "BasicReturn: %A")
    model.FlowControl.Add(printfn "FlowControl: %A")
    model.ModelShutdown.Add(printfn "ModelShutdown: %A")

    model,
    Stream.Src.tap acks,
    Stream.Src.tap nacks,
    Stream.Src.tap reconnected

  let props (model: IModel) (m: Message) =
    let p = model.CreateBasicProperties()
    p.CorrelationId <- m.id.ToString()
    p.Persistent <- true
    p

  let body (m: Message) =
    m.data

  type Inflight =
    { xs: Map<uint64, Message * IVar<PublishResult>> }
    static member empty = { xs = Map.empty }

  let removeAll deliveryTag inflight =
    let mutable removed = List.empty
    let mutable current = deliveryTag
    let mutable inflight' = inflight
    // treeSet.headSet(current).clear()
    // https://docs.oracle.com/javase/7/docs/api/java/util/TreeSet.html#headSet(E)
    // Remove all tags, less than
    // Count downwards until we reach the least element pending confirmation
    while inflight'.xs |> Map.containsKey current do
      let x = inflight'.xs |> Map.find current
      inflight' <- {inflight' with xs = inflight'.xs |> Map.remove current }
      current <- current - 1UL
      removed <- x :: removed

    inflight', removed

  let create (hosts: string seq) =
    // We need a connection factory to connect to RMQ
    let fac = new ConnectionFactory()

    // By providing a list of hosts to the connection factory, we can ensure the
    // the client reconnects to any broker that is up and running.
    fac.AutomaticRecoveryEnabled <- true
    let conn = fac.CreateConnection(List hosts)

    // This is the way that our app communicates with our message bus (this file)
    let publish = Ch ()

    // This is our way to ensure all callers get a reply at some point; even if it leads
    // to them resending the message
    let tooSlow = Ch ()

    // Let's create channels/streams for the things we care about, semantically...
    let model, acks, nacks, reconnected = createModel conn

    // We curry 'props' with the model state to make the code below nicer.
    let props = props model

    // Inflight invariants:

    // 1. `inflight` reply promise/value invariant: any write to the reply variable
    //    removes the delivery tag from the map, otherwise, multiple writes to the write-once
    //    variable may happen.
    // 2. Messages ACKed or NACKed from the broker do not have to exist in the map
    //    if they have been republished or timed out from the client.

    // This whole iteration loop is single threaded, by garantuee of Hopac
    let rec iter (inflight: Inflight) =
      Alt.choose [
        // https://www.rabbitmq.com/blog/2011/02/10/introducing-publisher-confirms/
        publish ^=> fun (messages, reply) ->
          let mutable tag = 1UL // "counting starts at 1" (from confirms.html)
          let mutable inflight' = inflight

          for message in messages do
            // The client library keep track of the next sequence number
            tag <- model.NextPublishSeqNo
            inflight' <- { inflight' with xs = inflight'.xs |> Map.add tag (message, reply) }
            model.BasicPublish("sample", "hellos", props message, body message)

            // When we use start, we shunt the work to the Hopac scheduler
            start (
              Alt.choose [
                // After 500 ms the client just doesn't care any more. Give your RMQ some more
                // CPU and Memory, why don't-ya?
                timeOutMillis 500 ^=> fun () -> tooSlow *<- tag
                // This will cancel the timeout
                reply ^->. ()
              ])

          iter inflight'

        reconnected ^=> fun _ ->
          // When the connection goes down and then up again, we can't be sure of whether
          // the inflight messages will be ACKed or not, so we have to republish them.
          // From the blog post introducing publisher confirms: "it does not necessarily
          // mean that the messages were lost" (but could mean they were)
          for KeyValue (_, (m, iv)) in inflight.xs do
            // NOTE: unbounded result set in memory; we shunt all pending messages into
            // the Hopac scheduler, to be redelivered into this loop
            queue (publish *<+ (Array.singleton m, iv))

          // our new publishes will re-queue with new delivery tags, so we clear our state
          iter Inflight.empty

        Stream.values acks ^=> fun (baea: BasicAckEventArgs) ->
          if baea.Multiple then
            let inflight', removed = removeAll baea.DeliveryTag inflight
            // PrepareJob ensures we've replied to all IVars before mapping all of those
            // replies to the iteration Alternative.
            Alt.prepareJob <| fun () ->
            // Ack all, ignore the units returned from sending Successful to the reply
            // Promise/IVar.
            removed |> List.map (fun (_, reply) -> reply *<= Successful) |> Job.conIgnore
            // and then iterate with the lesser set of messages pending
            >>-. iter inflight'
          else
            // We could replace this branch with the above Multiple=true branch.
            // This is the code you'd write when it's not a list that needs to be acked.
            match  inflight.xs |> Map.tryFind baea.DeliveryTag with
            | None ->
              // In this case we have nothing particular to do...
              iter inflight

            | Some (_, reply) ->
              // But in this case we should tell the caller that the message was successfully
              // sent.
              Alt.prepareJob <| fun () ->
              reply *<= Successful
              // iterate with the set of messages pending save for the one just delivered
              >>-. iter { inflight with xs = inflight.xs |> Map.remove baea.DeliveryTag }

        Stream.values nacks ^=> fun (bnea: BasicNackEventArgs) ->
          // "basic.nack will only be delivered if an internal error occurs
          // in the Erlang process responsible for a queue." from confirms.html
          if bnea.Multiple then
            let inflight', removed = removeAll bnea.DeliveryTag inflight
            for m, iv in removed do
              queue (publish *<+ (Array.singleton m, iv))
            iter inflight'
          else
            // We could replace this branch with the above Multiple=true branch.
            // This is the code you'd write when it's not a list that needs to be acked.
            match inflight.xs |> Map.tryFind bnea.DeliveryTag with
            | None ->
              iter inflight
            | Some (m, iv) ->
              queue (publish *<+ (Array.singleton m, iv))
              iter { inflight with xs = inflight.xs |> Map.remove bnea.DeliveryTag }

        // This channel receives delivery tags that are now overdue; we should return 503 Service Unavailable
        // to the caller
        tooSlow ^=> fun deliveryTag ->
          match inflight.xs |> Map.tryFind deliveryTag with
          | None ->
            // Race lost, the delivery tag has already been nacked or acked.
            iter inflight
          | Some (_, reply) ->
            // Race won, we should tell the client that.
            Alt.prepareJob <| fun () ->
            reply *<= FailureClientShouldRetry
            >>-. iter { inflight with xs = inflight.xs |> Map.remove deliveryTag }
      ]

    server (iter Inflight.empty)

    { conn = conn; model = model; publish = publish }

  let publish (x: T) (messages: Message seq): Alt<PublishResult> =
    // https://github.com/Hopac/Hopac/blob/master/Docs/Client-Server.md
    x.publish *<+=>- fun replyIv -> Array.ofSeq messages, replyIv

let create (hosts: string seq) =
  Impl.create hosts

let createSimple () =
  [
    "amqp://human:12345678@localhost:5681/"
    "amqp://human:12345678@localhost:5682/"
    "amqp://human:12345678@localhost:5683/"
  ]
  |> create

let publishAsync x messages: Async<PublishResult> =
  Alt.toAsync (Impl.publish x messages)