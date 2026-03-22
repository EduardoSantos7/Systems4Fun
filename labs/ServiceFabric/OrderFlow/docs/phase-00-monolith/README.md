### What you are learning

This is the starting point. Take a moment to appreciate how simple and clean it is. Everything is in one place. You can run it with `dotnet run` and test it immediately. There are no clusters, no manifests, no configuration files. This simplicity is real, and you should not forget it — because everything we add later has a cost.

### The pain points to notice right now

Read these carefully. Every single one will be directly addressed by a Service Fabric feature in a later phase.

**Pain Point 1 — State lives in process memory and is not durable**

If this process crashes (hardware failure, OS restart, deployment), every order in `_orders` and every inventory count in `_inventory` is gone. You lose data.

**Pain Point 2 — You cannot run two copies of this app**

Run two instances of this app behind a load balancer. Customer A reserves the last keyboard through instance 1. Customer B simultaneously checks stock through instance 2 — instance 2 says there are still 100 keyboards. They both complete orders. You just oversold inventory. The state is not shared.

**Pain Point 3 — Crashes mid-transaction leave inconsistent state**

If the process dies after decrementing inventory but before marking the order as fulfilled, you have permanently lost those units. No rollback. No compensation. The state is just wrong.

**Pain Point 4 — Scaling is coarse**

During a flash sale, the Inventory service is hammered. But your only option is to scale the entire application — OrderService, InventoryService, PaymentService, and all. You cannot scale just the part that is hot. And scaling adds more copies with diverging in-memory state (see Pain Point 2).

**Pain Point 5 — No reliable coordination between logical services**

OrderService calls InventoryService via a direct C# method call. This only works because they are in the same process. In any real deployment, they would need to be separate services — potentially written in different languages, deployed on different machines, scaled independently. You have no service discovery, no location transparency, no retry logic.

**Pain Point 6 — Deployment requires downtime**

To deploy a new version, you stop the process, deploy, restart. Any in-flight orders are lost. Any in-memory state is gone.

**Pain Point 7 — No health visibility**

You have no way to ask "is the inventory service healthy?" or "how many orders are in the pending state?" from the outside. No structured health model.

### Mini recap

You have built a working order processing system that is clean, simple, and completely unsuitable for any meaningful production scenario. Write these seven pain points down — they are the curriculum for the rest of this course.

### Checkpoint questions

1. Why does running two copies of this app cause inventory overselling?
2. What would need to be true for `PaymentService` to safely call `InventoryService` if they were separate processes on separate machines?
3. If the process crashes between inventory reservation and payment confirmation, what is the state of the system?
