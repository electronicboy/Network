# netty-transport-raknet

## Recovery observability

`RakChannelOption.RAK_METRICS` and `RakChannelOption.RAK_SERVER_METRICS`
provide compatibility-preserving callbacks for transport recovery internals.
In addition to packet, ACK/NACK, queue, and lifecycle counters, exporters can
observe:

- original, NACK-retransmitted, and timeout-retransmitted datagrams and bytes;
- one-based retransmission attempts and acknowledgement progress;
- congestion-controlled datagram bytes and retransmitted reliable datagrams in
  flight; tracked unreliable datagrams use payload-free samples;
- congestion window, slow-start threshold, smoothed RTT, RTT variation, and
  retransmission timeout; and
- recovery-epoch start/end timing and explicit per-channel state removal on
  close.

Sessions additionally report filtered delivery and pacing rates, minimum RTT,
recent packet-round loss, packet-timed round, startup, and persistent
no-progress state. These signals explain controller decisions; they are not
performance claims.

Periodic recovery-state callbacks are limited to 10 Hz per channel; recovery
transitions and terminal state are emitted immediately, while send and ACK
events are not sampled. Prometheus exporters should turn the send type into a
bounded label and aggregate channel state server-wide or into a fixed set of
cohorts. Do not use remote addresses, RakNet GUIDs, or other peer identifiers
as labels: they create unbounded time-series cardinality.

## Congestion control and recovery

RakNet sessions use one wire-compatible sender implementation: bounded NACK/PTO
recovery combined with a delivery-rate/minRTT model, BDP-derived in-flight
limit, bounded token pacer, path-step guardrails, and a persistent no-progress
reset. A NACK makes a reliable datagram immediately eligible for bounded fast
retransmit; there is no reordering timer that can hold a reliable-ordered game
stream. One loss is recorded as evidence but does not by itself reduce the
congestion window. The loss policy keeps path-independent hard-loss evidence
separate from path-scoped delay/loss evidence, applies a 0.70 HARD response or
a less severe 0.90 DELAY response at most once while an evidence epoch is held,
and uses response-specific rearming evidence. HARD needs two disjoint
actionable clear buckets; DELAY needs two consecutive actionable 256-packet
windows with no classified loss. PTO expiry is a progress probe rather than
congestion-loss proof; NACK evidence remains exact-once, while the
third backed-off no-progress probe invokes the persistent reset.
Rearming retains minRTT/path provenance and the filtered bandwidth seed while
restarting bounded bandwidth discovery; persistent no-progress instead starts
a fresh loss-evidence epoch.

The implementation borrows a limited set of ideas from
[IETF BBR draft-06](https://datatracker.ietf.org/doc/html/draft-ietf-ccwg-bbr-06),
[RFC 9002](https://www.rfc-editor.org/rfc/rfc9002.html), and
[RFC 8985](https://www.rfc-editor.org/rfc/rfc8985.html). It is not a BBR, QUIC,
or RACK implementation. Deterministic tests establish accounting and
state-machine invariants; external-qdisc campaigns remain required for
production capacity claims. See
[`adverse-network-research.md`](../benchmark/docs/adverse-network-research.md)
for the exact borrowed mechanisms, deliberate omissions, and validation plan.

Minecraft's relevant application traffic is predominantly reliable and
ordered, so that path is the performance focus. Unreliable datagrams still
share model accounting and pacing to prevent a congestion-control bypass, but
no new unreliable-message scheduling or deadline API is implied.

Model snapshots also expose cumulative hard-loss and delay-qualified
loss-response counts. These are monotonic for the lifetime of one session and
count actual congestion-window reductions, not raw loss reports. The extended
`rakCongestionModelState` overload default-delegates to the original callback,
so existing `RakChannelMetrics` and `RakServerMetrics` implementations continue
to receive snapshots. Exporters should sum the counters only across fixed
cohorts, never attach a remote address or peer identifier, and remove retained
session state when `rakRecoveryStateClosed` arrives.

## Established channel benchmarks

The benchmark kit for established RakNet channel bandwidth, latency, fanout, fairness, and impaired-network matrix runs lives in the sibling [`benchmark`](../benchmark) module.
