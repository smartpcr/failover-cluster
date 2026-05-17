//! [`SimulatedClock`] — virtual tick counter shared by the
//! [`SimulatedNetwork`](crate::network::SimulatedNetwork) and any test
//! that wants to assert on the simulated time elapsed during a
//! scenario.
//!
//! # What this type actually does
//!
//! Every per-RPC latency window applied by
//! [`SimulatedNetwork`](crate::network::SimulatedNetwork) goes through
//! [`SimulatedClock::delay`], which atomically advances an internal
//! tick counter denominated in microseconds and yields once to the
//! tokio runtime. Currently the call NO LONGER wall-clock sleeps,
//! so nonzero-latency simulated-network tests
//! are now scheduler-independent and run in pure virtual time. After a
//! scenario, the test can read [`SimulatedClock::now_micros`] /
//! [`SimulatedClock::elapsed`] to see exactly how much simulated
//! transit time the network charged across the entire run. This is the
//! structural wiring the Stage 8.1 brief asks for: drops, partition
//! changes, and per-RPC delivery delays all flow through one
//! observable clock.
//!
//! # Deterministic driver-tick advancement (Stage 8.1)
//!
//! The harness adds [`ManualTickSource`] — a
//! [`xraft_server::TickSource`] implementation handed to (and woken
//! by) the test [`SimulatedCluster`](crate::simulated::SimulatedCluster)
//! harness. This version replaced the initial `tokio::sync::Notify`-backed
//! wakeup with a per-listener
//! [`tokio::sync::mpsc::UnboundedSender`] so ticks BUFFER when the
//! driver is busy processing RPCs and are NEVER dropped.
//! `Notify::notify_waiters` only wakes listeners currently parked on
//! `notified()`; a driver that yields between a `tick().await` return
//! and the next `tick().await` call loses every notify that fires
//! during that gap. Per-listener
//! unbounded mpsc guarantees one-to-one delivery: every controller
//! `trigger()` enqueues one `()` into every registered listener's
//! channel; each listener drains them in order via `recv().await`.
//!
//! Each `trigger()` atomically advances this clock by the configured
//! tick quantum (so [`elapsed`](Self::elapsed) is a faithful record
//! of how many simulated ticks the driver consumed). A single
//! harness-owned background pump triggers at a caller-configurable
//! cadence by default. Tests that want fully-deterministic
//! step-by-step control disable the pump and drive ticks manually via
//! [`ManualTickController::trigger`] /
//! [`SimulatedCluster::tick_once`].
//!
//! Production code keeps the default
//! [`xraft_server::IntervalTickSource`] — nothing in this crate
//! changes the production driver's tick path.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use xraft_server::TickSource;

/// Virtual tick counter. Cheap to
/// clone via `Arc`.
///
/// Internal storage is microseconds since construction (or last
/// [`Self::reset`]). All accessor methods are `&self` so a single
/// `Arc<SimulatedClock>` can be shared between the test harness, the
/// [`SimulatedNetwork`](crate::network::SimulatedNetwork), and any
/// test code that wants to observe simulated time.
#[derive(Debug, Default)]
pub struct SimulatedClock {
    micros: AtomicU64,
}

impl SimulatedClock {
    /// Build a fresh clock at tick `0`.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Atomically advance the simulated clock by `d`'s microsecond
    /// count, then [`tokio::task::yield_now`] once so peer tasks
    /// (drivers, other dispatch arms) get a chance to interleave.
    /// Called by
    /// [`SimulatedNetwork`](crate::network::SimulatedNetwork)'s
    /// dispatch path on every RPC so the clock is the single observable
    /// record of how much transit time the simulated network charged.
    ///
    /// # Pure virtual delay
    ///
    /// An earlier version method did `tokio::time::sleep(d).await`, which
    /// coupled any nonzero-latency simulated-network scenario to the
    /// tokio scheduler — a 50 ms configured latency burned 50 ms of
    /// REAL wall-clock per RPC and made partition tests that issue
    /// thousands of RPCs intolerably slow AND scheduler-dependent.
    /// This version makes the call PURE VIRTUAL: simulated time still
    /// advances faithfully (so the harness's clock-elapsed observable
    /// is unchanged), but no wall-clock waiting happens. A single
    /// `yield_now()` is preserved so concurrent tasks still get
    /// interleaving — without it the dispatch arm would run to
    /// completion before any driver task got a slot, which would
    /// serialise the simulated cluster in a way the old `sleep` did
    /// not.
    ///
    /// `d == Duration::ZERO` is fast-pathed (no advance, no yield).
    pub async fn delay(&self, d: Duration) {
        if d.is_zero() {
            return;
        }
        let micros = d.as_micros().min(u64::MAX as u128) as u64;
        // Bump the counter atomically so concurrent observers see a
        // monotonic record of "simulated time charged to dispatch".
        self.micros.fetch_add(micros, Ordering::SeqCst);
        // Yield exactly once: lets concurrent driver / dispatch tasks
        // interleave without coupling to wall-clock — pure virtual time.
        tokio::task::yield_now().await;
    }

    /// Advance the simulated clock by `delta` microseconds without
    /// sleeping. Used by tests that want to inject a logical jump
    /// (e.g. "pretend 100 ms passed") without paying wall-clock cost.
    pub fn advance(&self, delta: Duration) -> u64 {
        let micros = delta.as_micros().min(u64::MAX as u128) as u64;
        self.micros.fetch_add(micros, Ordering::SeqCst) + micros
    }

    /// Current simulated time in microseconds since the last reset.
    pub fn now_micros(&self) -> u64 {
        self.micros.load(Ordering::SeqCst)
    }

    /// Current simulated time as a `Duration` since the last reset.
    pub fn elapsed(&self) -> Duration {
        Duration::from_micros(self.now_micros())
    }

    /// Reset the clock back to tick `0`. Useful between test phases.
    pub fn reset(&self) {
        self.micros.store(0, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------------
// ManualTickSource — externally-driven [`TickSource`] for simulated tests
// ---------------------------------------------------------------------------

/// Test-side controller that drives a fleet of attached
/// [`ManualTickSource`]s. Each [`Self::trigger`] fires exactly one
/// tick on every attached source AND atomically advances the
/// supplied [`SimulatedClock`] by `tick_quantum` BEFORE the tick is
/// delivered, so the clock IS the canonical record of how much
/// simulated time the driver consumed.
///
/// # Why per-listener mpsc, not a shared `Notify`
///
/// An earlier attempt used [`tokio::sync::Notify::notify_waiters`]
/// and lost any tick that fired while a driver was busy processing
/// RPCs between `tick().await` calls.
/// `notify_waiters` only wakes listeners CURRENTLY parked on
/// `notified()`; a driver that yields between a `tick().await`
/// return and the next `tick().await` call observes zero
/// notifications during the gap. The current version swaps in a per-listener
/// [`UnboundedSender`] / [`UnboundedReceiver`] pair — every
/// `trigger()` enqueues a unit `()` into every registered listener's
/// channel, and each listener drains them in order via
/// `recv().await`. Ticks BUFFER per-listener and are NEVER dropped.
///
/// Replaces the per-node `tokio::time::interval` in the simulated
/// harness with an externally-controlled fabric. The harness still
/// runs a background "wall-clock pump" task by default so existing
/// tests do not need to manually step ticks, but tests that want
/// fully deterministic step-by-step control can use
/// [`Self::trigger`] directly.
///
/// Construction: build a [`ManualTickController`] first, then call
/// [`Self::tick_source`] per driver to obtain a fresh
/// `Box<dyn TickSource>` registered against this controller.
#[derive(Clone, Debug)]
pub struct ManualTickController {
    inner: Arc<ManualTickInner>,
}

#[derive(Debug)]
struct ManualTickInner {
    listeners: Mutex<Vec<UnboundedSender<()>>>,
    clock: Arc<SimulatedClock>,
    tick_quantum: Duration,
}

impl ManualTickController {
    /// Build a controller that advances `clock` by `tick_quantum` on
    /// every triggered tick.
    pub fn new(clock: Arc<SimulatedClock>, tick_quantum: Duration) -> Self {
        Self {
            inner: Arc::new(ManualTickInner {
                listeners: Mutex::new(Vec::new()),
                clock,
                tick_quantum,
            }),
        }
    }

    /// Manually fire one tick to EVERY attached driver. The clock is
    /// advanced atomically by `tick_quantum` BEFORE the tick is
    /// enqueued on each listener so any listener that drains its
    /// channel immediately observes the post-tick time.
    ///
    /// Dropped listeners (whose receiver has been dropped) are
    /// pruned from the listener list inline; their channel returns
    /// an error from `send` and we filter them out.
    pub fn trigger(&self) {
        self.inner.clock.advance(self.inner.tick_quantum);
        let mut g = self
            .inner
            .listeners
            .lock()
            .expect("ManualTickController listener mutex poisoned");
        g.retain(|tx| tx.send(()).is_ok());
    }

    /// Build a fresh [`ManualTickSource`] registered against this
    /// controller. Each call returns a NEW source whose
    /// `tick().await` drains from its own dedicated unbounded
    /// channel; every [`Self::trigger`] enqueues one `()` per
    /// registered source, so ticks BUFFER per-listener and are
    /// never lost.
    pub fn tick_source(&self) -> Box<dyn TickSource> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.inner
            .listeners
            .lock()
            .expect("ManualTickController listener mutex poisoned")
            .push(tx);
        Box::new(ManualTickSource { rx })
    }

    /// Borrow the shared [`SimulatedClock`].
    pub fn clock(&self) -> Arc<SimulatedClock> {
        self.inner.clock.clone()
    }

    /// Borrow the per-tick quantum applied by [`Self::trigger`].
    pub fn tick_quantum(&self) -> Duration {
        self.inner.tick_quantum
    }

    /// Count the currently-attached listeners. Test-only diagnostic;
    /// production code never inspects this.
    pub fn listener_count(&self) -> usize {
        self.inner
            .listeners
            .lock()
            .expect("ManualTickController listener mutex poisoned")
            .len()
    }
}

/// Single-driver listener built from a [`ManualTickController`].
/// Each tick is delivered via an unbounded mpsc channel so the
/// listener never misses one even if it falls arbitrarily far behind
/// the controller.
pub struct ManualTickSource {
    rx: UnboundedReceiver<()>,
}

#[async_trait]
impl TickSource for ManualTickSource {
    async fn tick(&mut self) {
        // `recv()` returns `None` only after the controller drops
        // its corresponding sender (i.e. the controller is gone).
        // In that case the harness is shutting down — park forever
        // on a never-resolving future so the driver's outer
        // `tokio::select!` continues to drive shutdown via its
        // `shutdown_rx` arm. (Yielding here in a tight loop would
        // burn CPU and starve other arms.)
        if self.rx.recv().await.is_none() {
            std::future::pending::<()>().await;
        }
    }

    fn rebuild(&mut self, _new_interval: Duration) {
        // ManualTickSource is externally driven — interval changes
        // belong on the controller (e.g. by stopping the wall-clock
        // pump and re-spawning it at the new cadence). Silently
        // ignored here so SIGHUP-style reload paths do not panic in
        // tests that happen to install one.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_clock_starts_at_zero() {
        let c = SimulatedClock::new();
        assert_eq!(c.now_micros(), 0);
    }

    #[test]
    fn advance_returns_post_increment_value() {
        let c = SimulatedClock::new();
        assert_eq!(c.advance(Duration::from_micros(5)), 5);
        assert_eq!(c.advance(Duration::from_micros(3)), 8);
        assert_eq!(c.now_micros(), 8);
    }

    #[test]
    fn reset_returns_to_zero() {
        let c = SimulatedClock::new();
        c.advance(Duration::from_micros(42));
        c.reset();
        assert_eq!(c.now_micros(), 0);
    }

    // `delay` must advance the
    // simulated clock by the supplied duration AND must NOT wall-clock
    // sleep. The whole call should complete in well under a
    // millisecond even for a multi-second simulated `d` — that's the
    // determinism win the harness contract requires.
    #[tokio::test]
    async fn delay_advances_counter_without_wall_sleep() {
        let c = SimulatedClock::new();
        let before = std::time::Instant::now();
        // Ask for 5 SECONDS of simulated transit. An earlier version
        // would burn 5 s of real wall-clock; post-fix it should be
        // sub-millisecond.
        c.delay(Duration::from_secs(5)).await;
        let wall = before.elapsed();
        assert_eq!(
            c.now_micros(),
            5_000_000,
            "simulated clock must advance by exactly 5 s"
        );
        assert!(
            wall < Duration::from_millis(50),
            "delay must be virtual-only (no wall sleep); got wall={wall:?}"
        );
    }

    #[tokio::test]
    async fn zero_delay_is_no_op() {
        let c = SimulatedClock::new();
        c.delay(Duration::ZERO).await;
        assert_eq!(c.now_micros(), 0);
    }

    // ManualTickController.trigger
    // must advance the SimulatedClock AND deliver one tick to every
    // attached listener with PERFECT delivery — even when ticks
    // fire before the listener has called `tick().await`, AND when
    // multiple listeners drain at different rates. Two listeners;
    // one trigger; both wake (sequentially, NOT join!); clock
    // advances exactly once.
    //
    // the earlier implementation used `Notify::notify_waiters()`
    // which silently dropped this scenario — only currently-parked
    // listeners woke. The current version swapped to per-listener mpsc so the
    // tick BUFFERS in each listener's channel and is delivered on
    // the next `tick().await`. This test pins that contract.
    #[tokio::test]
    async fn manual_tick_controller_drives_all_listeners() {
        let clock = SimulatedClock::new();
        let ctrl = ManualTickController::new(clock.clone(), Duration::from_millis(5));
        let mut src_a = ctrl.tick_source();
        let mut src_b = ctrl.tick_source();

        // Fire BEFORE either listener calls `tick().await`. With the
        // per-listener mpsc design the tick must BUFFER for both
        // listeners, so the sequential `tick().await` calls below
        // both complete immediately.
        ctrl.trigger();

        tokio::time::timeout(Duration::from_secs(1), async {
            src_a.tick().await;
            src_b.tick().await;
        })
        .await
        .expect("buffered trigger must wake both sequential listeners");

        assert_eq!(
            clock.elapsed(),
            Duration::from_millis(5),
            "exactly one tick_quantum (5ms) must be applied per trigger"
        );
    }

    // Ticks must BUFFER for
    // listeners that are busy doing other work when `trigger()`
    // fires. This is the canonical scenario a `Notify`-based
    // implementation would break — a driver that returns from
    // `tick().await`, processes an inbound RPC, then loops back to
    // `tick().await` would miss every `notify_waiters()` fired
    // during the processing gap.
    //
    // We model that here by firing 5 triggers BEFORE the listener
    // calls `tick().await` even once. With per-listener mpsc, all
    // 5 buffer; 5 sequential `tick().await` calls drain them.
    #[tokio::test]
    async fn buffered_ticks_survive_busy_listener_gap() {
        let clock = SimulatedClock::new();
        let ctrl = ManualTickController::new(clock.clone(), Duration::from_millis(3));
        let mut src = ctrl.tick_source();

        // Fire 5 triggers up front; listener is NOT parked on any of them.
        for _ in 0..5 {
            ctrl.trigger();
        }

        // Now drain all 5 via sequential ticks — every one must
        // arrive without the controller firing any further trigger.
        for i in 0..5 {
            tokio::time::timeout(Duration::from_secs(1), src.tick())
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "buffered tick #{i} must be delivered without an \
                         additional trigger; mpsc backing dropped a queued tick"
                    )
                });
        }

        assert_eq!(
            clock.elapsed(),
            Duration::from_millis(15),
            "5 triggers must advance the clock by exactly 5 * quantum (3ms)"
        );

        // After the 5 buffered ticks drain, a 6th `tick().await`
        // must BLOCK — the channel is empty and no further trigger
        // has fired.
        let blocked = tokio::time::timeout(Duration::from_millis(50), src.tick()).await;
        assert!(
            blocked.is_err(),
            "tick() must block when no buffered triggers remain"
        );
    }

    // driver pumping through a
    // ManualTickSource produces deterministic clock advancement —
    // ten triggers + ten ticks = ten quanta on the clock, with no
    // intervening wall-clock dependency.
    #[tokio::test]
    async fn ten_manual_ticks_advance_clock_by_ten_quanta() {
        let clock = SimulatedClock::new();
        let quantum = Duration::from_millis(7);
        let ctrl = ManualTickController::new(clock.clone(), quantum);
        let mut src = ctrl.tick_source();

        // Spawn a tiny pump that does 10 triggers spaced 1ms apart.
        let pump = {
            let ctrl = ctrl.clone();
            tokio::spawn(async move {
                for _ in 0..10 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    ctrl.trigger();
                }
            })
        };

        for _ in 0..10 {
            tokio::time::timeout(Duration::from_secs(2), src.tick())
                .await
                .expect("each manual trigger must wake the source");
        }
        pump.await.unwrap();

        assert_eq!(
            clock.elapsed(),
            quantum * 10,
            "ten ticks must advance the clock by exactly 10 * quantum"
        );
    }

    // triggers fired while NO listener is registered
    // simply advance the clock and drop on the floor — there is no
    // listener to deliver them to. This is the expected behaviour
    // and is documented here so a future refactor cannot
    // accidentally "fix" it by buffering ticks on the controller
    // itself (which would cause every newly-registered listener to
    // drain a backlog of pre-registration ticks and double-fire the
    // election timer).
    #[tokio::test]
    async fn triggers_with_no_listeners_only_advance_clock() {
        let clock = SimulatedClock::new();
        let ctrl = ManualTickController::new(clock.clone(), Duration::from_millis(2));
        for _ in 0..3 {
            ctrl.trigger();
        }
        assert_eq!(clock.elapsed(), Duration::from_millis(6));
        // Newly-registered listener sees ZERO buffered ticks.
        let mut src = ctrl.tick_source();
        let blocked = tokio::time::timeout(Duration::from_millis(50), src.tick()).await;
        assert!(
            blocked.is_err(),
            "fresh listener must not inherit pre-registration ticks"
        );
    }
}
