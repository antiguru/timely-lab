use rand::{thread_rng, Rng};
use timely::dataflow::InputHandle;
use timely::dataflow::operators::{Input, Inspect, Probe, Operator, Capability};
use timely::dataflow::channels::pact::Exchange;
use std::collections::BinaryHeap;
use std::cmp::Reverse;

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {
        let index = worker.index();
        let mut input = InputHandle::new();

        // create a new input, exchange data, and inspect its output
        let probe = worker.dataflow::<i32, _, _>(|scope| {
            let stream = scope.input_from(&mut input)
                .inspect(move |x| println!("worker {}:\thello {:?}", index, x));
            stream
                .unary_frontier(Exchange::new(|x: &(i32, u64)| x.1), "re-order", |_cap, _info| {
                    let mut heap = BinaryHeap::new();
                    let mut buffer = Vec::new();
                    let mut cap: Option<Capability<i32>> = None;
                    move |input, output| {
                        if let Some(cap) = cap.as_mut() {
                            while heap.peek().map_or(false, |t_d: &Reverse<(i32, _)>| !input.frontier.less_equal(&t_d.0.0)) {
                                let (_, d) = heap.pop().unwrap().0;
                                output.session(cap).give(d);
                                if let Some(Reverse((t, _))) = heap.peek() {
                                    if cap.time() < t {
                                        *cap = cap.delayed(t)
                                    }
                                }
                            }
                        }
                        if heap.is_empty() {
                            cap.take();
                        }
                        while let Some((time, data)) = input.next() {
                            data.swap(&mut buffer);
                            for pair in buffer.drain(..) {
                                heap.push(Reverse(pair));
                            }
                            if let Some(Reverse((t, _))) = heap.peek() {
                                if cap.as_ref().map_or(true, |cap| cap.time() > &t) {
                                    cap = Some(time.delayed(&t));
                                }
                            }
                        }
                    }
                })
                .inspect_batch(move |t, x| println!("worker {}:\tre-order {} -> {:?}", index, t, x))
                .probe()
        });

        let mut rng = thread_rng();
        // introduce data and watch!
        for round in 0..1000 {
            for _ in 0..1 {
                input.send((round + rng.gen_range(0..100), rng.gen_range(1000..2000) as u64));
            }
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    }).unwrap();
}
