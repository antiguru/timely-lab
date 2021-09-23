use rand::{thread_rng, Rng};
use timely::dataflow::InputHandle;
use timely::dataflow::operators::{Input, Inspect, Probe, Operator};
use timely::dataflow::channels::pact::Exchange;
use std::collections::HashMap;

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {
        let mut input_1 = InputHandle::<_, (u64, u64)>::new();
        let mut input_2 = InputHandle::<_, (u64, u64)>::new();

        // create a new input, exchange data, and inspect its output
        let probe = worker.dataflow::<i32, _, _>(|scope| {
            let index = scope.index();
            let stream_1 = scope.input_from(&mut input_1)
                .inspect(move |x| println!("worker {}:\thello {:?}", index, x));
            let stream_2 = scope.input_from(&mut input_2)
                .inspect(move |x| println!("worker {}:\thello {:?}", index, x));

            let mut in1_buffer = Vec::new();
            let mut in2_buffer = Vec::new();

            let mut in1_stash = HashMap::<i32, Vec<_>>::new();
            let mut in2_stash = HashMap::<i32, Vec<_>>::new();
            stream_1
                .binary_notify(&stream_2, Exchange::new(|(x, _)| *x), Exchange::new(|(x, _)| *x), "join", None, move |in1, in2, output, not| {
                    while let Some((time, data)) = in1.next() {
                        data.swap(&mut in1_buffer);
                        in1_stash.entry(time.time().clone()).or_default().extend(in1_buffer.drain(..));
                        not.notify_at(time.retain());
                    }
                    while let Some((time, data)) = in2.next() {
                        data.swap(&mut in2_buffer);
                        in2_stash.entry(*time.time()).or_default().extend(in2_buffer.drain(..));
                        not.notify_at(time.retain());
                    }
                    while let Some((time, _cnt)) = not.next() {
                        let mut session = output.session(&time);
                        let in1_data: HashMap<_, _> = in1_stash.remove(time.time()).unwrap_or_default().into_iter().collect();
                        let mut in2_data: HashMap<_, _> = in2_stash.remove(time.time()).unwrap_or_default().into_iter().collect();
                        for (k, v1) in in1_data.into_iter() {
                            if let Some(v2) = in2_data.remove(&k) {
                                session.give((k, v1, v2));
                            }
                        }
                    }
                })
                .inspect(move |x: &(u64, u64, u64)| println!("worker {}:\tjoined {:?}", index, x))
                .probe()
        });

        let mut rng = thread_rng();
        // introduce data and watch!
        for round in 0i32..100 {
            for _ in 0..10 {
                input_1.send((rng.gen_range(0..10), rng.gen_range(100..200)));
                input_2.send((rng.gen_range(0..10), rng.gen_range(300..400)));
            }
            input_1.advance_to(round + 1);
            input_2.advance_to(round + 1);
            while probe.less_than(input_1.time()) {
                worker.step();
            }
        }
    }).unwrap();
}
