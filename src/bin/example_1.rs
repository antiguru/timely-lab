use rand::{thread_rng, Rng};
use timely::dataflow::InputHandle;
#[allow(unused)]
use timely::dataflow::operators::{Input, Inspect, Probe, Operator, Capability};

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
                // missing implementation
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
