use timely::dataflow::InputHandle;
use timely::dataflow::operators::{Input, Inspect, Map, Probe};

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {
        let index = worker.index();
        let mut input = InputHandle::new();

        // create a new input, exchange data, and inspect its output
        let probe = worker.dataflow::<i32, _, _>(|scope| {
            scope.input_from(&mut input)
                .map(|x| (x, x * x))
                .inspect(move |x| println!("worker {}:\thello {:?}", index, x))
                .probe()
        });

        // introduce data and watch!
        for round in 0..1000 {
            input.send(round);
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    }).unwrap();
}
