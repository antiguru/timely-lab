use timely::dataflow::InputHandle;
use timely::dataflow::operators::{Input, Inspect, Probe};

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let mut input = InputHandle::new();

        let probe = worker.dataflow::<i32, _, _>(|scope| {
            scope.input_from(&mut input)
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
