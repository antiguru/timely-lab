use rand::{thread_rng, Rng};
use timely::dataflow::InputHandle;
#[allow(unused)]
use timely::dataflow::operators::{Input, Inspect, Probe, Operator};

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
            #[allow(unused)]
            let stream_2 = scope.input_from(&mut input_2)
                .inspect(move |x| println!("worker {}:\thello {:?}", index, x));

            stream_1
                // missing implementation
                .inspect(move |x| println!("worker {}:\tjoined {:?}", index, x))
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
