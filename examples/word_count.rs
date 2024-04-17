use ferrum_refinery::framework::refinery::Refinery;

fn main() {
    let refinery = Refinery::new("deposit/input/path", "deposit/output/path");
    refinery.refine();
}
