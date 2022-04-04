use std::collections::HashMap;

pub enum PrivacyPolicy {
    /*
     * Simple static privacy policy for a given camera/video
     *   `epsilon`-DP for `multiplier` events,
     *    where each event is made up of <= `k` non-contiguous segments that have a total duration of <= `rho`
     *
     * For example if we measure the max persistence to be 60 seconds, and individuals appear at most
     * twice (on their way to and from work), and we want to protect them over a week (14x), we'd set:
     * k=2*7, rho=60*7
     */
    Static {
        k_segments: u64,
        epsilon: f64,
        rho_ms: u64,
    },
    // TODO
    Mask {},
}

// PrivacyPolicy for each camera, identified by a unique string
pub type PolicyMap = HashMap<String, PrivacyPolicy>;

// TODO
//struct PrivacyBudget {}
//
//struct Video {
//    budget: PrivacyBudget,
//}
