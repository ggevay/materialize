// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::adt::interval::Interval;
use crate::Timestamp;
use mz_proto::IntoRustIfSome;
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use serde::{Deserialize, Serialize};

include!(concat!(env!("OUT_DIR"), "/mz_repr.refresh_schedule.rs"));

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RefreshSchedule {
    // `REFRESH EVERY`s
    pub everies: Vec<RefreshEvery>,
    // `REFRESH AT`s
    pub ats: Vec<Timestamp>,
}

impl RefreshSchedule {
    pub fn empty() -> RefreshSchedule {
        RefreshSchedule {
            everies: Vec::new(),
            ats: Vec::new(),
        }
    }

    /// Rounds up the timestamp to the time of the next refresh.
    /// Returns None if there is no next refresh.
    pub fn round_up_timestamp(&self, timestamp: Timestamp) -> Option<Timestamp> {
        let next_every = self
            .everies
            .iter()
            // We use a `filter_map` here to simply discard such refreshes where the timestamp
            // overflowed, since the system would never reach that time anyway.
            .filter_map(|refresh_every| timestamp.round_up(refresh_every))
            .min();
        let next_at = self
            .ats
            .iter()
            .filter(|at| **at >= timestamp)
            .min()
            .cloned();
        // Take the min of `next_every` and `next_at`, but with considering None to be bigger than
        // any Some. Note: Simply `min(next_every, next_at)` wouldn't do what we want, because None
        // is smaller than any Some.
        next_every.into_iter().chain(next_at).min()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RefreshEvery {
    pub interval: Interval,
    pub aligned_to: Timestamp,
}

impl RustType<ProtoRefreshSchedule> for RefreshSchedule {
    fn into_proto(&self) -> ProtoRefreshSchedule {
        ProtoRefreshSchedule {
            everies: self.everies.into_proto(),
            ats: self.ats.into_proto(),
        }
    }

    fn from_proto(proto: ProtoRefreshSchedule) -> Result<Self, TryFromProtoError> {
        Ok(RefreshSchedule {
            everies: proto.everies.into_rust()?,
            ats: proto.ats.into_rust()?,
        })
    }
}

impl RustType<ProtoRefreshEvery> for RefreshEvery {
    fn into_proto(&self) -> ProtoRefreshEvery {
        ProtoRefreshEvery {
            interval: Some(self.interval.into_proto()),
            aligned_to: Some(self.aligned_to.into_proto()),
        }
    }

    fn from_proto(proto: ProtoRefreshEvery) -> Result<Self, TryFromProtoError> {
        Ok(RefreshEvery {
            interval: proto
                .interval
                .into_rust_if_some("ProtoRefreshEvery::interval")?,
            aligned_to: proto
                .aligned_to
                .into_rust_if_some("ProtoRefreshEvery::aligned_to")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::adt::interval::Interval;
    use crate::refresh_schedule::{RefreshEvery, RefreshSchedule};
    use crate::Timestamp;
    use std::str::FromStr;

    #[mz_ore::test]
    fn test_round_up_timestamp() {
        let ts = |t: u64| Timestamp::new(t);
        let test = |schedule: RefreshSchedule| {
            move |expected_ts: Option<u64>, input_ts| {
                assert_eq!(
                    expected_ts.map(ts),
                    schedule.round_up_timestamp(ts(input_ts))
                )
            }
        };
        {
            let schedule = RefreshSchedule {
                everies: vec![],
                ats: vec![ts(123), ts(456)],
            };
            let test = test(schedule);
            test(Some(123), 0);
            test(Some(123), 50);
            test(Some(123), 122);
            test(Some(123), 123);
            test(Some(456), 124);
            test(Some(456), 130);
            test(Some(456), 455);
            test(Some(456), 456);
            test(None, 457);
            test(None, 12345678);
            test(None, u64::MAX - 1000);
            test(None, u64::MAX - 1);
            test(None, u64::MAX);
        }
        {
            let schedule = RefreshSchedule {
                everies: vec![RefreshEvery {
                    interval: Interval::from_str("100 milliseconds").unwrap(),
                    aligned_to: ts(500),
                }],
                ats: vec![],
            };
            let test = test(schedule);
            test(Some(0), 0);
            test(Some(100), 1);
            test(Some(100), 2);
            test(Some(100), 99);
            test(Some(100), 100);
            test(Some(200), 101);
            test(Some(200), 102);
            test(Some(200), 199);
            test(Some(200), 200);
            test(Some(300), 201);
            test(Some(400), 400);
            test(Some(500), 401);
            test(Some(500), 450);
            test(Some(500), 499);
            test(Some(500), 500);
            test(Some(600), 501);
            test(Some(600), 599);
            test(Some(600), 600);
            test(Some(700), 601);
            test(Some(5434532600), 5434532599);
            test(Some(5434532600), 5434532600);
            test(Some(5434532700), 5434532601);
            test(None, u64::MAX);
        }
        {
            let schedule = RefreshSchedule {
                everies: vec![RefreshEvery {
                    interval: Interval::from_str("100 milliseconds").unwrap(),
                    aligned_to: ts(542),
                }],
                ats: vec![],
            };
            let test = test(schedule);
            test(Some(42), 0);
            test(Some(42), 1);
            test(Some(42), 41);
            test(Some(42), 42);
            test(Some(142), 43);
            test(Some(442), 441);
            test(Some(442), 442);
            test(Some(542), 443);
            test(Some(542), 541);
            test(Some(542), 542);
            test(Some(642), 543);
            test(None, u64::MAX);
        }
        {
            let schedule = RefreshSchedule {
                everies: vec![
                    RefreshEvery {
                        interval: Interval::from_str("100 milliseconds").unwrap(),
                        aligned_to: ts(400),
                    },
                    RefreshEvery {
                        interval: Interval::from_str("100 milliseconds").unwrap(),
                        aligned_to: ts(542),
                    },
                ],
                ats: vec![ts(2), ts(300), ts(400), ts(471), ts(541), ts(123456)],
            };
            let test = test(schedule);
            test(Some(0), 0);
            test(Some(2), 1);
            test(Some(2), 2);
            test(Some(42), 3);
            test(Some(42), 41);
            test(Some(42), 42);
            test(Some(100), 43);
            test(Some(100), 99);
            test(Some(100), 100);
            test(Some(142), 101);
            test(Some(142), 141);
            test(Some(142), 142);
            test(Some(200), 143);
            test(Some(300), 243);
            test(Some(300), 299);
            test(Some(300), 300);
            test(Some(342), 301);
            test(Some(400), 343);
            test(Some(400), 399);
            test(Some(400), 400);
            test(Some(442), 401);
            test(Some(442), 441);
            test(Some(442), 442);
            test(Some(471), 443);
            test(Some(471), 470);
            test(Some(471), 471);
            test(Some(500), 472);
            test(Some(500), 472);
            test(Some(500), 500);
            test(Some(541), 501);
            test(Some(541), 540);
            test(Some(541), 541);
            test(Some(542), 542);
            test(Some(600), 543);
            test(Some(65500), 65454);
            test(Some(87842), 87831);
            test(Some(123442), 123442);
            test(Some(123456), 123443);
            test(Some(123456), 123456);
            test(Some(123500), 123457);
            test(None, u64::MAX);
        }
    }
}
