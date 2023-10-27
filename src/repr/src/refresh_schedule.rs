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
            .map(|refresh_every| timestamp.round_up(refresh_every))
            .min();
        let next_at = self
            .ats
            .iter()
            .filter(|at| **at >= timestamp)
            .min()
            .cloned();

        // //////////
        // let xx = next_every.into_iter().chain(next_at).min();
        // println!("###### round_up_timestamp(self: {:?}, timestamp: {}) -> {:?}", self, timestamp, xx);

        // Note: `min(next_every, next_at)` wouldn't do what we want, because None is smaller than
        // any Some, but we'd need None to be bigger than any Some.
        next_every.into_iter().chain(next_at).min()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RefreshEvery {
    pub interval: Interval,
    pub starting_at: Timestamp,
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
            starting_at: Some(self.starting_at.into_proto()),
        }
    }

    fn from_proto(proto: ProtoRefreshEvery) -> Result<Self, TryFromProtoError> {
        Ok(RefreshEvery {
            interval: proto
                .interval
                .into_rust_if_some("ProtoRefreshEvery::interval")?,
            starting_at: proto
                .starting_at
                .into_rust_if_some("ProtoRefreshEvery::starting_at")?,
        })
    }
}
