// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::pin::Pin;

use async_stream::stream;
use futures::{Stream, StreamExt};

use crate::error;
use crate::error::Result;
use crate::store::state_store::KeySet;

struct CollectingState {
    pairs: Vec<(String, Vec<u8>)>,
}

fn segment_index(base: &str, key: &str) -> Option<usize> {
    if key == base {
        Some(0)
    } else {
        key.strip_prefix(base)?.strip_prefix('/')?.parse().ok()
    }
}

fn merge_multiple_values(mut group: CollectingState) -> Result<(KeySet, Vec<u8>)> {
    let base = group.key().to_string();
    if group.pairs.len() == 1 && group.pairs[0].0 == base {
        let (key, value) = group.pairs.pop().unwrap();
        return Ok((KeySet::new(key, 0), value));
    }

    group
        .pairs
        .sort_unstable_by_key(|(key, _)| segment_index(&base, key));
    for (index, (key, _)) in group.pairs.iter().enumerate() {
        if segment_index(&base, key) != Some(index) || (index == 0 && key != &base) {
            let reason = format!("expected segment {index}, found {key}");
            return error::IncompleteRecordSnafu {
                key: base,
                reason,
                present_keys: group
                    .pairs
                    .into_iter()
                    .map(|(key, _)| key)
                    .collect::<Vec<_>>(),
            }
            .fail();
        }
    }

    let segments = group.pairs.len() - 1;
    let mut value = Vec::with_capacity(group.pairs.iter().map(|(_, bytes)| bytes.len()).sum());
    for (_, bytes) in group.pairs {
        value.extend_from_slice(&bytes);
    }
    Ok((KeySet::new(base, segments), value))
}

impl CollectingState {
    fn new(key: String, value: Vec<u8>) -> CollectingState {
        Self {
            pairs: vec![(key, value)],
        }
    }

    fn push(&mut self, key: String, value: Vec<u8>) {
        self.pairs.push((key, value));
    }

    fn key(&self) -> &str {
        let key = self.pairs[0].0.as_str();
        // A partial put can leave only children of a procedure record.
        if let Some((parent, _)) = key.rsplit_once('/')
            && let Some((_, suffix)) = parent.rsplit_once('.')
            && super::KeyType::from_str(suffix).is_some()
        {
            return parent;
        }
        key
    }
}

type Upstream = dyn Stream<Item = Result<(String, Vec<u8>)>> + Send;

/// Merges multiple values that have the same prefix of the key
/// from `upstream` into a single value.
pub fn multiple_value_stream(
    mut upstream: Pin<Box<Upstream>>,
) -> impl Stream<Item = Result<(KeySet, Vec<u8>)>> {
    stream! {
        let mut collecting: Option<CollectingState> = None;
        while let Some(item) = upstream.next().await {
            let (key, value) = match item {
                Ok(pair) => pair,
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };
            match collecting.take() {
                Some(mut current) => {
                    if key.strip_prefix(current.key()).is_some_and(|suffix| suffix.starts_with('/')) {
                        // Pushes the key value pair into `collecting`.
                        current.push(key, value);
                        collecting = Some(current);
                    } else {
                        // Starts to collect next key value pair.
                        collecting = Some(CollectingState::new(key, value));
                        yield merge_multiple_values(current);
                    }
                }
                None => collecting = Some(CollectingState::new(key, value)),
            }
        }
        if let Some(current) = collecting.take() {
            yield merge_multiple_values(current)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use futures::TryStreamExt;
    use futures::stream::{self};

    use super::*;
    use crate::error::{self};

    #[test]
    fn test_key_set_keys() {
        let key = KeySet::new("baz".to_string(), 3);
        let keys = key.keys();
        assert_eq!(keys.len(), 4);
        assert_eq!(&keys[0], "baz");
        assert_eq!(&keys[1], &KeySet::with_segment_suffix("baz", 1));
    }

    #[tokio::test]
    async fn test_merge_multiple_values() {
        let upstream = stream::iter(vec![
            Ok(("foo".to_string(), vec![0, 1, 2, 3])),
            Ok(("foo/0002".to_string(), vec![6, 7])),
            Ok(("foo/0003".to_string(), vec![8])),
            Ok(("foo/0001".to_string(), vec![4, 5])),
            Ok(("bar".to_string(), vec![0, 1, 2, 3])),
            Ok(("baz".to_string(), vec![0, 1, 2, 3])),
            Ok(("baz/0003".to_string(), vec![8])),
            Ok(("baz/0001".to_string(), vec![4, 5])),
            Ok(("baz/0002".to_string(), vec![6, 7])),
        ]);
        let mut stream = Box::pin(multiple_value_stream(Box::pin(upstream)));
        let (key, value) = stream.try_next().await.unwrap().unwrap();
        let keys = key.keys();
        assert_eq!(keys[0], "foo");
        assert_eq!(keys.len(), 4);
        assert_eq!(value, vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
        let (key, value) = stream.try_next().await.unwrap().unwrap();
        let keys = key.keys();
        assert_eq!(keys[0], "bar");
        assert_eq!(keys.len(), 1);
        assert_eq!(value, vec![0, 1, 2, 3]);
        let (key, value) = stream.try_next().await.unwrap().unwrap();
        let keys = key.keys();
        assert_eq!(keys[0], "baz");
        assert_eq!(keys.len(), 4);
        assert_eq!(value, vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
        assert!(stream.try_next().await.unwrap().is_none());
        // Call again
        assert!(stream.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_corrupt_group_does_not_end_scan() {
        let upstream = stream::iter(vec![
            Ok(("a.step".to_string(), vec![1])),
            Ok(("a.step/0000000002".to_string(), vec![3])),
            Ok(("b.step/0000000001".to_string(), vec![9])),
            Ok(("c.step".to_string(), vec![4])),
        ]);
        let mut stream = Box::pin(multiple_value_stream(Box::pin(upstream)));
        assert!(stream.try_next().await.is_err());
        assert!(stream.try_next().await.is_err());
        let (key, value) = stream.try_next().await.unwrap().unwrap();
        assert_eq!(key.key(), "c.step");
        assert_eq!(value, [4]);
    }

    #[tokio::test]
    async fn test_empty_upstream() {
        let upstream = stream::iter(vec![]);
        let mut stream = Box::pin(multiple_value_stream(Box::pin(upstream)));
        assert!(stream.try_next().await.unwrap().is_none());
        // Call again
        assert!(stream.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_multiple_values_stream_err() {
        let upstream = stream::iter(vec![
            Err(error::UnexpectedSnafu { err_msg: "mock" }.build()),
            Ok(("foo".to_string(), vec![0, 1, 2, 3])),
            Ok(("foo/0001".to_string(), vec![4, 5])),
        ]);
        let mut stream = Box::pin(multiple_value_stream(Box::pin(upstream)));
        let err = stream.try_next().await.unwrap_err();
        assert_matches!(err, error::Error::Unexpected { .. });

        let upstream = stream::iter(vec![
            Ok(("foo".to_string(), vec![0, 1, 2, 3])),
            Ok(("foo/0001".to_string(), vec![4, 5])),
            Err(error::UnexpectedSnafu { err_msg: "mock" }.build()),
        ]);
        let mut stream = Box::pin(multiple_value_stream(Box::pin(upstream)));
        let err = stream.try_next().await.unwrap_err();
        assert_matches!(err, error::Error::Unexpected { .. });
    }
}
