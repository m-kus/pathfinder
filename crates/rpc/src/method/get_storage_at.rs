use anyhow::Context;
use pathfinder_common::{BlockNumber, ContractAddress, StorageAddress, StorageValue};

use crate::context::RpcContext;
use crate::dto::StorageResponseFlags;
use crate::types::BlockId;
use crate::RpcVersion;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Input {
    pub contract_address: ContractAddress,
    pub key: StorageAddress,
    pub block_id: BlockId,
    pub response_flags: StorageResponseFlags,
}

impl crate::dto::DeserializeForVersion for Input {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        let rpc_version = value.version;

        value.deserialize_map(|value| {
            let contract_address = value.deserialize("contract_address").map(ContractAddress)?;
            let key = value.deserialize("key").map(StorageAddress)?;
            let block_id = value.deserialize("block_id")?;
            let response_flags = if rpc_version >= RpcVersion::V10 {
                value
                    .deserialize_optional("response_flags")?
                    .unwrap_or_default()
            } else {
                StorageResponseFlags::default()
            };

            Ok(Self {
                contract_address,
                key,
                block_id,
                response_flags,
            })
        })
    }
}

#[derive(Debug)]
pub enum Output {
    Value(StorageValue),
    WithLastUpdateBlock {
        value: StorageValue,
        last_update_block: BlockNumber,
    },
}

crate::error::generate_rpc_error_subset!(Error: ContractNotFound, BlockNotFound);

/// Get the value of the storage at the given address and key.
pub async fn get_storage_at(
    context: RpcContext,
    input: Input,
    rpc_version: RpcVersion,
) -> Result<Output, Error> {
    let include_last_update_block = input
        .response_flags
        .0
        .iter()
        .any(|f| matches!(f, crate::dto::StorageResponseFlag::IncludeLastUpdateBlock));

    let span = tracing::Span::current();
    let jh = util::task::spawn_blocking(move |_| {
        let _g = span.enter();
        let mut db = context
            .storage
            .connection()
            .context("Opening database connection")?;

        let tx = db.transaction().context("Creating database transaction")?;

        if input.block_id.is_pending() {
            let pending_data = context
                .pending_data
                .get(&tx, rpc_version)
                .context("Querying pending data")?;

            let storage_value = pending_data.find_storage_value(input.contract_address, input.key);

            if let Some(value) = storage_value {
                return if include_last_update_block {
                    // Determine which block actually modified this key:
                    // check the pre-confirmed block's own state update first,
                    // then fall back to the pre-latest block number.
                    let last_update_block = if pending_data
                        .pending_state_update()
                        .storage_value(input.contract_address, input.key)
                        .is_some()
                    {
                        pending_data.pending_block_number()
                    } else {
                        pending_data
                            .pre_latest_block_number()
                            .unwrap_or(pending_data.pending_block_number())
                    };
                    Ok(Output::WithLastUpdateBlock {
                        value,
                        last_update_block,
                    })
                } else {
                    Ok(Output::Value(value))
                };
            }
        }

        let block_id = input
            .block_id
            .to_common_coerced(&tx)
            .map_err(|_| Error::BlockNotFound)?;
        if !tx.block_exists(block_id)? {
            return Err(Error::BlockNotFound);
        }

        let value = tx
            .storage_value_with_block_number(block_id, input.contract_address, input.key)
            .context("Querying storage value")?;

        let (value, block_number) = match value {
            Some(v) => v,
            None => {
                if tx.contract_exists(input.contract_address, block_id)? {
                    (StorageValue::ZERO, BlockNumber::GENESIS)
                } else {
                    return Err(Error::ContractNotFound);
                }
            }
        };

        if include_last_update_block {
            Ok(Output::WithLastUpdateBlock {
                value,
                last_update_block: block_number,
            })
        } else {
            Ok(Output::Value(value))
        }
    });

    jh.await.context("Database read panic or shutting down")?
}

impl crate::dto::SerializeForVersion for Output {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        match self {
            Output::Value(value) => serializer.serialize(value),
            Output::WithLastUpdateBlock {
                value,
                last_update_block,
            } => {
                let mut s = serializer.serialize_struct()?;
                s.serialize_field("value", value)?;
                s.serialize_field("last_update_block", last_update_block)?;
                s.end()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::BlockNumber;
    use serde_json::json;

    use super::*;
    use crate::dto::{DeserializeForVersion, SerializeForVersion, StorageResponseFlag};
    use crate::RpcVersion;

    fn default_flags() -> StorageResponseFlags {
        StorageResponseFlags::default()
    }

    fn with_last_update_block_flag() -> StorageResponseFlags {
        StorageResponseFlags(vec![StorageResponseFlag::IncludeLastUpdateBlock])
    }

    fn input(contract_address: ContractAddress, key: StorageAddress, block_id: BlockId) -> Input {
        Input {
            contract_address,
            key,
            block_id,
            response_flags: default_flags(),
        }
    }

    fn assert_value(output: &Output, expected: StorageValue) {
        match output {
            Output::Value(v) => assert_eq!(*v, expected),
            other => panic!("expected Output::Value, got {other:?}"),
        }
    }

    /// # Important
    ///
    /// `BlockId` parsing is tested in
    /// [`get_block`][crate::rpc::method::get_block::tests::parsing]
    /// and is not repeated here.
    #[rstest::rstest]
    #[case::positional(json!(["1", "2", "latest"]))]
    #[case::named(json!({"contract_address": "0x1", "key": "0x2", "block_id": "latest"}))]
    fn parsing(#[case] input: serde_json::Value) {
        let expected = Input {
            contract_address: contract_address!("0x1"),
            key: storage_address!("0x2"),
            block_id: BlockId::Latest,
            response_flags: default_flags(),
        };

        let input = Input::deserialize(crate::dto::Value::new(input, RpcVersion::V07)).unwrap();

        assert_eq!(input, expected);
    }

    #[test]
    fn parsing_with_response_flags() {
        let json_input = json!({
            "contract_address": "0x1",
            "key": "0x2",
            "block_id": "latest",
            "response_flags": ["INCLUDE_LAST_UPDATE_BLOCK"]
        });

        let input =
            Input::deserialize(crate::dto::Value::new(json_input, RpcVersion::V10)).unwrap();

        assert_eq!(input.contract_address, contract_address!("0x1"));
        assert_eq!(input.response_flags.0.len(), 1);
        assert_eq!(
            input.response_flags.0[0],
            StorageResponseFlag::IncludeLastUpdateBlock
        );
    }

    #[test]
    fn parsing_without_response_flags_v10() {
        let json_input = json!({
            "contract_address": "0x1",
            "key": "0x2",
            "block_id": "latest"
        });

        let input =
            Input::deserialize(crate::dto::Value::new(json_input, RpcVersion::V10)).unwrap();

        assert!(input.response_flags.0.is_empty());
    }

    const RPC_VERSION: RpcVersion = RpcVersion::V09;

    #[tokio::test]
    async fn pending() {
        let ctx = RpcContext::for_tests_with_pending().await;
        let result = get_storage_at(
            ctx,
            input(
                contract_address_bytes!(b"pending contract 1 address"),
                storage_address_bytes!(b"pending storage key 0"),
                BlockId::Pending,
            ),
            RPC_VERSION,
        )
        .await
        .unwrap();

        assert_value(&result, storage_value_bytes!(b"pending storage value 0"));
    }

    #[tokio::test]
    async fn pre_confirmed() {
        let ctx = RpcContext::for_tests_with_pre_confirmed().await;

        let inp = Input {
            contract_address: contract_address_bytes!(b"preconfirmed contract 1 address"),
            key: storage_address_bytes!(b"preconfirmed storage key 0"),
            block_id: BlockId::Pending,
            response_flags: default_flags(),
        };

        let result = get_storage_at(ctx.clone(), inp.clone(), RpcVersion::V09)
            .await
            .unwrap();
        assert_value(
            &result,
            storage_value_bytes!(b"preconfirmed storage value 0"),
        );

        // JSON-RPC version before 0.9 are expected to ignore the pre-confirmed block.
        let err = get_storage_at(ctx, inp, RpcVersion::V08).await.unwrap_err();
        assert_matches!(err, Error::ContractNotFound);
    }

    #[tokio::test]
    async fn pre_latest() {
        let ctx = RpcContext::for_tests_with_pre_latest_and_pre_confirmed().await;

        let inp = Input {
            contract_address: contract_address_bytes!(b"prelatest contract 1 address"),
            key: storage_address_bytes!(b"prelatest storage key 0"),
            block_id: BlockId::Pending,
            response_flags: default_flags(),
        };

        let result = get_storage_at(ctx.clone(), inp.clone(), RpcVersion::V09)
            .await
            .unwrap();
        assert_value(&result, storage_value_bytes!(b"prelatest storage value 0"));

        // JSON-RPC version before 0.9 are expected to ignore the pre-latest block.
        let err = get_storage_at(ctx, inp, RpcVersion::V08).await.unwrap_err();
        assert_matches!(err, Error::ContractNotFound);
    }

    #[tokio::test]
    async fn pending_falls_back_to_latest() {
        let ctx = RpcContext::for_tests_with_pending().await;
        let result = get_storage_at(
            ctx,
            input(
                contract_address_bytes!(b"contract 1"),
                storage_address_bytes!(b"storage addr 0"),
                BlockId::Pending,
            ),
            RPC_VERSION,
        )
        .await
        .unwrap();

        assert_value(&result, storage_value_bytes!(b"storage value 2"));
    }

    #[tokio::test]
    async fn pending_deployed_defaults_to_zero() {
        let ctx = RpcContext::for_tests_with_pending().await;
        let result = get_storage_at(
            ctx,
            input(
                contract_address_bytes!(b"pending contract 0 address"),
                storage_address_bytes!(b"non-existent"),
                BlockId::Pending,
            ),
            RPC_VERSION,
        )
        .await
        .unwrap();

        assert_value(&result, StorageValue::ZERO);
    }

    #[tokio::test]
    async fn latest() {
        let ctx = RpcContext::for_tests_with_pending().await;
        let result = get_storage_at(
            ctx,
            input(
                contract_address_bytes!(b"contract 1"),
                storage_address_bytes!(b"storage addr 0"),
                BlockId::Latest,
            ),
            RPC_VERSION,
        )
        .await
        .unwrap();

        assert_value(&result, storage_value_bytes!(b"storage value 2"));
    }

    #[tokio::test]
    async fn l1_accepted() {
        let ctx = RpcContext::for_tests_with_pending().await;
        let result = get_storage_at(
            ctx,
            input(
                contract_address_bytes!(b"contract 1"),
                storage_address_bytes!(b"storage addr 0"),
                BlockId::L1Accepted,
            ),
            RPC_VERSION,
        )
        .await
        .unwrap();

        assert_value(&result, storage_value_bytes!(b"storage value 1"));
    }

    #[tokio::test]
    async fn defaults_to_zero() {
        let ctx = RpcContext::for_tests_with_pending().await;
        let result = get_storage_at(
            ctx,
            input(
                contract_address_bytes!(b"contract 1"),
                storage_address_bytes!(b"non-existent"),
                BlockId::Latest,
            ),
            RPC_VERSION,
        )
        .await
        .unwrap();

        assert_value(&result, StorageValue::ZERO);
    }

    #[tokio::test]
    async fn by_hash() {
        let ctx = RpcContext::for_tests_with_pending().await;
        let result = get_storage_at(
            ctx,
            input(
                contract_address_bytes!(b"contract 1"),
                storage_address_bytes!(b"storage addr 0"),
                BlockId::Hash(block_hash_bytes!(b"block 1")),
            ),
            RPC_VERSION,
        )
        .await
        .unwrap();

        assert_value(&result, storage_value_bytes!(b"storage value 1"));
    }

    #[tokio::test]
    async fn by_number() {
        let ctx = RpcContext::for_tests_with_pending().await;
        let result = get_storage_at(
            ctx,
            input(
                contract_address_bytes!(b"contract 1"),
                storage_address_bytes!(b"storage addr 0"),
                BlockId::Number(BlockNumber::GENESIS + 1),
            ),
            RPC_VERSION,
        )
        .await
        .unwrap();

        assert_value(&result, storage_value_bytes!(b"storage value 1"));
    }

    #[tokio::test]
    async fn unknown_contract() {
        let ctx = RpcContext::for_tests_with_pending().await;
        let result = get_storage_at(
            ctx,
            input(
                contract_address_bytes!(b"non-existent"),
                storage_address_bytes!(b"storage addr 0"),
                BlockId::Latest,
            ),
            RPC_VERSION,
        )
        .await;

        assert_matches!(result, Err(Error::ContractNotFound));
    }

    #[tokio::test]
    async fn contract_is_unknown_before_deployment() {
        let ctx = RpcContext::for_tests_with_pending().await;
        let result = get_storage_at(
            ctx,
            input(
                contract_address_bytes!(b"contract 1"),
                storage_address_bytes!(b"storage addr 0"),
                BlockId::Hash(block_hash_bytes!(b"genesis")),
            ),
            RPC_VERSION,
        )
        .await;

        assert_matches!(result, Err(Error::ContractNotFound));
    }

    #[tokio::test]
    async fn block_not_found_by_number() {
        let ctx = RpcContext::for_tests_with_pending().await;
        let result = get_storage_at(
            ctx,
            input(
                contract_address_bytes!(b"contract 1"),
                storage_address_bytes!(b"storage addr 0"),
                BlockId::Number(BlockNumber::MAX),
            ),
            RPC_VERSION,
        )
        .await;

        assert_matches!(result, Err(Error::BlockNotFound));
    }

    #[tokio::test]
    async fn block_not_found_by_hash() {
        let ctx = RpcContext::for_tests_with_pending().await;
        let result = get_storage_at(
            ctx,
            input(
                contract_address_bytes!(b"contract 1"),
                storage_address_bytes!(b"storage addr 0"),
                BlockId::Hash(block_hash_bytes!(b"unknown")),
            ),
            RPC_VERSION,
        )
        .await;

        assert_matches!(result, Err(Error::BlockNotFound));
    }

    #[tokio::test]
    async fn with_include_last_update_block_pending() {
        let ctx = RpcContext::for_tests_with_pending().await;
        let result = get_storage_at(
            ctx,
            Input {
                contract_address: contract_address_bytes!(b"pending contract 1 address"),
                key: storage_address_bytes!(b"pending storage key 0"),
                block_id: BlockId::Pending,
                response_flags: with_last_update_block_flag(),
            },
            RPC_VERSION,
        )
        .await
        .unwrap();

        match result {
            Output::WithLastUpdateBlock {
                value,
                last_update_block,
            } => {
                assert_eq!(value, storage_value_bytes!(b"pending storage value 0"));
                // Pending block number is one past the latest stored block (block 2).
                assert_eq!(last_update_block, BlockNumber::GENESIS + 3);
            }
            other => panic!("expected Output::WithLastUpdateBlock, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn with_include_last_update_block_pending_falls_back() {
        let ctx = RpcContext::for_tests_with_pending().await;
        // Key exists on-chain but not in pending — should fall back to DB query.
        let result = get_storage_at(
            ctx,
            Input {
                contract_address: contract_address_bytes!(b"contract 1"),
                key: storage_address_bytes!(b"storage addr 0"),
                block_id: BlockId::Pending,
                response_flags: with_last_update_block_flag(),
            },
            RPC_VERSION,
        )
        .await
        .unwrap();

        match result {
            Output::WithLastUpdateBlock {
                value,
                last_update_block,
            } => {
                assert_eq!(value, storage_value_bytes!(b"storage value 2"));
                assert_eq!(last_update_block, BlockNumber::GENESIS + 2);
            }
            other => panic!("expected Output::WithLastUpdateBlock, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn with_include_last_update_block_flag() {
        let ctx = RpcContext::for_tests_with_pending().await;
        let result = get_storage_at(
            ctx,
            Input {
                contract_address: contract_address_bytes!(b"contract 1"),
                key: storage_address_bytes!(b"storage addr 0"),
                block_id: BlockId::Latest,
                response_flags: with_last_update_block_flag(),
            },
            RPC_VERSION,
        )
        .await
        .unwrap();

        match result {
            Output::WithLastUpdateBlock {
                value,
                last_update_block,
            } => {
                assert_eq!(value, storage_value_bytes!(b"storage value 2"));
                assert_eq!(last_update_block, BlockNumber::GENESIS + 2);
            }
            other => panic!("expected Output::WithLastUpdateBlock, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn with_include_last_update_block_flag_by_number() {
        let ctx = RpcContext::for_tests_with_pending().await;
        let result = get_storage_at(
            ctx,
            Input {
                contract_address: contract_address_bytes!(b"contract 1"),
                key: storage_address_bytes!(b"storage addr 0"),
                block_id: BlockId::Number(BlockNumber::GENESIS + 1),
                response_flags: with_last_update_block_flag(),
            },
            RPC_VERSION,
        )
        .await
        .unwrap();

        match result {
            Output::WithLastUpdateBlock {
                value,
                last_update_block,
            } => {
                assert_eq!(value, storage_value_bytes!(b"storage value 1"));
                assert_eq!(last_update_block, BlockNumber::GENESIS + 1);
            }
            other => panic!("expected Output::WithLastUpdateBlock, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn with_include_last_update_block_defaults_to_zero() {
        let ctx = RpcContext::for_tests_with_pending().await;
        let result = get_storage_at(
            ctx,
            Input {
                contract_address: contract_address_bytes!(b"contract 1"),
                key: storage_address_bytes!(b"non-existent"),
                block_id: BlockId::Latest,
                response_flags: with_last_update_block_flag(),
            },
            RPC_VERSION,
        )
        .await
        .unwrap();

        match result {
            Output::WithLastUpdateBlock {
                value,
                last_update_block,
            } => {
                assert_eq!(value, StorageValue::ZERO);
                assert_eq!(last_update_block, BlockNumber::GENESIS);
            }
            other => panic!("expected Output::WithLastUpdateBlock, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn with_flag_value_in_pre_confirmed_diff() {
        // Value modified in the pre-confirmed block's own state update.
        // last_update_block should be the pre-confirmed block number.
        let ctx = RpcContext::for_tests_with_pre_latest_and_pre_confirmed().await;
        let result = get_storage_at(
            ctx,
            Input {
                contract_address: contract_address_bytes!(b"preconfirmed contract 1 address"),
                key: storage_address_bytes!(b"preconfirmed storage key 0"),
                block_id: BlockId::Pending,
                response_flags: with_last_update_block_flag(),
            },
            RpcVersion::V10,
        )
        .await
        .unwrap();

        match result {
            Output::WithLastUpdateBlock {
                value,
                last_update_block,
            } => {
                assert_eq!(value, storage_value_bytes!(b"preconfirmed storage value 0"));
                // Pre-confirmed is latest + 2 when pre-latest is present.
                assert_eq!(last_update_block, BlockNumber::GENESIS + 4);
            }
            other => panic!("expected Output::WithLastUpdateBlock, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn with_flag_value_in_pre_latest_diff() {
        // Value modified in the pre-latest block (not in pre-confirmed's own diff).
        // last_update_block should be the pre-latest block number.
        let ctx = RpcContext::for_tests_with_pre_latest_and_pre_confirmed().await;
        let result = get_storage_at(
            ctx,
            Input {
                contract_address: contract_address_bytes!(b"prelatest contract 1 address"),
                key: storage_address_bytes!(b"prelatest storage key 0"),
                block_id: BlockId::Pending,
                response_flags: with_last_update_block_flag(),
            },
            RpcVersion::V10,
        )
        .await
        .unwrap();

        match result {
            Output::WithLastUpdateBlock {
                value,
                last_update_block,
            } => {
                assert_eq!(value, storage_value_bytes!(b"prelatest storage value 0"));
                // Pre-latest is latest + 1.
                assert_eq!(last_update_block, BlockNumber::GENESIS + 3);
            }
            other => panic!("expected Output::WithLastUpdateBlock, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn with_flag_pre_confirmed_falls_back_to_db() {
        // Value not in any pending diff — falls through to DB query.
        let ctx = RpcContext::for_tests_with_pre_latest_and_pre_confirmed().await;
        let result = get_storage_at(
            ctx,
            Input {
                contract_address: contract_address_bytes!(b"contract 1"),
                key: storage_address_bytes!(b"storage addr 0"),
                block_id: BlockId::Pending,
                response_flags: with_last_update_block_flag(),
            },
            RpcVersion::V10,
        )
        .await
        .unwrap();

        match result {
            Output::WithLastUpdateBlock {
                value,
                last_update_block,
            } => {
                assert_eq!(value, storage_value_bytes!(b"storage value 2"));
                // Last modified at block 2 in the DB.
                assert_eq!(last_update_block, BlockNumber::GENESIS + 2);
            }
            other => panic!("expected Output::WithLastUpdateBlock, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn with_flag_pre_confirmed_unknown_contract() {
        let ctx = RpcContext::for_tests_with_pre_latest_and_pre_confirmed().await;
        let result = get_storage_at(
            ctx,
            Input {
                contract_address: contract_address_bytes!(b"non-existent"),
                key: storage_address_bytes!(b"storage addr 0"),
                block_id: BlockId::Pending,
                response_flags: with_last_update_block_flag(),
            },
            RpcVersion::V10,
        )
        .await;

        assert_matches!(result, Err(Error::ContractNotFound));
    }

    #[test]
    fn serialize_plain_value() {
        let output = Output::Value(storage_value!("0x1234"));
        let serialized = output
            .serialize(crate::dto::Serializer {
                version: RpcVersion::V10,
            })
            .unwrap();
        assert_eq!(serialized, json!("0x1234"));
    }

    #[test]
    fn serialize_with_last_update_block() {
        let output = Output::WithLastUpdateBlock {
            value: storage_value!("0x1234"),
            last_update_block: BlockNumber::new_or_panic(42),
        };
        let serialized = output
            .serialize(crate::dto::Serializer {
                version: RpcVersion::V10,
            })
            .unwrap();
        assert_eq!(
            serialized,
            json!({"value": "0x1234", "last_update_block": 42})
        );
    }
}
