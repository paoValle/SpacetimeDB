//! Signature failures are durable decisions, not transport failures. Retries must
//! replay those decisions without consulting a potentially different reducer schema.
use spacetimedb::host::{FunctionArgs, IdcReducerCallError, IdcReducerCallOutcome};
use spacetimedb_lib::{hash_bytes, ConnectionId, Hash, Identity};
use spacetimedb_schema::def::reducer_signature_hash;
use spacetimedb_testing::modules::{CompilationMode, CompiledModule, DEFAULT_CONFIG};

#[test]
fn signature_errors_are_replayed_and_do_not_stall_the_stream() {
    CompiledModule::compile("module-test", CompilationMode::Debug).with_module_async(
        DEFAULT_CONFIG,
        |handle| async move {
            let host = handle.client.module();
            let connection = ConnectionId::from_u128(1);
            let outdated = hash_bytes(b"outdated signature");
            let call = |name: &'static str, seq, hash: Hash| {
                let host = host.clone();
                async move {
                    host.call_idc_reducer(
                        Identity::ZERO,
                        connection,
                        name,
                        FunctionArgs::Bsatn(vec![255].into()),
                        42,
                        seq,
                        0,
                        hash,
                    )
                    .await
                }
            };

            // Sequence admission takes precedence even over an invalid signature.
            assert!(matches!(
                call("add", 2, outdated).await,
                Err(IdcReducerCallError::OutOfOrder { expected: 1, actual: 2 })
            ));
            let IdcReducerCallOutcome::Applied(failed) = call("add", 1, outdated).await.unwrap() else {
                panic!("new message must record a terminal decision");
            };
            let error = failed.result.outcome.into_result().unwrap_err().to_string();
            assert!(error.contains("signature mismatch"), "{error}");
            let offset = failed.tx_offset.await.unwrap();

            // A changed route/schema and invalid arguments cannot replace a stored answer.
            let IdcReducerCallOutcome::Replayed(replayed) =
                call("removed_reducer", 1, hash_bytes(b"different signature"))
                    .await
                    .unwrap()
            else {
                panic!("decided message must replay before reducer lookup");
            };
            assert_eq!(&replayed.result_payload[..], error.as_bytes());
            assert!(
                replayed.tx_offset >= offset,
                "replay durability must cover the original decision"
            );

            let IdcReducerCallOutcome::Applied(missing) = call("removed_reducer", 2, outdated).await.unwrap() else {
                panic!("missing reducer must produce a terminal decision");
            };
            assert!(missing
                .result
                .outcome
                .into_result()
                .unwrap_err()
                .to_string()
                .contains("no such reducer"));

            let (_, reducer, owning) = host.info.module_def.reducer_by_name_with_module("add").unwrap();
            let hash = reducer_signature_hash(
                &reducer.params(),
                &reducer.ok_return_type,
                &reducer.err_return_type,
                owning.typespace(),
            );
            let IdcReducerCallOutcome::Applied(invalid_args) = call("add", 3, hash).await.unwrap() else {
                panic!("invalid arguments must produce a terminal decision");
            };
            assert!(invalid_args.result.is_err());

            let (_, reducer, owning) = host.info.module_def.reducer_by_name_with_module("say_hello").unwrap();
            let hash = reducer_signature_hash(
                &reducer.params(),
                &reducer.ok_return_type,
                &reducer.err_return_type,
                owning.typespace(),
            );
            let IdcReducerCallOutcome::Applied(valid) = host
                .call_idc_reducer(
                    Identity::ZERO,
                    connection,
                    "say_hello",
                    FunctionArgs::Nullary,
                    42,
                    4,
                    3,
                    hash,
                )
                .await
                .unwrap()
            else {
                panic!("valid message following terminal errors must run");
            };
            assert!(valid.result.outcome.into_result().is_ok());
            let offset = valid.tx_offset.await.unwrap();
            let IdcReducerCallOutcome::Replayed(replayed) = call("removed_reducer", 4, outdated).await.unwrap() else {
                panic!("successful decisions must also survive schema changes");
            };
            assert!(replayed.result_payload.is_empty());
            assert!(replayed.tx_offset >= offset);
            assert!(matches!(
                call("removed_reducer", 1, outdated).await.unwrap(),
                IdcReducerCallOutcome::AlreadyAcked
            ));
        },
    );
}
