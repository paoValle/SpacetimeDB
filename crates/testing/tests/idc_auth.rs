use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use spacetimedb::auth::idc;
use spacetimedb::db::{Config, Storage};
use spacetimedb_client_api::routes::database::DatabaseRoutes;
use spacetimedb_client_api::NodeDelegate;
use spacetimedb_lib::sats::product;
use spacetimedb_lib::{bsatn, Identity};
use spacetimedb_schema::def::reducer_signature_hash;
use spacetimedb_testing::modules::{CompilationMode, CompiledModule};
use tower::ServiceExt;

#[test]
#[serial_test::serial]
fn idc_receiver_runs_connection_lifecycle_without_jwt() {
    CompiledModule::compile("idc-auth-test", CompilationMode::Debug).with_module_async(
        Config {
            storage: Storage::Memory,
            page_pool_max_size: None,
        },
        |module| async move {
            let host = module.client.module();
            let (_, reducer, owning) = host.info.module_def.reducer_by_name_with_module("receive").unwrap();
            let signature_hash = reducer_signature_hash(
                &reducer.params(),
                &reducer.ok_return_type,
                &reducer.err_return_type,
                owning.typespace(),
            );
            let app = DatabaseRoutes::default()
                .into_router(module.env.clone())
                .with_state(module.env.clone());
            let clients_before = host.relational_db().connected_clients().unwrap();

            // Rejection in client_connected must prevent delivery. Accepted requests
            // must disconnect after success, replay, or failure, without exposing a JWT.
            for (sender, seq, fail, hash, status, counts, error) in [
                (
                    Identity::ZERO,
                    1,
                    false,
                    signature_hash,
                    StatusCode::FORBIDDEN,
                    (0, 0, 0),
                    "untrusted database",
                ),
                (Identity::ONE, 1, false, signature_hash, StatusCode::OK, (1, 1, 1), ""),
                (Identity::ONE, 1, false, signature_hash, StatusCode::OK, (2, 1, 2), ""),
                (
                    Identity::ONE,
                    2,
                    true,
                    signature_hash,
                    StatusCode::UNPROCESSABLE_ENTITY,
                    (3, 1, 3),
                    "delivery rejected",
                ),
                (
                    Identity::ONE,
                    3,
                    false,
                    spacetimedb_lib::Hash::from_byte_array([0; 32]),
                    StatusCode::UNPROCESSABLE_ENTITY,
                    (4, 1, 4),
                    "",
                ),
            ] {
                let token = idc::sign(module.env.jwt_auth_provider(), sender, module.db_identity).unwrap();
                let request = Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/{}/call-from-database/receive?stream_id=1&seq={seq}&signature_hash={}",
                        module.db_identity.to_hex(),
                        hash.to_hex(),
                    ))
                    .header("Authorization", format!("Bearer {token}"))
                    .header("Content-Type", "application/octet-stream")
                    .body(Body::from(bsatn::to_vec(&product![fail]).unwrap()))
                    .unwrap();
                let response = app.clone().oneshot(request).await.unwrap();
                let actual_status = response.status();
                let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
                let body = String::from_utf8_lossy(&body);
                assert_eq!(actual_status, status, "{body}");
                assert!(body.contains(error), "{body}");

                // Lifecycle counters verify callbacks actually ran; comparing sessions
                // also catches leaked st_client rows after rejected or failed requests.
                let (connected, received, disconnected): (u64, u64, u64) = counts;
                // Use a normal HTTP call to verify ordinary clients still receive a JWT.
                let request = Request::builder()
                    .method("POST")
                    .uri(format!("/{}/call/assert_counts", module.db_identity.to_hex()))
                    .header("Content-Type", "application/json")
                    .body(Body::from(format!("[{connected},{received},{disconnected}]")))
                    .unwrap();
                let response = app.clone().oneshot(request).await.unwrap();
                let status = response.status();
                let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
                assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
                assert_eq!(host.relational_db().connected_clients().unwrap(), clients_before);
            }
        },
    );
}
