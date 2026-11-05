//! The built-in `"crud"` api pack and the per-subtree guard wrappers —
//! the assembly's helper face, called from the `impl Bootstrap` matrix.

use axum::Router;
use std::sync::Arc;

use rushwind_storage::Viewer;
use rushwind_storage_axum::CrudApi;

use crate::config::{AuthzRef, StorageEndpointConfig, ViewerPolicy};
use crate::{
    with_authn, with_authorization, with_authorization_claim, with_authorization_for,
    BootstrapError, RouteInput,
};

pub(crate) fn random_instance_id() -> String {
    let mut bytes = [0u8; 16];
    let _ = getrandom::fill(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The built-in `"crud"` api pack: the storage line's HTTP edge
/// (`rushwind-storage-axum`'s [`CrudApi`]) bound to the configured
/// repository. Expects the schema's column fields as JSON in and out.
///
/// The edge runs the endpoint's `viewer` policy: `viewer: all` is the
/// explicit all-access opt-in (`CrudApi::with_viewer` answering every
/// request with [`Viewer::all`]); an absent policy keeps the axum
/// layer's deny default — every request sees nothing, and tenant-scoped
/// tables reject outright. The deny default replaced the old
/// all-access-for-everyone behavior (audit H2); a real deployment
/// overrides this pack with one that derives the viewer from the
/// request's authenticated claims.
pub(crate) fn crud_api_pack(
    endpoint: &StorageEndpointConfig,
    input: RouteInput,
) -> Result<Router, BootstrapError> {
    let repo = input.repository.ok_or_else(|| {
        BootstrapError::Failed("crud api pack requires a configured storage".to_string())
    })?;
    let mut api = CrudApi::new(repo);
    api = match endpoint.viewer {
        Some(ViewerPolicy::All) => api.with_viewer(Arc::new(|_headers| Viewer::all())),
        // Absent: the deny default. No policy is mounted, so every
        // request runs Viewer::default — fail closed.
        None => api,
    };
    Ok(api.router())
}

/// Applies the configured per-subtree security wraps to one router: the
/// authorization bridge first, the authentication bridge second — later
/// `layer` calls wrap earlier ones, so the authenticator ends up
/// outermost and the claims it inserts reach the permission point
/// beneath it. The permission point's project axis is fixed,
/// claim-carried, or empty — the two explicit shapes are mutually
/// exclusive.
pub(crate) fn apply_guards(
    router: Router,
    input: &RouteInput,
    authn: &Option<String>,
    authz: &Option<AuthzRef>,
) -> Result<Router, BootstrapError> {
    let router = apply_guards_authz(router, input, authz)?;
    let Some(authn_instance) = authn else {
        return Ok(router);
    };
    let authenticator =
        input
            .authn
            .get(authn_instance)
            .ok_or_else(|| BootstrapError::UnknownEngine {
                domain: "authn instance".to_string(),
                name: authn_instance.clone(),
            })?;
    Ok(with_authn(router, Arc::clone(authenticator)))
}

/// The authorization half of [`apply_guards`].
pub(crate) fn apply_guards_authz(
    router: Router,
    input: &RouteInput,
    authz: &Option<AuthzRef>,
) -> Result<Router, BootstrapError> {
    let Some(reference) = authz else {
        return Ok(router);
    };
    let engine =
        input
            .authz
            .get(&reference.engine)
            .ok_or_else(|| BootstrapError::UnknownEngine {
                domain: "authz instance".to_string(),
                name: reference.engine.clone(),
            })?;
    Ok(match (&reference.project, &reference.project_claim) {
        (Some(_), Some(_)) => {
            return Err(BootstrapError::Config(
                "authz reference: project and project_claim are mutually exclusive".to_string(),
            ));
        }
        (Some(project), None) => with_authorization_for(
            router,
            Arc::clone(engine),
            reference.action.clone(),
            reference.resource.clone(),
            project.clone(),
        ),
        (None, Some(claim)) => with_authorization_claim(
            router,
            Arc::clone(engine),
            reference.action.clone(),
            reference.resource.clone(),
            claim.clone(),
        ),
        (None, None) => with_authorization(
            router,
            Arc::clone(engine),
            reference.action.clone(),
            reference.resource.clone(),
        ),
    })
}
