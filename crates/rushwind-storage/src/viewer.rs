//! Viewer-scoped tenancy — the tenant axis plus five data ranges.
//!
//! A [`Viewer`] rides in every [`QueryCtx`](crate::QueryCtx) and bounds what
//! the repository is allowed to see on two orthogonal axes: which *tenant's*
//! rows are visible ([`Tenancy`]) and, within that, which *rows* the actor
//! owns ([`DataRange`]). Engines must enforce both on every path, and a row
//! outside the scope is reported exactly like a missing row.

use crate::filter::{FilterExpr, Op};
use crate::value::Value;

/// The tenant axis — which tenant's rows the viewer acts on.
///
/// Mirrors the reference port's `EnforceTenant` three-state decision:
/// a table declared tenant-scoped (a [`Schema`](crate::Schema) carrying a
/// tenant column) resolves every call against this axis.
///
/// - [`Tenancy::Unidentified`] **fails closed**: the default, and the
///   shape of a viewer nobody attached an identity to. A tenant-scoped
///   call from an unidentified viewer errors rather than silently losing
///   the tenant predicate — the missing-predicate bug this axis exists to
///   prevent.
/// - [`Tenancy::Platform`] passes without any predicate: platform
///   operators and system background jobs (scheduled sweeps, inter-service
///   maintenance) that legitimately see across tenants.
/// - [`Tenancy::Tenant`] is the tenant business view: every path is
///   conjoined with the tenant predicate and creates are force-stamped.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Tenancy {
    /// No tenancy identity attached. On a tenant-scoped table this is a
    /// hard error ([`StorageError::MissingViewer`](crate::StorageError::MissingViewer)),
    /// never a silent pass — fail closed.
    #[default]
    Unidentified,
    /// Platform operator or system background job: no tenant predicate is
    /// injected anywhere, and creates may address tenants explicitly.
    Platform,
    /// A tenant business view bound to one tenant id: predicates conjoin
    /// with it, creates are force-stamped with it.
    Tenant(i64),
}

/// How much of the table the viewer may see.
///
/// The five ranges: `ALL` (everything), `UNIT` (the viewer's
/// organizational unit), `USER` (an explicit list of owner ids), `OWN`
/// (only rows the viewer owns), and `NONE` (deny
/// everything). The default is `NONE` — scopes close by default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum DataRange {
    /// The whole table (`ALL`).
    All,
    /// Rows belonging to the viewer's organizational unit (`UNIT`).
    Unit,
    /// Rows owned by an explicit list of users (`USER`).
    User,
    /// Rows owned by the viewer (`SELF`).
    Own,
    /// Nothing (`NONE`); every query yields an empty result.
    #[default]
    None,
}

/// The outcome of resolving a [`Viewer`] against concrete scope columns.
#[derive(Clone, Debug, PartialEq)]
pub enum Scope {
    /// The viewer may not see any row: engines short-circuit to empty
    /// results without touching the store.
    Deny,
    /// No extra predicate is required.
    Unrestricted,
    /// Every read must be conjoined with this filter.
    Scoped(FilterExpr),
}

/// The tenant-axis verdict [`Viewer::tenant_decision`] resolves for one
/// table: whether the engine must conjoin the tenant predicate and
/// force-stamp it on creates, or the table rides without one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TenancyDecision {
    /// The table declares no tenant column — no predicate anywhere.
    Unscoped,
    /// Platform or system viewer on a tenant-scoped table: no predicate is
    /// injected, and creates keep whatever tenant value the row carries.
    Pass,
    /// Tenant business viewer: every path conjoins the tenant predicate,
    /// and creates are force-stamped with this id (a tenant actor cannot
    /// address another tenant by writing the column itself).
    Enforce(i64),
}

/// The actor on whose behalf a query runs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Viewer {
    /// The tenant axis the actor acts on.
    pub tenancy: Tenancy,
    /// The data range the actor may see.
    pub range: DataRange,
    /// The actor's own id (the `OWN` range needs it).
    pub actor_id: Option<i64>,
    /// The actor's organizational unit id (the `UNIT` range needs it).
    pub unit_id: Option<i64>,
    /// The explicit owner ids of the `USER` range.
    pub subjects: Vec<i64>,
}

impl Viewer {
    /// The conventional owner column (`SELF`/`USER` scoping), shared by all
    /// engines so the same schema works across them.
    pub const OWNER_COLUMN: &'static str = "owner_id";

    /// The conventional organizational-unit column (`UNIT` scoping).
    pub const UNIT_COLUMN: &'static str = "unit_id";

    /// The conventional tenant column — the column name a
    /// [`Schema`](crate::Schema) declares to opt a table into tenant
    /// enforcement.
    pub const TENANT_COLUMN: &'static str = "tenant_id";

    /// A viewer that sees everything — tests, bootstrap jobs, migrations.
    /// The tenant axis rides as [`Tenancy::Platform`]: seeing everything
    /// includes every tenant.
    pub fn all() -> Self {
        Self {
            tenancy: Tenancy::Platform,
            range: DataRange::All,
            ..Self::default()
        }
    }

    /// The system background identity — scheduled sweeps, inter-service
    /// maintenance, archival jobs. The port of the reference's standard
    /// system context: platform tenancy, whole-table range, no per-row
    /// ownership. Distinguishable from [`Viewer::all`] only by intent; both
    /// pass tenant enforcement.
    pub fn system() -> Self {
        Self::all()
    }

    /// A tenant business viewer seeing the whole tenant — the tenant
    /// administrator shape. Conjoin narrower ranges through the public
    /// fields when the actor's reach is smaller than the tenant's.
    pub fn tenant(tenant_id: i64) -> Self {
        Self {
            tenancy: Tenancy::Tenant(tenant_id),
            range: DataRange::All,
            ..Self::default()
        }
    }

    /// A tenant business viewer restricted to its own rows — the standard
    /// user identity of the reference's user context: one tenant, own rows.
    pub fn tenant_user(tenant_id: i64, actor_id: i64) -> Self {
        Self {
            tenancy: Tenancy::Tenant(tenant_id),
            range: DataRange::Own,
            actor_id: Some(actor_id),
            ..Self::default()
        }
    }

    /// A viewer restricted to its own rows.
    pub fn own(actor_id: i64) -> Self {
        Self {
            range: DataRange::Own,
            actor_id: Some(actor_id),
            ..Self::default()
        }
    }

    /// A viewer restricted to one organizational unit.
    pub fn unit(unit_id: i64) -> Self {
        Self {
            range: DataRange::Unit,
            unit_id: Some(unit_id),
            ..Self::default()
        }
    }

    /// The tenant enforcement decision for one table, mirroring the
    /// reference port's `EnforceTenant`: the tenant column absent
    /// ([`TenancyDecision::Unscoped`]) or a platform/system viewer
    /// ([`TenancyDecision::Pass`]) needs no predicate; a tenant business
    /// viewer carries the id to conjoin ([`TenancyDecision::Enforce`]); an
    /// unidentified viewer on a tenant-scoped table fails closed with
    /// [`StorageError::MissingViewer`](crate::StorageError::MissingViewer).
    pub fn tenant_decision(
        &self,
        tenant_column: Option<&str>,
    ) -> Result<TenancyDecision, crate::StorageError> {
        tenant_column.map_or(Ok(TenancyDecision::Unscoped), |_| match self.tenancy {
            Tenancy::Unidentified => Err(crate::StorageError::MissingViewer),
            Tenancy::Platform => Ok(TenancyDecision::Pass),
            Tenancy::Tenant(id) => Ok(TenancyDecision::Enforce(id)),
        })
    }

    /// Resolves the viewer against the owner and unit column names of the
    /// table being queried. The returned [`Scope`] tells the engine what to
    /// conjoin (or whether to short-circuit).
    pub fn scope(&self, owner_col: &str, unit_col: &str) -> Scope {
        match self.range {
            DataRange::All => Scope::Unrestricted,
            DataRange::None => Scope::Deny,
            DataRange::Own => match self.actor_id {
                Some(id) => Scope::Scoped(FilterExpr::cond(owner_col, Op::Eq, [Value::Int(id)])),
                None => Scope::Deny,
            },
            DataRange::Unit => match self.unit_id {
                Some(id) => Scope::Scoped(FilterExpr::cond(unit_col, Op::Eq, [Value::Int(id)])),
                None => Scope::Deny,
            },
            DataRange::User => {
                if self.subjects.is_empty() {
                    Scope::Deny
                } else {
                    Scope::Scoped(FilterExpr::cond(
                        owner_col,
                        Op::In,
                        self.subjects.iter().copied().map(Value::Int),
                    ))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn own_scope_targets_the_actor() {
        let scope = Viewer::own(7).scope("owner_id", "unit_id");
        match scope {
            Scope::Scoped(expr) => {
                assert_eq!(
                    expr.node().to_owned(),
                    crate::filter::FilterNode::Cond(crate::filter::Condition::new(
                        "owner_id",
                        Op::Eq,
                        [Value::Int(7)]
                    ))
                );
            }
            other => panic!("expected a scoped filter, got {other:#?}"),
        }
    }

    #[test]
    fn own_without_actor_id_denies() {
        let viewer = Viewer {
            range: DataRange::Own,
            ..Viewer::default()
        };
        assert_eq!(viewer.scope("owner_id", "unit_id"), Scope::Deny);
    }

    #[test]
    fn unit_scope_targets_the_unit_column() {
        let scope = Viewer::unit(3).scope("owner_id", "unit_id");
        match scope {
            Scope::Scoped(_) => {}
            other => panic!("expected a scoped filter, got {other:#?}"),
        }
    }

    #[test]
    fn user_range_with_empty_subjects_denies() {
        let viewer = Viewer {
            range: DataRange::User,
            subjects: Vec::new(),
            ..Viewer::default()
        };
        assert_eq!(viewer.scope("owner_id", "unit_id"), Scope::Deny);
    }

    #[test]
    fn tenant_axis_skips_unscoped_tables() {
        let viewer = Viewer::tenant_user(5, 7);
        assert_eq!(
            viewer.tenant_decision(None),
            Ok(TenancyDecision::Unscoped),
            "no tenant column: the axis never fires"
        );
    }

    #[test]
    fn tenant_viewer_enforces_its_id() {
        assert_eq!(
            Viewer::tenant_user(5, 7).tenant_decision(Some(Viewer::TENANT_COLUMN)),
            Ok(TenancyDecision::Enforce(5))
        );
    }

    #[test]
    fn platform_and_system_viewers_pass_tenant_enforcement() {
        for viewer in [Viewer::all(), Viewer::system()] {
            assert_eq!(
                viewer.tenant_decision(Some(Viewer::TENANT_COLUMN)),
                Ok(TenancyDecision::Pass)
            );
        }
    }

    #[test]
    fn unidentified_viewer_fails_closed_on_tenant_tables() {
        let viewer = Viewer::own(7);
        assert_eq!(
            viewer.tenant_decision(Some(Viewer::TENANT_COLUMN)),
            Err(crate::StorageError::MissingViewer),
            "a viewer nobody attached tenancy to must error, never pass silently"
        );
    }

    #[test]
    fn default_viewer_fails_closed_on_tenant_tables() {
        let viewer = Viewer::default();
        assert_eq!(
            viewer.tenant_decision(Some(Viewer::TENANT_COLUMN)),
            Err(crate::StorageError::MissingViewer)
        );
        assert_eq!(viewer.scope("owner_id", "unit_id"), Scope::Deny);
    }
}
