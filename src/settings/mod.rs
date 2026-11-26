//! Product settings: constants, one module per area, each named for the
//! setting as [5.4.1](../../docs/5.4.1-configuration.md#product-settings)
//! names it. A deployment setting is not here; it is read into
//! [`Config`](crate::config::Config).

pub mod collection;
pub mod profiles;
pub mod scheduling;
pub mod storage;
pub mod transform;
