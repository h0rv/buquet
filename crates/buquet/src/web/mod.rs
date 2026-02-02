//! Web UI module using axum, maud, and htmx.

mod handlers;
mod templates;

pub use handlers::create_router;
