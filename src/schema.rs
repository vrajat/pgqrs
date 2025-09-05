// @generated automatically by Diesel CLI.

pub mod pgqrs {
    diesel::table! {
        pgqrs.meta (queue_name) {
            queue_name -> Varchar,
            created_at -> Timestamptz,
        }
    }
}
