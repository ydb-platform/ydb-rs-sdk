use crate::db;
use rocket::http::{ContentType, Status};
use rocket::request::{FromRequest, Outcome};
use rocket::response::Redirect;
use rocket::{form::Form, get, post, routes, uri, Build, FromForm, Request, Rocket};
use rocket_dyn_templates::tera::Tera;
use rocket_dyn_templates::Template;
use std::collections::HashMap;
use ydb::YdbError;

#[get("/")]
fn index_page() -> (Status, (ContentType, &'static str)) {
    (Status::Ok, (ContentType::HTML, include_str!("index.html")))
}

#[get("/?<url>")]
async fn insert_url(url: &str) -> (Status, (ContentType, &'static str)) {
    let hash = hashers::fnv::fnv1a32(url.as_bytes()).to_string();
    (
        Status::InternalServerError,
        (ContentType::Text, "unimplemented"),
    )
}

pub fn build() -> Rocket<Build> {
    rocket::build().mount("/", routes![index_page, insert_url])
}
