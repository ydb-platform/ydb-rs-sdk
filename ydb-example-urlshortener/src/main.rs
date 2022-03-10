#[macro_use] extern crate rocket;

use rocket::http::ContentType;
use rocket::response::Redirect;

#[get("/")]
fn redirect()->Redirect {
    Redirect::to(uri!(ui_index()))
}

#[get("/ui")]
fn ui_index()->(ContentType,&'static str) {
    (ContentType::HTML, include_str!("index.html"))
}

#[rocket::main]
async fn main() {
    rocket::build()
        .mount("/", routes![redirect, ui_index])
        .launch().await;
}
